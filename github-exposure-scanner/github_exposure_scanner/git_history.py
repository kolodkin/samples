"""Local git-history scanning: mirror-clone a repo and walk its object DB.

Everything git-specific lives here. Detection (rules.scan_text / classify_context)
and the fan-out DAG are reused unchanged. Each unique blob is scanned once
(dedup), binary/oversized blobs are skipped before any regex runs, and every
finding is attributed to the commit that first introduced its blob.

Raw blob bytes are scanned in memory and discarded — never stored or logged.
"""

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass

from .github_api import is_scannable
from .rules import classify_context, scan_text

GIT_FIXTURE_ENV = "GHX_GIT_FIXTURE_DIR"


@dataclass(frozen=True)
class BlobIntro:
    """The commit that first introduced a blob."""
    path: str
    commit_sha: str
    author: str
    date: str  # YYYY-MM-DD


def _run_git(repo_dir: str, *args: str, timeout: int = 300) -> bytes:
    """Run ``git -C repo_dir <args>`` and return raw stdout bytes."""
    return subprocess.run(
        ["git", "-C", repo_dir, *args],
        capture_output=True, timeout=timeout, check=True,
    ).stdout


def clone_mirror(url: str, dest: str, timeout: int = 300) -> None:
    """Mirror-clone ``url`` into ``dest`` (all refs + full object DB)."""
    subprocess.run(
        ["git", "clone", "--mirror", "--quiet", url, dest],
        capture_output=True, timeout=timeout, check=True,
    )


def clone_url(org: str, repo: str, token: str | None) -> str:
    if token:
        return f"https://x-access-token:{token}@github.com/{org}/{repo}.git"
    return f"https://github.com/{org}/{repo}.git"


def repo_dir_for(org: str, repo: str, token: str | None, clone_timeout: int) -> tuple[str, bool]:
    """Resolve a local repo dir to scan → ``(path, is_temp)``.

    Fixture mode (``GHX_GIT_FIXTURE_DIR``) returns a pre-built repo at
    ``<dir>/<org>/<repo>`` that must not be deleted. Otherwise a mirror clone is
    made into a temp dir the caller must remove when ``is_temp`` is True.
    """
    fixture = os.environ.get(GIT_FIXTURE_ENV)
    if fixture:
        return os.path.join(fixture, org, repo), False
    dest = tempfile.mkdtemp(prefix="ghx-clone-")
    try:
        clone_mirror(clone_url(org, repo, token), dest, timeout=clone_timeout)
    except Exception:
        shutil.rmtree(dest, ignore_errors=True)
        raise
    return dest, True


def head_blob_shas(repo_dir: str) -> set[str]:
    """Blob shas reachable from HEAD (the current default-branch tree)."""
    out = _run_git(repo_dir, "ls-tree", "-r", "HEAD").decode("utf-8", "replace")
    shas: set[str] = set()
    for line in out.splitlines():
        meta, _, _ = line.partition("\t")   # "<mode> <type> <sha>\t<path>"
        parts = meta.split()
        if len(parts) >= 3 and parts[1] == "blob":
            shas.add(parts[2])
    return shas
