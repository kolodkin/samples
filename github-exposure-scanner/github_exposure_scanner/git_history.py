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
from .rules import finding_confidence, scan_text

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


def iter_blob_history(repo_dir: str, max_commits: int = 0) -> dict[str, BlobIntro]:
    """Map each blob sha → the commit that first introduced it.

    Walks ``git log --all --reverse`` (oldest first), so the first time a blob
    sha appears in a commit's ``--raw`` diff is its introduction. Deletions
    (null new-sha) are ignored. ``max_commits`` (>0) stops after that many
    commits, keeping the oldest history where secrets are most likely buried.
    """
    out = _run_git(
        repo_dir,
        "log", "--all", "--reverse", "--no-renames", "--no-abbrev",
        "--format=commit%x00%H%x00%an%x00%aI", "--raw",
    ).decode("utf-8", "replace")

    intros: dict[str, BlobIntro] = {}
    cur_sha = cur_author = cur_date = ""
    commits_seen = 0
    for line in out.splitlines():
        if line.startswith("commit\x00"):
            _, cur_sha, cur_author, iso = line.split("\x00")
            cur_date = iso[:10]  # YYYY-MM-DD
            commits_seen += 1
            if max_commits and commits_seen > max_commits:
                break
            continue
        if not line.startswith(":"):
            continue
        meta, tab, path = line.partition("\t")
        if not tab:
            continue
        fields = meta[1:].split()  # drop leading ':' → "<omode> <nmode> <osha> <nsha> <status>"
        if len(fields) < 5:
            continue
        new_sha, status = fields[3], fields[4]
        if status.startswith("D") or set(new_sha) == {"0"}:
            continue
        if new_sha not in intros:
            intros[new_sha] = BlobIntro(
                path=path, commit_sha=cur_sha, author=cur_author, date=cur_date
            )
    return intros


def read_blob_bytes(repo_dir: str, blob_sha: str) -> bytes | None:
    """Return a blob's raw bytes, or None if it cannot be read."""
    try:
        return _run_git(repo_dir, "cat-file", "blob", blob_sha)
    except subprocess.CalledProcessError:
        return None


def scan_history(
    repo_dir: str, org: str, repo: str, stars: int, max_file_kb: int, detected_at: str,
    max_commits: int = 0, max_blobs: int = 0,
) -> dict[str, list]:
    """Scan a repo's full history → redacted finding columns with attribution.

    Each unique introduced blob is scanned once; binary (NUL-byte) and oversized
    blobs are skipped before any regex runs. ``still_present_at_head`` marks
    blobs still reachable from HEAD.
    """
    from .scan import FINDING_FIELDS, _empty_columns  # lazy import avoids cycle

    max_bytes = max_file_kb * 1024
    head = head_blob_shas(repo_dir)
    intros = iter_blob_history(repo_dir, max_commits)
    all_paths = [bi.path for bi in intros.values()]
    cols = _empty_columns(FINDING_FIELDS)

    scanned = 0
    for blob_sha, bi in intros.items():
        if max_blobs and scanned >= max_blobs:
            break
        scanned += 1
        raw = read_blob_bytes(repo_dir, blob_sha)
        if raw is None or b"\x00" in raw:
            continue
        if not is_scannable(bi.path, len(raw), max_bytes):
            continue
        text = raw.decode("utf-8", "replace")
        still = 1 if blob_sha in head else 0
        for f in scan_text(bi.path, text):
            context, confidence = finding_confidence(f, all_paths)
            cols["org"].append(org)
            cols["repo"].append(repo)
            cols["path"].append(f.path)
            cols["line"].append(f.line)
            cols["rule_id"].append(f.rule_id)
            cols["secret_type"].append(f.secret_type)
            cols["severity"].append(f.severity)
            cols["masked_value"].append(f.masked_value)
            cols["permalink"].append(
                f"https://github.com/{org}/{repo}/blob/{bi.commit_sha}/{f.path}#L{f.line}"
            )
            cols["repo_stars"].append(int(stars))
            cols["detected_at"].append(detected_at)
            cols["context"].append(context)
            cols["confidence"].append(confidence)
            cols["commit_sha"].append(bi.commit_sha)
            cols["commit_author"].append(bi.author)
            cols["first_seen"].append(bi.date)
            cols["still_present_at_head"].append(still)
    return cols
