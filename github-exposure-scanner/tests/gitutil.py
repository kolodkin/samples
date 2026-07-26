"""Build throwaway local git repos for offline history-scanning tests."""

import os
import subprocess


def _git(cwd: str, *args: str, env: dict | None = None) -> None:
    subprocess.run(["git", "-C", cwd, *args], check=True, capture_output=True, env=env)


def make_repo(path: str, commits: list[dict]) -> None:
    """Create a git repo at ``path`` from a list of commit specs.

    Each commit spec is a dict::

        {"message": str, "date": "YYYY-MM-DD", "files": {relpath: content | None}}

    ``None`` content deletes that path. Author/committer are fixed and the date
    is pinned so ``first_seen`` is deterministic.
    """
    os.makedirs(path, exist_ok=True)
    subprocess.run(["git", "init", "-q", path], check=True, capture_output=True)
    _git(path, "config", "user.name", "Test Dev")
    _git(path, "config", "user.email", "dev@example.com")
    for spec in commits:
        for rel, content in spec["files"].items():
            fp = os.path.join(path, rel)
            if content is None:
                _git(path, "rm", "-q", "--ignore-unmatch", rel)
                continue
            os.makedirs(os.path.dirname(fp) or path, exist_ok=True)
            with open(fp, "w", encoding="utf-8") as f:
                f.write(content)
            _git(path, "add", rel)
        stamp = spec.get("date", "2020-01-01") + "T00:00:00"
        env = {**os.environ, "GIT_AUTHOR_DATE": stamp, "GIT_COMMITTER_DATE": stamp}
        subprocess.run(
            ["git", "-C", path, "commit", "-q", "-m", spec["message"]],
            check=True, capture_output=True, env=env,
        )
