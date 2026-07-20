# Git History Scanning — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the GitHub Exposure Scanner scan a repo's full git history (not just HEAD) by default, attributing each leaked secret to the commit that introduced it and flagging whether it is still present at HEAD.

**Architecture:** A new `git_history.py` module mirror-clones each repo and walks its object database locally with `git`. Detection (`rules.scan_text` / `classify_context`) and the aaiclick fan-out DAG are reused unchanged. Each unique blob is scanned once (dedup); binary/oversized blobs are skipped before any regex runs. History is the default; `--head-only` restores today's API-based fast path.

**Tech Stack:** Python 3.11, aaiclick (`Object`, `@task`/`@job` orchestration), `git` CLI via `subprocess`, `pytest` (async), offline tests build real local git repos.

## Global Constraints

- **Public data only, read-only.** Targets come from engagement authorization, never a built-in list.
- **Redaction is mandatory** — only masked fingerprints (`first4••••last4`) and locations ever leave `rules.py`. Never log or store raw secret values or raw blob content.
- **No live secret validation** — the scanner must never call third-party services to test a detected secret. Permanent non-goal.
- **Per-repo isolation** — one repo's clone/scan failure records a row and never aborts the run.
- **Python 3.11**, match existing style (async tasks, `dict[str, list]` column builders, `create_object_from_value`).
- **New finding fields, exact names/types:** `commit_sha` (String), `commit_author` (String), `first_seen` (String, `YYYY-MM-DD`), `still_present_at_head` (UInt8, 0/1).
- **Prefer `Object.markdown()`** for all report tables (per repo CLAUDE.md).
- Spec: `docs/superpowers/specs/2026-07-20-git-history-scanning-design.md`.

---

## File Structure

- **Create** `github_exposure_scanner/git_history.py` — all git-specific logic: clone, object walk, attribution, history scan.
- **Modify** `github_exposure_scanner/scan.py` — extend finding schema; history branch in `scan_one_repo`; `max_repo_mb` size cap in `list_repos_impl`.
- **Modify** `github_exposure_scanner/__init__.py` — `head_only` + cap params on the `@job` entry, plumbed to children.
- **Modify** `github_exposure_scanner/report.py` — split findings into Live-at-HEAD vs Historical-only tables.
- **Modify** `github-exposure-scanner.sh` — `--head-only`, `--max-repo-mb`, `--max-commits`, `--max-blobs`, `--clone-timeout` flags.
- **Modify** `README.md`, `SPEC.md` — new default, flags, responsible-use note.
- **Create** `tests/gitutil.py` — `make_repo()` helper (builds a local git repo from a commit list).
- **Create** `tests/test_git_history.py` — unit tests for the walk.
- **Modify** `tests/test_scan.py` — assert head-path fills new fields.
- **Modify** `tests/test_job_dynamic.py` — history-path e2e + a `head_only` variant.
- **Modify** `tests/test_report.py` — assert the two groupings render.

---

## Task 1: Extend the finding schema with attribution fields

**Files:**
- Modify: `github_exposure_scanner/scan.py`
- Test: `tests/test_scan.py`

**Interfaces:**
- Produces: `FINDING_FIELDS` (list) now ends with `"commit_sha", "commit_author", "first_seen", "still_present_at_head"`; `_FINDING_TYPES["still_present_at_head"] = FieldSpec(type="UInt8")`. `scan_repo_findings(...)` (HEAD path) fills these: `commit_sha=sha`, `commit_author=""`, `first_seen=""`, `still_present_at_head=1`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_scan.py`:

```python
async def test_head_path_fills_attribution_fields():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, max_file_kb=512, client=client, scope=None)
        data = await findings.data(orient=ORIENT_DICT)
        idx = data["secret_type"].index("AWS Key")
        assert data["still_present_at_head"][idx] == 1   # HEAD findings are live by definition
        assert data["commit_sha"][idx] != ""             # head sha recorded
        assert data["first_seen"][idx] == ""             # unknown on the HEAD path
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py::test_head_path_fills_attribution_fields -v`
Expected: FAIL with `KeyError: 'still_present_at_head'`.

- [ ] **Step 3: Extend the schema**

In `github_exposure_scanner/scan.py`, append the four fields to `FINDING_FIELDS`:

```python
FINDING_FIELDS = [
    "org", "repo", "path", "line", "rule_id", "secret_type", "severity",
    "masked_value", "permalink", "repo_stars", "detected_at", "context", "confidence",
    "commit_sha", "commit_author", "first_seen", "still_present_at_head",
]
```

Add the UInt8 type to `_FINDING_TYPES`:

```python
_FINDING_TYPES = {
    "line": FieldSpec(type="UInt32"),
    "repo_stars": FieldSpec(type="Int64"),
    "confidence": FieldSpec(type="Float64"),
    "still_present_at_head": FieldSpec(type="UInt8"),
}
```

- [ ] **Step 4: Fill the new fields on the HEAD path**

In `scan_repo_findings`, inside the `for f in scan_text(path, text):` loop, after the existing `cols["confidence"].append(confidence)` line, append the four new values:

```python
            cols["commit_sha"].append(sha)
            cols["commit_author"].append("")
            cols["first_seen"].append("")
            cols["still_present_at_head"].append(1)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py -v`
Expected: PASS (all tests in the file, including the existing ones).

- [ ] **Step 6: Commit**

```bash
git add github_exposure_scanner/scan.py tests/test_scan.py
git commit -m "Add commit-attribution fields to finding schema"
```

---

## Task 2: git subprocess helpers, mirror clone, and repo-dir resolution

**Files:**
- Create: `github_exposure_scanner/git_history.py`
- Create: `tests/gitutil.py`
- Test: `tests/test_git_history.py`

**Interfaces:**
- Produces:
  - `GIT_FIXTURE_ENV = "GHX_GIT_FIXTURE_DIR"`
  - `_run_git(repo_dir: str, *args: str, timeout: int = 300) -> bytes`
  - `clone_mirror(url: str, dest: str, timeout: int = 300) -> None`
  - `clone_url(org: str, repo: str, token: str | None) -> str`
  - `repo_dir_for(org: str, repo: str, token: str | None, clone_timeout: int) -> tuple[str, bool]` — returns `(path, is_temp)`; fixture mode returns a pre-built repo and `is_temp=False`.
  - `tests/gitutil.py::make_repo(path: str, commits: list[dict]) -> None`
- Consumes: `github_api.is_scannable`, `rules.scan_text`, `rules.classify_context` (used in later tasks).

- [ ] **Step 1: Write the test helper**

Create `tests/gitutil.py`:

```python
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
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_git_history.py`:

```python
import subprocess

from github_exposure_scanner import git_history as gh

from .gitutil import make_repo


def test_clone_mirror_and_repo_dir_fixture(tmp_path, monkeypatch):
    src = str(tmp_path / "src")
    make_repo(src, [{"message": "init", "date": "2020-01-01",
                     "files": {"a.txt": "hello\n"}}])

    # clone_mirror produces a usable object DB
    dest = str(tmp_path / "mirror")
    gh.clone_mirror(src, dest)
    out = subprocess.run(["git", "-C", dest, "rev-list", "--all"],
                         check=True, capture_output=True).stdout
    assert out.strip(), "mirror clone should contain commits"

    # fixture mode: repo_dir_for returns <fixture>/<org>/<repo>, not a temp dir
    fixture_root = str(tmp_path / "fx")
    make_repo(f"{fixture_root}/acme/widgets",
              [{"message": "init", "files": {"a.txt": "hi\n"}}])
    monkeypatch.setenv(gh.GIT_FIXTURE_ENV, fixture_root)
    path, is_temp = gh.repo_dir_for("acme", "widgets", None, 300)
    assert path == f"{fixture_root}/acme/widgets"
    assert is_temp is False


def test_clone_url_token_injection():
    assert gh.clone_url("acme", "widgets", None) == "https://github.com/acme/widgets.git"
    assert gh.clone_url("acme", "widgets", "TKN") == \
        "https://x-access-token:TKN@github.com/acme/widgets.git"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'github_exposure_scanner.git_history'`.

- [ ] **Step 4: Create the module with clone + resolution helpers**

Create `github_exposure_scanner/git_history.py`:

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add github_exposure_scanner/git_history.py tests/gitutil.py tests/test_git_history.py
git commit -m "Add git mirror-clone and repo-dir resolution helpers"
```

---

## Task 3: HEAD blob enumeration

**Files:**
- Modify: `github_exposure_scanner/git_history.py`
- Test: `tests/test_git_history.py`

**Interfaces:**
- Produces: `head_blob_shas(repo_dir: str) -> set[str]` — blob object shas reachable from HEAD.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_git_history.py`:

```python
def test_head_blob_shas_reflects_current_tree(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "add two", "date": "2020-01-01",
         "files": {"keep.txt": "keep\n", "gone.txt": "gone\n"}},
        {"message": "remove one", "date": "2020-01-02",
         "files": {"gone.txt": None}},
    ])
    head = gh.head_blob_shas(repo)
    # keep.txt's blob is at HEAD; gone.txt's is not.
    keep_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD:keep.txt"],
                              check=True, capture_output=True, text=True).stdout.strip()
    assert keep_sha in head
    assert len(head) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py::test_head_blob_shas_reflects_current_tree -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'head_blob_shas'`.

- [ ] **Step 3: Implement `head_blob_shas`**

Add to `github_exposure_scanner/git_history.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add github_exposure_scanner/git_history.py tests/test_git_history.py
git commit -m "Add HEAD blob enumeration for still-present detection"
```

---

## Task 4: Blob-introduction attribution

**Files:**
- Modify: `github_exposure_scanner/git_history.py`
- Test: `tests/test_git_history.py`

**Interfaces:**
- Produces: `iter_blob_history(repo_dir: str, max_commits: int = 0) -> dict[str, BlobIntro]` — maps each blob sha to the oldest commit that introduced it. `max_commits > 0` stops after that many commits (oldest first).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_git_history.py`:

```python
def test_iter_blob_history_attributes_introducing_commit(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "add secret", "date": "2020-01-01",
         "files": {"config.py": "KEY = 'v1'\n"}},
        {"message": "unrelated", "date": "2020-06-01",
         "files": {"README.md": "docs\n"}},
        {"message": "change secret", "date": "2021-01-01",
         "files": {"config.py": "KEY = 'v2'\n"}},
    ])
    intros = gh.iter_blob_history(repo)

    v1_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD~2:config.py"],
                            check=True, capture_output=True, text=True).stdout.strip()
    v2_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD:config.py"],
                            check=True, capture_output=True, text=True).stdout.strip()

    assert intros[v1_sha].first_seen if False else True  # (BlobIntro has no first_seen attr)
    assert intros[v1_sha].date == "2020-01-01"
    assert intros[v1_sha].path == "config.py"
    assert intros[v2_sha].date == "2021-01-01"        # the modifying commit introduced v2
    assert intros[v1_sha].author == "Test Dev"
```

Note: remove the placeholder assertion line before committing — it is only here to flag that `BlobIntro` exposes `date`, not `first_seen`. Replace that line with nothing. Final test keeps the four real assertions.

Corrected test body (use this):

```python
def test_iter_blob_history_attributes_introducing_commit(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "add secret", "date": "2020-01-01",
         "files": {"config.py": "KEY = 'v1'\n"}},
        {"message": "unrelated", "date": "2020-06-01",
         "files": {"README.md": "docs\n"}},
        {"message": "change secret", "date": "2021-01-01",
         "files": {"config.py": "KEY = 'v2'\n"}},
    ])
    intros = gh.iter_blob_history(repo)
    v1_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD~2:config.py"],
                            check=True, capture_output=True, text=True).stdout.strip()
    v2_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD:config.py"],
                            check=True, capture_output=True, text=True).stdout.strip()
    assert intros[v1_sha].date == "2020-01-01"
    assert intros[v1_sha].path == "config.py"
    assert intros[v1_sha].author == "Test Dev"
    assert intros[v2_sha].date == "2021-01-01"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py::test_iter_blob_history_attributes_introducing_commit -v`
Expected: FAIL with `AttributeError: ... has no attribute 'iter_blob_history'`.

- [ ] **Step 3: Implement `iter_blob_history`**

Add to `github_exposure_scanner/git_history.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add github_exposure_scanner/git_history.py tests/test_git_history.py
git commit -m "Attribute each blob to its introducing commit"
```

---

## Task 5: Blob reading and the full history scan

**Files:**
- Modify: `github_exposure_scanner/git_history.py`
- Test: `tests/test_git_history.py`

**Interfaces:**
- Produces:
  - `read_blob_bytes(repo_dir: str, blob_sha: str) -> bytes | None`
  - `scan_history(repo_dir: str, org: str, repo: str, stars: int, max_file_kb: int, detected_at: str, max_commits: int = 0, max_blobs: int = 0) -> dict[str, list]` — returns finding columns keyed by `FINDING_FIELDS`.
- Consumes: `scan.FINDING_FIELDS`, `scan._empty_columns` (imported lazily to avoid an import cycle).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_git_history.py`:

```python
AWS_KEY = "AKIAIOSFODNN7EXAMPLE"


def test_scan_history_finds_deleted_secret_redacted(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "leak key", "date": "2020-01-01",
         "files": {"src/config.py": f"AWS = '{AWS_KEY}'\n"}},
        {"message": "scrub key", "date": "2020-02-01",
         "files": {"src/config.py": "AWS = ''\n"}},
    ])
    cols = gh.scan_history(repo, "acme", "widgets", stars=1200,
                           max_file_kb=512, detected_at="2026-07-20")
    assert "AWS Key" in cols["secret_type"]
    idx = cols["secret_type"].index("AWS Key")
    assert cols["still_present_at_head"][idx] == 0        # the leaking blob was scrubbed
    assert cols["first_seen"][idx] == "2020-01-01"
    assert cols["commit_author"][idx] == "Test Dev"
    assert cols["path"][idx] == "src/config.py"
    assert AWS_KEY not in "".join(cols["masked_value"])   # still redacted
    assert f"/blob/{cols['commit_sha'][idx]}/src/config.py#L1" in cols["permalink"][idx]


def test_scan_history_skips_binary_blob(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "binary", "date": "2020-01-01",
         "files": {"data.bin": "AKIAIOSFODNN7EXAMPLE\x00binary\n"}},
    ])
    cols = gh.scan_history(repo, "acme", "widgets", 1, 512, "2026-07-20")
    assert cols["secret_type"] == []   # NUL byte → skipped before regex
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py::test_scan_history_finds_deleted_secret_redacted -v`
Expected: FAIL with `AttributeError: ... has no attribute 'scan_history'`.

- [ ] **Step 3: Implement reading + scan**

Add to `github_exposure_scanner/git_history.py`:

```python
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
            context, confidence = classify_context(f.secret_type, f.path, all_paths)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd github-exposure-scanner && uv run pytest tests/test_git_history.py -v`
Expected: PASS (all history-module tests).

- [ ] **Step 5: Commit**

```bash
git add github_exposure_scanner/git_history.py tests/test_git_history.py
git commit -m "Scan full history with dedup, binary skip, and attribution"
```

---

## Task 6: Wire the history path end-to-end

**Files:**
- Modify: `github_exposure_scanner/scan.py`
- Modify: `github_exposure_scanner/__init__.py`
- Test: `tests/test_scan.py`, `tests/test_job_dynamic.py`

**Interfaces:**
- Consumes: `git_history.repo_dir_for`, `git_history.scan_history` (Task 2/5).
- Produces:
  - `list_repos_impl(..., max_repo_mb: int | None = None)` — a repo whose GitHub `size` (KB) exceeds `max_repo_mb * 1024` is recorded with a `list_error` skip row and not scanned.
  - `scan_one_repo(org, repo, head_sha, stars, max_file_kb, out, tree=None, head_only=False, max_commits=0, max_blobs=0, clone_timeout=300)` — history by default, HEAD-only when `head_only=True`.
  - `exposure_pipeline(targets=None, max_repos=25, max_file_kb=512, publish_airtable=False, head_only=False, max_repo_mb=100, max_commits=0, max_blobs=0, clone_timeout=300)`.

- [ ] **Step 1: Write the failing size-cap test**

Add to `tests/test_scan.py`:

```python
async def test_max_repo_mb_skips_oversized_repo():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        # acme/widgets fixture repo.json reports size in KB; cap at ~0 MB skips it.
        repos, _ = await list_repos_impl(
            ["acme/widgets"], 25, client, "2026-07-17", scope=None, max_repo_mb=0
        )
        data = await repos.data(orient=ORIENT_DICT)
        assert data["list_error"][0] and "exceeds" in data["list_error"][0]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py::test_max_repo_mb_skips_oversized_repo -v`
Expected: FAIL with `TypeError: list_repos_impl() got an unexpected keyword argument 'max_repo_mb'`.

- [ ] **Step 3: Add the size cap to `list_repos_impl`**

In `github_exposure_scanner/scan.py`, change the signature and add the cap check at the top of the inner `_add_repo`:

```python
async def list_repos_impl(
    targets: list[str], max_repos: int, client: GitHubClient, now: str,
    scope: str | None = "job", max_repo_mb: int | None = None,
) -> tuple[Object, dict[str, list[dict]]]:
```

Then, as the first lines inside `async def _add_repo(org, repo_meta)` (before `sha = ...`):

```python
        repo = repo_meta["name"]
        branch = repo_meta.get("default_branch", "main")
        size_kb = repo_meta.get("size", 0)
        if max_repo_mb is not None and size_kb > max_repo_mb * 1024:
            _append_repo_row(
                cols, org=org, repo=repo, branch=branch,
                stars=repo_meta.get("stargazers_count", 0),
                pushed_at=repo_meta.get("pushed_at", ""),
                language=repo_meta.get("language"), size_kb=size_kb,
                list_error=f"skipped: {size_kb}KB exceeds {max_repo_mb}MB cap",
            )
            return
```

Remove the now-duplicated `repo = repo_meta["name"]` / `branch = ...` lines that previously started the function (they are replaced by the block above).

- [ ] **Step 4: Run the size-cap test**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py::test_max_repo_mb_skips_oversized_repo -v`
Expected: PASS.

- [ ] **Step 5: Add the history branch to `scan_one_repo`**

In `github_exposure_scanner/scan.py`, add imports near the top:

```python
import asyncio
import os
import shutil

from .git_history import repo_dir_for, scan_history
```

Replace the `scan_one_repo` task body with the branching version:

```python
@task
async def scan_one_repo(
    org: str, repo: str, head_sha: str, stars: int, max_file_kb: int, out: Object,
    tree: list[dict] | None = None, head_only: bool = False,
    max_commits: int = 0, max_blobs: int = 0, clone_timeout: int = 300,
) -> None:
    """Scan one repo and append its redacted findings into shared ``out``.

    History scan by default (mirror clone + object walk); ``head_only`` uses the
    API-based current-HEAD scan. One task instance per discovered repository.
    """
    detected_at = datetime.now(UTC).strftime("%Y-%m-%d")
    if head_only:
        client = make_client()
        try:
            cols = await scan_repo_findings(
                client, org, repo, head_sha, stars, max_file_kb, detected_at, tree=tree
            )
        finally:
            await client.aclose()
    else:
        repo_dir, is_temp = repo_dir_for(org, repo, os.environ.get("GITHUB_TOKEN"), clone_timeout)
        try:
            cols = await asyncio.to_thread(
                scan_history, repo_dir, org, repo, stars, max_file_kb, detected_at,
                max_commits, max_blobs,
            )
        finally:
            if is_temp:
                shutil.rmtree(repo_dir, ignore_errors=True)
    if cols["org"]:  # only touch the table when this repo actually had findings
        part = await create_object_from_value(cols, fields=_FINDING_TYPES)
        await out.insert(part)
```

- [ ] **Step 6: Plumb params through the `@job` entry**

In `github_exposure_scanner/__init__.py`, change the `exposure_pipeline` signature:

```python
@job("github_exposure_scanner")
async def exposure_pipeline(
    targets: list[str] | None = None,
    max_repos: int = 25,
    max_file_kb: int = 512,
    publish_airtable: bool = False,
    head_only: bool = False,
    max_repo_mb: int = 100,
    max_commits: int = 0,
    max_blobs: int = 0,
    clone_timeout: int = 300,
):
```

Pass the size cap to listing (only meaningful for the clone path):

```python
        repos, trees = await list_repos_impl(
            targets, max_repos, client, datetime.now(UTC).strftime("%Y-%m-%d"),
            scope="job", max_repo_mb=None if head_only else max_repo_mb,
        )
```

Pass the per-repo knobs when building children (add the four kwargs to each `scan_one_repo(...)`):

```python
        scan_one_repo(
            org=rows["org"][i],
            repo=rows["repo"][i],
            head_sha=rows["head_sha"][i],
            stars=int(rows["stars"][i]),
            max_file_kb=max_file_kb,
            out=findings,
            tree=trees.get(f'{rows["org"][i]}/{rows["repo"][i]}'),
            head_only=head_only,
            max_commits=max_commits,
            max_blobs=max_blobs,
            clone_timeout=clone_timeout,
        )
```

- [ ] **Step 7: Rewrite the dynamic e2e test for both modes**

Replace the body of `tests/test_job_dynamic.py` with:

```python
"""End-to-end tests of the dynamic fan-out job via the in-process runner.

Covers the default history-scan path (local git fixtures, no network) and the
legacy HEAD-only path (canned API fixtures).
"""

import os

from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED

from github_exposure_scanner import exposure_pipeline

from .gitutil import make_repo

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
AWS_KEY = "AKIAIOSFODNN7EXAMPLE"


async def test_head_only_fan_out(orch_ctx, monkeypatch, tmp_path):
    monkeypatch.setenv("GHX_FIXTURE_DIR", FIXTURES)
    report_file = tmp_path / "report.md"
    monkeypatch.setenv("AAICLICK_REPORT_FILE", str(report_file))

    job = await exposure_pipeline(targets=["acme", "acme/proxytool"], max_repos=25, head_only=True)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, f"Job failed: {job.error}"
    report = report_file.read_text()
    assert "AWS Key" in report
    assert "Private Key" in report
    assert AWS_KEY not in report  # redacted


async def test_history_fan_out(orch_ctx, monkeypatch, tmp_path):
    # Listing uses the canned API fixtures; scanning uses local git repos.
    monkeypatch.setenv("GHX_FIXTURE_DIR", FIXTURES)
    git_root = tmp_path / "gitfx"
    make_repo(str(git_root / "acme" / "widgets"), [
        {"message": "leak", "date": "2020-01-01",
         "files": {"src/config.py": f"AWS = '{AWS_KEY}'\n"}},
    ])
    make_repo(str(git_root / "acme" / "proxytool"), [
        {"message": "add key", "date": "2019-05-05",
         "files": {"id_rsa": "-----BEGIN RSA PRIVATE KEY-----\nabc\n"}},
        {"message": "scrub key", "date": "2019-05-06", "files": {"id_rsa": "cleaned\n"}},
    ])
    monkeypatch.setenv("GHX_GIT_FIXTURE_DIR", str(git_root))
    report_file = tmp_path / "report.md"
    monkeypatch.setenv("AAICLICK_REPORT_FILE", str(report_file))

    job = await exposure_pipeline(targets=["acme/widgets", "acme/proxytool"], max_repos=25)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, f"Job failed: {job.error}"
    report = report_file.read_text()
    assert "AWS Key" in report          # widgets, still live at HEAD
    assert "Private Key" in report      # proxytool, historical-only
    assert "Historical-only" in report  # the grouping heading (Task 7)
    assert AWS_KEY not in report        # redacted
```

- [ ] **Step 8: Run the full suite**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py tests/test_job_dynamic.py -v`
Expected: PASS. (`test_history_fan_out` needs Task 7's report headings — it is expected to fail on the `"Historical-only"` assertion until Task 7 lands; run it again at the end of Task 7.)

Note for the implementer: if executing strictly task-by-task, temporarily drop the `assert "Historical-only" in report` line, complete Task 7, then restore it. Do not mark Task 6 complete with a failing assertion — either land Task 7's report change first or keep that one assertion commented until Task 7.

- [ ] **Step 9: Commit**

```bash
git add github_exposure_scanner/scan.py github_exposure_scanner/__init__.py tests/test_scan.py tests/test_job_dynamic.py
git commit -m "Make history scanning the default, HEAD-only opt-in"
```

---

## Task 7: Report — Live-at-HEAD vs Historical-only groupings

**Files:**
- Modify: `github_exposure_scanner/report.py`
- Test: `tests/test_report.py`

**Interfaces:**
- Consumes: findings Object with `still_present_at_head` / `first_seen` columns.
- Produces: `render_report(...)` output containing a **"Live at HEAD"** section and a **"Historical-only"** section, each a `Object.markdown()` table (or an explicit empty note).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_report.py` (import helpers already present in that file — mirror its existing style; if it builds findings via `scan_repos_impl`, reuse that). Add:

```python
async def test_report_splits_live_and_historical(tmp_path, monkeypatch):
    from github_exposure_scanner import git_history as gh
    from aaiclick import create_object_from_value
    from github_exposure_scanner.scan import _FINDING_TYPES
    from .gitutil import make_repo

    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "leak", "date": "2020-01-01",
         "files": {"live.py": "AWS = 'AKIAIOSFODNN7EXAMPLE'\n"}},
        {"message": "leak2", "date": "2020-02-01",
         "files": {"gone.py": "AWS2 = 'AKIA1234567890ABCDEF'\n"}},
        {"message": "scrub", "date": "2020-03-01", "files": {"gone.py": None}},
    ])
    cols = gh.scan_history(repo, "acme", "widgets", 1200, 512, "2026-07-20")
    async with data_context():
        findings = await create_object_from_value(cols, name="f", scope=None, fields=_FINDING_TYPES)
        # minimal repos/summary objects are already built by this file's helpers;
        # reuse them or build one-row equivalents as the existing tests do.
        report = await _render_with(findings)  # see note below
    assert "Live at HEAD" in report
    assert "Historical-only" in report
```

Note: `tests/test_report.py` already constructs `repos`/`summary` objects for its existing `render_report` test. Reuse that setup — extract a small local helper `_render_with(findings)` in the test module that calls `render_report(repos, findings, summary, None, None)` with those objects, rather than duplicating construction. If no such setup exists, build repos/summary the same way `test_e2e.py` does (`list_repos_impl` + `score_exposure_impl`).

- [ ] **Step 2: Run it to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_report.py::test_report_splits_live_and_historical -v`
Expected: FAIL — the phrases "Live at HEAD" / "Historical-only" are not in the report yet.

- [ ] **Step 3: Replace the "Findings" section in `render_report`**

In `github_exposure_scanner/report.py`, replace the single findings block (the `### Findings (redacted)` section) with two grouped sections:

```python
    _cols = ["org", "repo", "path", "line", "secret_type", "severity",
             "confidence", "context", "first_seen", "masked_value"]

    async def _findings_table(where: str) -> str:
        view = findings[_cols].view(
            where=where, order_by="confidence DESC, repo_stars DESC", limit=100
        )
        return await view.markdown(truncate={"path": 40, "context": 40})

    lines.append("### Findings — Live at HEAD (redacted)")
    lines.append("")
    live_count = await (await findings.view(where="still_present_at_head = 1")["org"].count()).data()
    if live_count:
        lines.append(await _findings_table("still_present_at_head = 1"))
    else:
        lines.append("_No secrets currently present at HEAD._")
    lines.append("")

    lines.append("### Findings — Historical-only (redacted)")
    lines.append("")
    hist_count = await (await findings.view(where="still_present_at_head = 0")["org"].count()).data()
    if hist_count:
        lines.append(await _findings_table("still_present_at_head = 0"))
    else:
        lines.append("_No secrets found only in history._")
    lines.append("")
```

Leave the surrounding `### Exposure Summary`, `### Scanned Repositories`, and `### Airtable` sections unchanged. Remove the old `### Findings (redacted)` block and its `total_findings`-gated single table.

- [ ] **Step 4: Run the report tests**

Run: `cd github-exposure-scanner && uv run pytest tests/test_report.py -v`
Expected: PASS.

- [ ] **Step 5: Re-run the dynamic history test (restore its assertion)**

If you removed/commented `assert "Historical-only" in report` in Task 6, restore it now.

Run: `cd github-exposure-scanner && uv run pytest tests/test_job_dynamic.py -v`
Expected: PASS (both modes).

- [ ] **Step 6: Commit**

```bash
git add github_exposure_scanner/report.py tests/test_report.py tests/test_job_dynamic.py
git commit -m "Split report into live-at-HEAD and historical-only findings"
```

---

## Task 8: Shell flags and documentation

**Files:**
- Modify: `github-exposure-scanner.sh`
- Modify: `README.md`
- Modify: `SPEC.md`

**Interfaces:** No Python interface changes — CLI flags map to `--params` JSON keys already accepted by `exposure_pipeline`.

- [ ] **Step 1: Add flag parsing to the shell runner**

In `github-exposure-scanner.sh`, add cases to the `while` flag loop (alongside the existing `--max-file-kb` case):

```bash
        --head-only)
            PARAMS_PARTS+=('"head_only": true')
            shift
            ;;
        --max-repo-mb)
            PARAMS_PARTS+=("\"max_repo_mb\": $2")
            shift 2
            ;;
        --max-commits)
            PARAMS_PARTS+=("\"max_commits\": $2")
            shift 2
            ;;
        --max-blobs)
            PARAMS_PARTS+=("\"max_blobs\": $2")
            shift 2
            ;;
        --clone-timeout)
            PARAMS_PARTS+=("\"clone_timeout\": $2")
            shift 2
            ;;
```

Update the two `Usage:` strings and the header comment to list the new flags and note that **history scanning is the default** (`--head-only` for the fast path). Add to the header comment block:

```bash
#   --head-only        Scan only current HEAD (fast path); default scans full
#                      git history via a local mirror clone
#   --max-repo-mb N    Skip repos whose GitHub size exceeds N MB (default: 100)
#   --max-commits N    Cap history walk to N oldest commits (0 = all)
#   --max-blobs N      Cap history scan to N unique blobs (0 = all)
#   --clone-timeout N  Per-repo clone timeout in seconds (default: 300)
```

- [ ] **Step 2: Verify the shell still parses (smoke)**

Run: `cd github-exposure-scanner && bash -n github-exposure-scanner.sh && echo OK`
Expected: `OK` (syntax check only — no execution).

- [ ] **Step 3: Update the README**

In `github-exposure-scanner/README.md`, update the description paragraph to state that history scanning is the default and findings are attributed to the introducing commit and flagged live-vs-historical, and show the default invocation plus the `--head-only` option. Keep the README to the project's minimal convention (title, one paragraph, run block):

```markdown
GitHub Exposure Scanner
---

Step 1 of a cyber-risk bot: given a GitHub organization (or specific `org/repo`
targets), it enumerates the org's public repositories, mirror-clones each, and
scans their **full git history** for leaked secrets using a built-in regex rule
library — catching secrets that were committed and later removed. Each finding
is attributed to the commit that introduced it and flagged as live-at-HEAD or
historical-only. Secrets are never shown in full — only masked fingerprints and
their location. Findings and a per-org exposure summary can optionally be
published to Airtable. Use `--head-only` for a fast current-HEAD scan.

\```bash
./github-exposure-scanner.sh --targets "octocat/Hello-World"
\```
```

- [ ] **Step 4: Update SPEC responsible-use + pipeline notes**

In `github-exposure-scanner/SPEC.md`:
- Update the opening/pipeline description to note history scanning is the default (mirror clone + object walk), that each finding carries `commit_sha`/`commit_author`/`first_seen`/`still_present_at_head`, and that `--head-only` keeps the API-based fast path.
- In **Responsible use**, add a sentence: history scanning surfaces secrets that were committed and later removed but remain in public git history — treat every historical finding as compromised and follow responsible-disclosure/rotation practice; the scanner never validates a secret against any live service.

- [ ] **Step 5: Run the whole suite**

Run: `cd github-exposure-scanner && uv run pytest -q`
Expected: PASS (all tests).

- [ ] **Step 6: Commit**

```bash
git add github-exposure-scanner.sh README.md SPEC.md
git commit -m "Document history-scan default and new CLI flags"
```

---

## Self-Review

**Spec coverage:**
- History access = local mirror clone + git walk → Tasks 2–6.
- Blob-based dedup walk → Task 5 (`scan_history` iterates unique blob shas).
- Full history + safety caps (`max_repo_mb`, `max_commits`, `max_blobs`, clone timeout) → Task 5 (`max_commits`/`max_blobs`), Task 6 (`max_repo_mb`, `clone_timeout`).
- History default, `--head-only` opt-in → Task 6 (entry + `scan_one_repo`), Task 8 (flag).
- Finding attribution fields → Task 1 (schema + HEAD path), Task 5 (history path).
- `still_present_at_head` triage flag → Tasks 3, 5; surfaced in report → Task 7.
- No live validation, redaction, public-only → enforced (Global Constraints; `scan_history` reuses `rules` redaction; nothing added calls external services).
- Offline deterministic tests via local git repos → `tests/gitutil.py` + Tasks 2–7.
- Combined-regex correctly deferred (spec's future list) — no task, matches revised spec.

**Placeholder scan:** No "TBD"/"add error handling" placeholders. The one illustrative bad-assertion in Task 4 Step 1 is explicitly flagged and replaced by the corrected test body in the same step.

**Type consistency:** `FINDING_FIELDS` order and the four field names (`commit_sha`, `commit_author`, `first_seen`, `still_present_at_head`) are identical across Task 1 (schema + HEAD path), Task 5 (`scan_history`), and Task 7 (report columns / `where` clauses). `still_present_at_head` is UInt8 (0/1) everywhere. `scan_one_repo` kwargs (`head_only`, `max_commits`, `max_blobs`, `clone_timeout`) match between Task 6's `scan.py` definition and the `__init__.py` call site. `repo_dir_for` returns `(path, is_temp)` in Task 2 and is unpacked that way in Task 6.
