# GitHub Exposure Scanner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `github-exposure-scanner`, an aaiclick sample that scans a GitHub org's (or specific repos') public code for leaked secrets and produces a per-org exposure profile, optionally published to Airtable.

**Architecture:** An aaiclick `@job` DAG (`list_repos → scan_repos → score_exposure → generate_report`, with an opt-in Airtable branch). External GitHub calls go through a small `httpx` client that has a file-based **fixture mode** for offline/deterministic tests. Secret detection is a pure-Python regex library; findings are redacted at detection time and loaded into aaiclick `Object`s so all counting/scoring is SQL.

**Tech Stack:** Python 3.10+, aaiclick[distributed], httpx, pydantic, pytest.

## Global Constraints

- Package dir `github-exposure-scanner/`, nested Python package `github_exposure_scanner/` (runnable via `python -m github_exposure_scanner`). Copied verbatim from `CLAUDE.md` project structure.
- `report.py` renders via `Object.markdown()` — no custom table rendering. The `@job` returns the terminal report task.
- Shell runner uses `PYTHON="${PYTHON:-uv run python}"` and follows the register-job → start-worker → poll → stop-worker orchestration pattern from `imdb-dataset-builder.sh`.
- README follows the exact convention in `CLAUDE.md` (setext title, one paragraph, one bash block — no other sections).
- **Raw secret values must never leave the scanning task** — not to stdout, ClickHouse, or Airtable. Only masked fingerprints (`first4 + "••••" + last4`) are stored/printed.
- Distributed backend env contract (in the `.sh`): `AAICLICK_SQL_URL` default `postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick`, `AAICLICK_CH_URL` default `clickhouse://default:benchmark@localhost:8123/default`, `AAICLICK_LOG_DIR` default `tmp/logs`.
- Airtable publishing is opt-in (`publish_airtable=True`) and additionally gated on `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID`; default is skipped.
- Secret detection: built-in regex rules only. Scan scope: current HEAD, top repos, GitHub API only (no cloning).

---

### Task 1: Project scaffold

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/__init__.py` (placeholder)
- Create: `github-exposure-scanner/github_exposure_scanner/__main__.py`
- Create: `github-exposure-scanner/github_exposure_scanner/pyproject.toml`
- Create: `github-exposure-scanner/pytest.ini`
- Create: `github-exposure-scanner/README.md`
- Test: `github-exposure-scanner/tests/test_smoke.py`

**Interfaces:**
- Produces: an importable `github_exposure_scanner` package with an `async def main(**kwargs)` (final wiring lands in Task 9; a stub is fine now).

- [ ] **Step 1: Create the package pyproject**

`github-exposure-scanner/github_exposure_scanner/pyproject.toml`:
```toml
[project]
name = "github-exposure-scanner"
version = "0.1.0"
description = "Scan a company's public GitHub repos for leaked secrets and profile its exposure"
requires-python = ">=3.10"

dependencies = [
    "aaiclick[distributed]",
    "httpx>=0.27.0",
    "pydantic>=2.0.0",
]
```

- [ ] **Step 2: Create the pytest config**

`github-exposure-scanner/pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
testpaths = tests
```

- [ ] **Step 3: Create the package stub files**

`github-exposure-scanner/github_exposure_scanner/__init__.py`:
```python
"""GitHub Exposure Scanner — cyber-bot step 1 (GitHub attack-surface exposure)."""


async def main(**kwargs):
    """Register the exposure-scanner job. Wired up in a later task."""
    raise NotImplementedError
```

`github-exposure-scanner/github_exposure_scanner/__main__.py`:
```python
import argparse
import asyncio
import json

from . import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register the GitHub exposure scanner job")
    parser.add_argument("--params", type=str, default=None, help="JSON dict of job params")
    args = parser.parse_args()
    kwargs = json.loads(args.params) if args.params else {}
    asyncio.run(main(**kwargs))
```

- [ ] **Step 4: Create the README**

`github-exposure-scanner/README.md`:
```markdown
GitHub Exposure Scanner
---

Step 1 of a cyber-risk bot: given a GitHub organization (or specific `org/repo` targets), it enumerates the org's public repositories, scans their current file contents for leaked secrets using a built-in regex rule library, scores each org's exposure, and prints a redacted report. Secrets are never shown in full — only masked fingerprints and their location. Findings and a per-org exposure summary can optionally be published to Airtable. It demonstrates aaiclick external-API ingestion, `create_object_from_value`, SQL aggregation over `Object`s, and gated Airtable publishing.

\```bash
./github-exposure-scanner.sh --targets "octocat/Hello-World"
\```
```
(Replace `\`` with real backticks when writing the file.)

- [ ] **Step 5: Write the smoke test**

`github-exposure-scanner/tests/test_smoke.py`:
```python
import github_exposure_scanner


def test_package_imports():
    assert hasattr(github_exposure_scanner, "main")
```

- [ ] **Step 6: Run the smoke test**

Run: `cd github-exposure-scanner && uv run pytest tests/test_smoke.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add github-exposure-scanner/
git commit -m "github-exposure-scanner: project scaffold"
```

---

### Task 2: Secret-detection rule library

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/rules.py`
- Test: `github-exposure-scanner/tests/test_rules.py`

**Interfaces:**
- Produces:
  - `SEVERITY_WEIGHT: dict[str, int]` = `{"Critical": 10, "High": 5, "Medium": 2, "Low": 1}`
  - `mask(value: str) -> str`
  - `@dataclass(frozen=True) Finding` with fields `path: str, line: int, rule_id: str, secret_type: str, severity: str, masked_value: str`
  - `scan_text(path: str, text: str) -> list[Finding]`

- [ ] **Step 1: Write the failing tests**

`github-exposure-scanner/tests/test_rules.py`:
```python
from github_exposure_scanner.rules import Finding, mask, scan_text, SEVERITY_WEIGHT


def test_mask_hides_middle():
    assert mask("AKIAIOSFODNN7EXAMPLE") == "AKIA••••MPLE"


def test_mask_short_value_fully_hidden():
    assert mask("abcd") == "••••"


def test_detects_aws_key_with_location():
    text = "line one\nkey = AKIAIOSFODNN7EXAMPLE\n"
    findings = scan_text("config.py", text)
    aws = [f for f in findings if f.secret_type == "AWS Key"]
    assert len(aws) == 1
    assert aws[0].line == 2
    assert aws[0].path == "config.py"
    assert aws[0].severity == "Critical"
    assert "AKIAIOSFODNN7EXAMPLE" not in aws[0].masked_value


def test_detects_github_pat():
    text = "token: ghp_" + "a" * 36
    findings = scan_text("x.env", text)
    assert any(f.secret_type == "GitHub PAT" for f in findings)


def test_high_entropy_assignment_masks_only_value():
    text = 'password = "s3cr3tV4lue_ABCDEFGHIJ"'
    findings = scan_text("s.py", text)
    he = [f for f in findings if f.secret_type == "High-entropy"]
    assert len(he) == 1
    assert "s3cr3tV4lue_ABCDEFGHIJ" not in he[0].masked_value


def test_clean_text_no_findings():
    assert scan_text("ok.py", "def add(a, b):\n    return a + b\n") == []


def test_severity_weight_table():
    assert SEVERITY_WEIGHT == {"Critical": 10, "High": 5, "Medium": 2, "Low": 1}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd github-exposure-scanner && uv run pytest tests/test_rules.py -v`
Expected: FAIL with `ModuleNotFoundError: github_exposure_scanner.rules`.

- [ ] **Step 3: Implement `rules.py`**

`github-exposure-scanner/github_exposure_scanner/rules.py`:
```python
"""Built-in regex rule library for leaked-secret detection.

Detection is pure Python over file text. Every match is redacted at
detection time — raw secret values never leave this module.
"""

import re
from dataclasses import dataclass

SEVERITY_WEIGHT: dict[str, int] = {"Critical": 10, "High": 5, "Medium": 2, "Low": 1}


@dataclass(frozen=True)
class _Rule:
    id: str
    secret_type: str
    severity: str
    pattern: re.Pattern
    value_group: int = 0  # which capture group holds the secret (0 = whole match)


RULES: list[_Rule] = [
    _Rule("aws-access-key", "AWS Key", "Critical", re.compile(r"AKIA[0-9A-Z]{16}")),
    _Rule("github-pat", "GitHub PAT", "Critical", re.compile(r"gh[pousr]_[A-Za-z0-9]{36}")),
    _Rule("stripe-live", "Stripe Key", "Critical", re.compile(r"sk_live_[0-9A-Za-z]{24}")),
    _Rule("private-key", "Private Key", "Critical",
          re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----")),
    _Rule("slack-token", "Slack Token", "High", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
    _Rule("google-api-key", "Google API Key", "High", re.compile(r"AIza[0-9A-Za-z_\-]{35}")),
    _Rule("jwt", "JWT", "Medium",
          re.compile(r"eyJ[A-Za-z0-9_\-]+\.eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+")),
    _Rule("high-entropy-assignment", "High-entropy", "Low", re.compile(
        r"""(?ix)
        (?:secret|token|password|passwd|api[_-]?key|access[_-]?key)
        ['"]?\s*[:=]\s*['"]([A-Za-z0-9+/=_\-]{16,})['"]
        """), value_group=1),
]


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    rule_id: str
    secret_type: str
    severity: str
    masked_value: str


def mask(value: str) -> str:
    """Redact a secret to ``first4 + "••••" + last4`` (fully hidden if short)."""
    if len(value) <= 8:
        return "••••"
    return f"{value[:4]}••••{value[-4:]}"


def scan_text(path: str, text: str) -> list[Finding]:
    """Scan file text line-by-line against every rule; return redacted findings."""
    findings: list[Finding] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for rule in RULES:
            for m in rule.pattern.finditer(line):
                value = m.group(rule.value_group)
                findings.append(Finding(
                    path=path, line=lineno, rule_id=rule.id,
                    secret_type=rule.secret_type, severity=rule.severity,
                    masked_value=mask(value),
                ))
    return findings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd github-exposure-scanner && uv run pytest tests/test_rules.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/rules.py github-exposure-scanner/tests/test_rules.py
git commit -m "github-exposure-scanner: regex secret-detection rule library"
```

---

### Task 3: GitHub API client with fixture mode

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/github_api.py`
- Test: `github-exposure-scanner/tests/test_github_api.py`
- Create (fixtures): `github-exposure-scanner/tests/fixtures/orgs/acme/repos.json`, `.../repos/acme/widgets/repo.json`, `.../repos/acme/widgets/tree.json`, `.../repos/acme/widgets/raw/src/config.py`

**Interfaces:**
- Produces:
  - `class RateLimitError(RuntimeError)`
  - `SCANNABLE: `helper `is_scannable(path: str, size: int, max_bytes: int) -> bool`
  - `class GitHubClient:` with:
    - `__init__(self, token: str | None = None, fixture_dir: str | None = None)`
    - `async def list_org_repos(self, org: str, max_repos: int) -> list[dict]` — each dict has `name, stargazers_count, pushed_at, language, size (KB), default_branch`
    - `async def get_repo(self, org: str, repo: str) -> dict` — same keys as above plus `name`
    - `async def get_head_sha(self, org: str, repo: str, branch: str) -> str`
    - `async def get_tree(self, org: str, repo: str, sha: str) -> list[dict]` — blob entries `{"path": str, "size": int}`
    - `async def get_raw(self, org: str, repo: str, sha: str, path: str) -> str`
    - `async def aclose(self) -> None`
  - `def make_client() -> GitHubClient` — reads `GITHUB_TOKEN` and `GHX_FIXTURE_DIR` from env.

- [ ] **Step 1: Create fixtures**

`tests/fixtures/orgs/acme/repos.json`:
```json
[
  {"name": "widgets", "stargazers_count": 1200, "pushed_at": "2026-05-01T00:00:00Z", "language": "Python", "size": 40, "default_branch": "main"},
  {"name": "docs", "stargazers_count": 5, "pushed_at": "2026-04-01T00:00:00Z", "language": "Markdown", "size": 10, "default_branch": "main"}
]
```

`tests/fixtures/repos/acme/widgets/repo.json`:
```json
{"name": "widgets", "stargazers_count": 1200, "pushed_at": "2026-05-01T00:00:00Z", "language": "Python", "size": 40, "default_branch": "main"}
```

`tests/fixtures/repos/acme/widgets/tree.json`:
```json
[
  {"path": "src/config.py", "size": 120},
  {"path": "README.md", "size": 30},
  {"path": "assets/logo.png", "size": 900000}
]
```

`tests/fixtures/repos/acme/widgets/raw/src/config.py`:
```
AWS_KEY = "AKIAIOSFODNN7EXAMPLE"
def ok():
    return 1
```
(Also create `tests/fixtures/repos/acme/widgets/raw/README.md` with harmless text, and a `tests/fixtures/repos/acme/docs/{repo.json,tree.json}` with an empty tree `[]`, for the multi-repo scan test in Task 5. For `docs`, `repo.json` mirrors the repos.json entry.)

- [ ] **Step 2: Write the failing tests**

`github-exposure-scanner/tests/test_github_api.py`:
```python
import os

from github_exposure_scanner.github_api import GitHubClient, is_scannable

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def test_is_scannable_rejects_large_and_binary():
    assert is_scannable("src/a.py", 100, 512 * 1024)
    assert not is_scannable("assets/logo.png", 100, 512 * 1024)
    assert not is_scannable("src/a.py", 900_000, 512 * 1024)


async def test_list_org_repos_sorted_and_capped():
    client = GitHubClient(fixture_dir=FIXTURES)
    repos = await client.list_org_repos("acme", max_repos=1)
    assert [r["name"] for r in repos] == ["widgets"]  # top by stars, capped to 1


async def test_get_tree_and_raw():
    client = GitHubClient(fixture_dir=FIXTURES)
    tree = await client.get_tree("acme", "widgets", "HEAD")
    assert {"path": "src/config.py", "size": 120} in tree
    raw = await client.get_raw("acme", "widgets", "HEAD", "src/config.py")
    assert "AKIAIOSFODNN7EXAMPLE" in raw
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd github-exposure-scanner && uv run pytest tests/test_github_api.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 4: Implement `github_api.py`**

`github-exposure-scanner/github_exposure_scanner/github_api.py`:
```python
"""GitHub REST client for the exposure scanner, with an offline fixture mode.

Live mode uses the GitHub REST API (api.github.com) for repo/tree metadata
and raw.githubusercontent.com for file content, sending ``GITHUB_TOKEN`` when
present. Fixture mode (``GHX_FIXTURE_DIR``) reads canned files instead — used
by the test suite and CI so no network or rate limits are involved.
"""

import json
import os

import httpx

API_BASE = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"

# Extensions we never scan (binary / non-text).
_BINARY_EXT = {
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".zip", ".gz", ".tar",
    ".jar", ".class", ".so", ".dylib", ".dll", ".exe", ".bin", ".woff",
    ".woff2", ".ttf", ".eot", ".mp4", ".mp3", ".wav", ".parquet", ".lock",
}


class RateLimitError(RuntimeError):
    """Raised when GitHub returns a rate-limit response."""


def is_scannable(path: str, size: int, max_bytes: int) -> bool:
    ext = os.path.splitext(path)[1].lower()
    if ext in _BINARY_EXT:
        return False
    return 0 < size <= max_bytes


class GitHubClient:
    def __init__(self, token: str | None = None, fixture_dir: str | None = None):
        self._fixture_dir = fixture_dir
        self._token = token
        self._client: httpx.AsyncClient | None = None

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            headers = {"Accept": "application/vnd.github+json"}
            if self._token:
                headers["Authorization"] = f"Bearer {self._token}"
            self._client = httpx.AsyncClient(headers=headers, timeout=30.0, follow_redirects=True)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    # --- fixture helpers ---
    def _fx(self, *parts: str) -> str:
        return os.path.join(self._fixture_dir, *parts)

    def _read_fixture_json(self, *parts: str):
        with open(self._fx(*parts), encoding="utf-8") as f:
            return json.load(f)

    # --- API surface ---
    async def _get_json(self, url: str, params: dict | None = None):
        resp = await self._http().get(url, params=params)
        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            raise RateLimitError("GitHub API rate limit exceeded (set GITHUB_TOKEN to raise it)")
        resp.raise_for_status()
        return resp.json()

    async def list_org_repos(self, org: str, max_repos: int) -> list[dict]:
        if self._fixture_dir:
            repos = self._read_fixture_json("orgs", org, "repos.json")
        else:
            repos = []
            page = 1
            while True:
                batch = await self._get_json(
                    f"{API_BASE}/orgs/{org}/repos",
                    params={"per_page": 100, "page": page, "type": "public"},
                )
                if not batch:
                    break
                repos.extend(batch)
                if len(batch) < 100:
                    break
                page += 1
        repos = [r for r in repos if not r.get("archived") and not r.get("fork")]
        repos.sort(key=lambda r: r.get("stargazers_count", 0), reverse=True)
        return repos[:max_repos]

    async def get_repo(self, org: str, repo: str) -> dict:
        if self._fixture_dir:
            return self._read_fixture_json("repos", org, repo, "repo.json")
        return await self._get_json(f"{API_BASE}/repos/{org}/{repo}")

    async def get_head_sha(self, org: str, repo: str, branch: str) -> str:
        if self._fixture_dir:
            return "HEAD"
        data = await self._get_json(f"{API_BASE}/repos/{org}/{repo}/commits/{branch}")
        return data["sha"]

    async def get_tree(self, org: str, repo: str, sha: str) -> list[dict]:
        if self._fixture_dir:
            return self._read_fixture_json("repos", org, repo, "tree.json")
        data = await self._get_json(
            f"{API_BASE}/repos/{org}/{repo}/git/trees/{sha}", params={"recursive": "1"}
        )
        return [
            {"path": e["path"], "size": e.get("size", 0)}
            for e in data.get("tree", [])
            if e.get("type") == "blob"
        ]

    async def get_raw(self, org: str, repo: str, sha: str, path: str) -> str:
        if self._fixture_dir:
            with open(self._fx("repos", org, repo, "raw", path), encoding="utf-8", errors="replace") as f:
                return f.read()
        resp = await self._http().get(f"{RAW_BASE}/{org}/{repo}/{sha}/{path}")
        resp.raise_for_status()
        return resp.text


def make_client() -> GitHubClient:
    return GitHubClient(
        token=os.environ.get("GITHUB_TOKEN"),
        fixture_dir=os.environ.get("GHX_FIXTURE_DIR"),
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd github-exposure-scanner && uv run pytest tests/test_github_api.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/github_api.py github-exposure-scanner/tests/test_github_api.py github-exposure-scanner/tests/fixtures/
git commit -m "github-exposure-scanner: GitHub client with offline fixture mode"
```

---

### Task 4: Pydantic result models

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/models.py`
- Test: `github-exposure-scanner/tests/test_models.py`

**Interfaces:**
- Produces:
  - `class Target(BaseModel): org: str; repo: str | None = None`
  - `def parse_target(raw: str) -> Target` — `"acme"` → `Target(org="acme")`, `"acme/widgets"` → `Target(org="acme", repo="widgets")`
  - `class AirtableValidationResult(BaseModel): status: str; base: str | None = None; reason: str | None = None`
  - `class AirtablePublishResult(BaseModel): status: str; base: str | None = None; table: str | None = None; rows: int | None = None; reason: str | None = None`

- [ ] **Step 1: Write the failing tests**

`github-exposure-scanner/tests/test_models.py`:
```python
import pytest

from github_exposure_scanner.models import Target, parse_target


def test_parse_bare_org():
    assert parse_target("acme") == Target(org="acme")


def test_parse_org_repo():
    assert parse_target("acme/widgets") == Target(org="acme", repo="widgets")


def test_parse_strips_whitespace():
    assert parse_target("  acme/widgets  ") == Target(org="acme", repo="widgets")


def test_parse_rejects_empty():
    with pytest.raises(ValueError):
        parse_target("   ")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd github-exposure-scanner && uv run pytest tests/test_models.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `models.py`**

`github-exposure-scanner/github_exposure_scanner/models.py`:
```python
"""Pydantic models and small parsers for the exposure scanner."""

from pydantic import BaseModel


class Target(BaseModel):
    org: str
    repo: str | None = None


def parse_target(raw: str) -> Target:
    """Parse a target string: ``"org"`` or ``"org/repo"``."""
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError("empty target")
    if "/" in cleaned:
        org, repo = cleaned.split("/", 1)
        return Target(org=org.strip(), repo=repo.strip())
    return Target(org=cleaned)


class AirtableValidationResult(BaseModel):
    status: str  # "ok" | "skipped"
    base: str | None = None
    reason: str | None = None


class AirtablePublishResult(BaseModel):
    status: str  # "published" | "skipped"
    base: str | None = None
    table: str | None = None
    rows: int | None = None
    reason: str | None = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd github-exposure-scanner && uv run pytest tests/test_models.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/models.py github-exposure-scanner/tests/test_models.py
git commit -m "github-exposure-scanner: result models and target parser"
```

---

### Task 5: Repo listing and secret scanning tasks

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/scan.py`
- Test: `github-exposure-scanner/tests/test_scan.py`

**Interfaces:**
- Consumes: `github_api.GitHubClient`, `models.Target`/`parse_target`, `rules.scan_text`.
- Produces:
  - `REPO_FIELDS: list[str]` = `["org", "repo", "repo_url", "default_branch", "head_sha", "stars", "pushed_at", "language", "size_kb", "files_to_scan", "list_error"]`
  - `FINDING_FIELDS: list[str]` = `["org", "repo", "path", "line", "rule_id", "secret_type", "severity", "masked_value", "permalink", "repo_stars", "detected_at"]`
  - `async def list_repos_impl(targets: list[str], max_repos: int, client, now: str) -> Object`
  - `async def scan_repos_impl(repos: Object, max_file_kb: int, client) -> Object`
  - `@task list_repos(targets, max_repos=25) -> Object` and `@task scan_repos(repos, max_file_kb=512) -> Object` (thin wrappers using `make_client()` and `datetime.now(UTC)`).

- [ ] **Step 1: Write the failing tests**

`github-exposure-scanner/tests/test_scan.py`:
```python
import os

from aaiclick import ORIENT_DICT
from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_list_repos_expands_org():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme"], max_repos=25, client=client, now="2026-07-17")
        data = await repos.data(orient=ORIENT_DICT)
        assert set(data["repo"]) == {"widgets", "docs"}
        widgets = data["repo"].index("widgets")
        assert data["stars"][widgets] == 1200
        assert data["files_to_scan"][widgets] == 2  # config.py + README.md, png excluded


async def test_scan_finds_planted_secret_redacted():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme/widgets"], max_repos=25, client=client, now="2026-07-17")
        findings = await scan_repos_impl(repos, max_file_kb=512, client=client)
        data = await findings.data(orient=ORIENT_DICT)
        assert "AWS Key" in data["secret_type"]
        idx = data["secret_type"].index("AWS Key")
        assert data["path"][idx] == "src/config.py"
        assert data["repo_stars"][idx] == 1200
        assert "AKIAIOSFODNN7EXAMPLE" not in "".join(data["masked_value"])
        assert "github.com/acme/widgets/blob/" in data["permalink"][idx]


async def test_list_error_recorded_not_raised():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["ghost/missing"], max_repos=25, client=client, now="2026-07-17")
        data = await repos.data(orient=ORIENT_DICT)
        assert data["list_error"][0] is not None and data["list_error"][0] != ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `scan.py`**

`github-exposure-scanner/github_exposure_scanner/scan.py`:
```python
"""Repo enumeration and secret-scanning tasks.

``list_repos_impl`` turns targets into a repos ``Object`` (one row per repo,
with a ``list_error`` column for repos that couldn't be listed). ``scan_repos_impl``
fetches each scannable file's content, runs the regex rules, and returns a
redacted findings ``Object``. File content is scanned in Python and discarded —
never stored.
"""

from datetime import UTC, datetime

from aaiclick import ORIENT_DICT, FieldSpec, create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .github_api import GitHubClient, is_scannable, make_client
from .models import parse_target
from .rules import scan_text

REPO_FIELDS = [
    "org", "repo", "repo_url", "default_branch", "head_sha", "stars",
    "pushed_at", "language", "size_kb", "files_to_scan", "list_error",
]
FINDING_FIELDS = [
    "org", "repo", "path", "line", "rule_id", "secret_type", "severity",
    "masked_value", "permalink", "repo_stars", "detected_at",
]

_REPO_TYPES = {
    "stars": FieldSpec(type="Int64"),
    "size_kb": FieldSpec(type="Int64"),
    "files_to_scan": FieldSpec(type="Int64"),
    "list_error": FieldSpec(type="String", nullable=True),
}
_FINDING_TYPES = {"line": FieldSpec(type="UInt32"), "repo_stars": FieldSpec(type="Int64")}


def _empty_columns(fields: list[str]) -> dict[str, list]:
    return {f: [] for f in fields}


async def list_repos_impl(targets: list[str], max_repos: int, client: GitHubClient, now: str) -> Object:
    cols = _empty_columns(REPO_FIELDS)

    async def _add_repo(org: str, repo_meta: dict) -> None:
        repo = repo_meta["name"]
        branch = repo_meta.get("default_branch", "main")
        try:
            sha = await client.get_head_sha(org, repo, branch)
            tree = await client.get_tree(org, repo, sha)
            files_to_scan = sum(1 for e in tree if is_scannable(e["path"], e.get("size", 0), 512 * 1024))
            error = None
        except Exception as exc:  # noqa: BLE001 — per-repo isolation
            sha, files_to_scan, error = "", 0, f"{type(exc).__name__}: {exc}"
        cols["org"].append(org)
        cols["repo"].append(repo)
        cols["repo_url"].append(f"https://github.com/{org}/{repo}")
        cols["default_branch"].append(branch)
        cols["head_sha"].append(sha)
        cols["stars"].append(int(repo_meta.get("stargazers_count", 0)))
        cols["pushed_at"].append(str(repo_meta.get("pushed_at", "")))
        cols["language"].append(str(repo_meta.get("language") or ""))
        cols["size_kb"].append(int(repo_meta.get("size", 0)))
        cols["files_to_scan"].append(files_to_scan)
        cols["list_error"].append(error)

    for raw in targets:
        target = parse_target(raw)
        try:
            if target.repo:
                metas = [await client.get_repo(target.org, target.repo)]
            else:
                metas = await client.list_org_repos(target.org, max_repos)
        except Exception as exc:  # noqa: BLE001 — record listing failure as a row
            cols["org"].append(target.org)
            cols["repo"].append(target.repo or "*")
            cols["repo_url"].append(f"https://github.com/{target.org}")
            cols["default_branch"].append("")
            cols["head_sha"].append("")
            cols["stars"].append(0)
            cols["pushed_at"].append("")
            cols["language"].append("")
            cols["size_kb"].append(0)
            cols["files_to_scan"].append(0)
            cols["list_error"].append(f"{type(exc).__name__}: {exc}")
            continue
        for meta in metas:
            await _add_repo(target.org, meta)

    return await create_object_from_value(cols, name="ghx_repos", scope="job", fields=_REPO_TYPES)


async def scan_repos_impl(repos: Object, max_file_kb: int, client: GitHubClient) -> Object:
    max_bytes = max_file_kb * 1024
    repo_rows = await repos.data(orient=ORIENT_DICT)
    n = len(repo_rows["repo"])
    detected_at = datetime.now(UTC).strftime("%Y-%m-%d")
    cols = _empty_columns(FINDING_FIELDS)

    for i in range(n):
        if repo_rows["list_error"][i]:
            continue
        org, repo = repo_rows["org"][i], repo_rows["repo"][i]
        sha, stars = repo_rows["head_sha"][i], repo_rows["stars"][i]
        try:
            tree = await client.get_tree(org, repo, sha)
        except Exception:  # noqa: BLE001 — skip repos whose tree can't be refetched
            continue
        for entry in tree:
            path, size = entry["path"], entry.get("size", 0)
            if not is_scannable(path, size, max_bytes):
                continue
            try:
                text = await client.get_raw(org, repo, sha, path)
            except Exception:  # noqa: BLE001 — skip unreadable files
                continue
            for f in scan_text(path, text):
                cols["org"].append(org)
                cols["repo"].append(repo)
                cols["path"].append(f.path)
                cols["line"].append(f.line)
                cols["rule_id"].append(f.rule_id)
                cols["secret_type"].append(f.secret_type)
                cols["severity"].append(f.severity)
                cols["masked_value"].append(f.masked_value)
                cols["permalink"].append(f"https://github.com/{org}/{repo}/blob/{sha}/{f.path}#L{f.line}")
                cols["repo_stars"].append(int(stars))
                cols["detected_at"].append(detected_at)

    return await create_object_from_value(cols, name="ghx_findings", scope="job", fields=_FINDING_TYPES)


@task
async def list_repos(targets: list[str], max_repos: int = 25) -> Object:
    client = make_client()
    try:
        return await list_repos_impl(targets, max_repos, client, datetime.now(UTC).strftime("%Y-%m-%d"))
    finally:
        await client.aclose()


@task
async def scan_repos(repos: Object, max_file_kb: int = 512) -> Object:
    client = make_client()
    try:
        return await scan_repos_impl(repos, max_file_kb, client)
    finally:
        await client.aclose()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd github-exposure-scanner && uv run pytest tests/test_scan.py -v`
Expected: PASS (3 tests). If the `ghost/missing` fixture path is absent, the `get_repo` open() raises `FileNotFoundError`, which is exactly the recorded `list_error` — no fixture needed for that case.

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/scan.py github-exposure-scanner/tests/test_scan.py
git commit -m "github-exposure-scanner: repo listing and secret scanning tasks"
```

---

### Task 6: Exposure scoring task

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/score.py`
- Test: `github-exposure-scanner/tests/test_score.py`

**Interfaces:**
- Consumes: repos `Object` (Task 5 `REPO_FIELDS`), findings `Object` (Task 5 `FINDING_FIELDS`), `rules.SEVERITY_WEIGHT`.
- Produces:
  - `SUMMARY_FIELDS: list[str]` = `["org", "repos_scanned", "files_scanned", "total_findings", "critical", "high", "medium", "low", "exposure_score", "risk_band", "top_secret_type", "scan_errors"]`
  - `def risk_band(score: int) -> str` — `0→"Clean"`, `1-9→"Low"`, `10-29→"Medium"`, `30-79→"High"`, `>=80→"Critical"`
  - `def compute_score(counts: dict[str, int], flagged_stars: int) -> int` — `base = Σ severity_weight×count`; `score = round(base × (1 + log10(1 + flagged_stars)))`
  - `async def score_exposure_impl(repos: Object, findings: Object) -> Object`
  - `@task score_exposure(repos, findings) -> Object`

- [ ] **Step 1: Write the failing tests**

`github-exposure-scanner/tests/test_score.py`:
```python
import math
import os

from aaiclick import ORIENT_DICT
from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl
from github_exposure_scanner.score import compute_score, risk_band, score_exposure_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def test_risk_band_thresholds():
    assert risk_band(0) == "Clean"
    assert risk_band(5) == "Low"
    assert risk_band(10) == "Medium"
    assert risk_band(30) == "High"
    assert risk_band(80) == "Critical"


def test_compute_score_weights_and_popularity():
    counts = {"Critical": 1, "High": 0, "Medium": 0, "Low": 0}
    assert compute_score(counts, flagged_stars=0) == 10
    assert compute_score(counts, flagged_stars=1200) == round(10 * (1 + math.log10(1201)))


async def test_score_exposure_summary():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17")
        findings = await scan_repos_impl(repos, 512, client)
        summary = await score_exposure_impl(repos, findings)
        data = await summary.data(orient=ORIENT_DICT)
        i = data["org"].index("acme")
        assert data["repos_scanned"][i] == 1
        assert data["critical"][i] >= 1
        assert data["exposure_score"][i] > 0
        assert data["risk_band"][i] in {"Low", "Medium", "High", "Critical"}
        assert data["top_secret_type"][i] == "AWS Key"
        assert data["scan_errors"][i] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd github-exposure-scanner && uv run pytest tests/test_score.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `score.py`**

`github-exposure-scanner/github_exposure_scanner/score.py`:
```python
"""Per-org exposure scoring — SQL aggregation plus a small scoring formula.

Heavy grouping runs in ClickHouse (via ``Object`` group-bys); the score and
risk band are computed in Python so the formula stays unit-testable.
"""

import math

from aaiclick import ORIENT_DICT, FieldSpec, create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .rules import SEVERITY_WEIGHT

SUMMARY_FIELDS = [
    "org", "repos_scanned", "files_scanned", "total_findings", "critical",
    "high", "medium", "low", "exposure_score", "risk_band", "top_secret_type",
    "scan_errors",
]

_SUMMARY_TYPES = {
    "repos_scanned": FieldSpec(type="Int64"),
    "files_scanned": FieldSpec(type="Int64"),
    "total_findings": FieldSpec(type="Int64"),
    "critical": FieldSpec(type="Int64"),
    "high": FieldSpec(type="Int64"),
    "medium": FieldSpec(type="Int64"),
    "low": FieldSpec(type="Int64"),
    "exposure_score": FieldSpec(type="Int64"),
    "scan_errors": FieldSpec(type="Int64"),
}


def risk_band(score: int) -> str:
    if score <= 0:
        return "Clean"
    if score < 10:
        return "Low"
    if score < 30:
        return "Medium"
    if score < 80:
        return "High"
    return "Critical"


def compute_score(counts: dict[str, int], flagged_stars: int) -> int:
    base = sum(SEVERITY_WEIGHT[sev] * counts.get(sev, 0) for sev in SEVERITY_WEIGHT)
    return round(base * (1 + math.log10(1 + flagged_stars)))


async def score_exposure_impl(repos: Object, findings: Object) -> Object:
    # Per-org repo aggregates via SQL.
    repo_agg = await repos.group_by("org").agg(
        {"repo": "count", "files_to_scan": "sum", "list_error": "count"}
    )
    repo_data = await repo_agg.data(orient=ORIENT_DICT)
    # ``list_error`` is Nullable — count() counts non-null entries = scan errors.

    finding_data = await findings.data(orient=ORIENT_DICT)

    orgs = list(repo_data["org"])
    per_org: dict[str, dict] = {
        org: {
            "repos_scanned": repo_data["repo"][idx],
            "files_scanned": repo_data["files_to_scan"][idx],
            "scan_errors": repo_data["list_error"][idx],
            "counts": {"Critical": 0, "High": 0, "Medium": 0, "Low": 0},
            "types": {},
            "flagged_repos": set(),
        }
        for idx, org in enumerate(orgs)
    }

    for i in range(len(finding_data["org"])):
        org = finding_data["org"][i]
        bucket = per_org.setdefault(
            org,
            {"repos_scanned": 0, "files_scanned": 0, "scan_errors": 0,
             "counts": {"Critical": 0, "High": 0, "Medium": 0, "Low": 0},
             "types": {}, "flagged_repos": set()},
        )
        sev = finding_data["severity"][i]
        bucket["counts"][sev] = bucket["counts"].get(sev, 0) + 1
        stype = finding_data["secret_type"][i]
        bucket["types"][stype] = bucket["types"].get(stype, 0) + 1
        bucket["flagged_repos"].add((finding_data["repo"][i], finding_data["repo_stars"][i]))

    cols: dict[str, list] = {f: [] for f in SUMMARY_FIELDS}
    for org, b in per_org.items():
        counts = b["counts"]
        total = sum(counts.values())
        flagged_stars = sum(stars for _, stars in b["flagged_repos"])
        score = compute_score(counts, flagged_stars)
        top_type = max(b["types"], key=b["types"].get) if b["types"] else ""
        cols["org"].append(org)
        cols["repos_scanned"].append(int(b["repos_scanned"]))
        cols["files_scanned"].append(int(b["files_scanned"]))
        cols["total_findings"].append(int(total))
        cols["critical"].append(counts["Critical"])
        cols["high"].append(counts["High"])
        cols["medium"].append(counts["Medium"])
        cols["low"].append(counts["Low"])
        cols["exposure_score"].append(score)
        cols["risk_band"].append(risk_band(score))
        cols["top_secret_type"].append(top_type)
        cols["scan_errors"].append(int(b["scan_errors"]))

    return await create_object_from_value(cols, name="ghx_summary", scope="job", fields=_SUMMARY_TYPES)


@task
async def score_exposure(repos: Object, findings: Object) -> Object:
    return await score_exposure_impl(repos, findings)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd github-exposure-scanner && uv run pytest tests/test_score.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/score.py github-exposure-scanner/tests/test_score.py
git commit -m "github-exposure-scanner: per-org exposure scoring"
```

---

### Task 7: Airtable publishing tasks

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/airtable.py`
- Test: `github-exposure-scanner/tests/test_airtable.py`

**Interfaces:**
- Consumes: findings `Object` (`FINDING_FIELDS`), summary `Object` (`SUMMARY_FIELDS`), `models.AirtableValidationResult`/`AirtablePublishResult`.
- Produces:
  - `@task validate_airtable_credentials() -> AirtableValidationResult` — `skipped` when env unset; otherwise probes `/meta/whoami` + `/meta/bases/{base}/tables`.
  - `@task publish_findings(findings: Object, validation) -> AirtablePublishResult` — table `AIRTABLE_FINDINGS_TABLE` (default `"GitHub Findings"`).
  - `@task publish_summary(summary: Object, validation) -> AirtablePublishResult` — table `AIRTABLE_SUMMARY_TABLE` (default `"GitHub Exposure Summary"`).

This task adapts the proven Airtable REST helpers from `imdb-dataset-builder/imdb_dataset_builder/airtable.py` (retry/backoff, batch create/delete, schema ensure). Reuse that module's `_airtable_request`, `_arequest`, `_chunks`, `_parse_base_id`, `_table_url`, `_list_all_record_ids`, `_delete_records`, `_create_records`, `_ensure_table_schema` verbatim, then add the two publish tasks below.

- [ ] **Step 1: Write the failing tests** (only the offline-safe skip paths are unit-tested; live publishing is covered manually via `--airtable`)

`github-exposure-scanner/tests/test_airtable.py`:
```python
import os

from aaiclick.data.data_context import data_context

from github_exposure_scanner.airtable import (
    _field_records,
    validate_airtable_credentials,
)
from github_exposure_scanner.models import AirtableValidationResult
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl
from github_exposure_scanner.github_api import GitHubClient

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_validate_skips_without_credentials(monkeypatch):
    monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
    monkeypatch.delenv("AIRTABLE_BASE_ID", raising=False)
    result = await validate_airtable_credentials.fn()  # call undecorated impl
    assert result.status == "skipped"


async def test_field_records_shape():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17")
        findings = await scan_repos_impl(repos, 512, client)
        records = await _field_records(findings, ["Organization", "Repository"], {"Organization": "org", "Repository": "repo"})
        assert records and records[0]["fields"]["Organization"] == "acme"
```

Note: if `@task` does not expose `.fn`, refactor `validate_airtable_credentials` to delegate to a module-level `async def _validate_impl()` and call that in the test instead.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd github-exposure-scanner && uv run pytest tests/test_airtable.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `airtable.py`**

Start by copying the helper functions listed above from `imdb-dataset-builder/imdb_dataset_builder/airtable.py`. Then add:

```python
import asyncio
import os

from aaiclick import ORIENT_DICT
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult, AirtableValidationResult

FINDINGS_TABLE = os.environ.get("AIRTABLE_FINDINGS_TABLE", "GitHub Findings")
SUMMARY_TABLE = os.environ.get("AIRTABLE_SUMMARY_TABLE", "GitHub Exposure Summary")

# (org column -> Airtable field name) mappings and field schemas.
_FINDINGS_MAP = {
    "Organization": "org", "Repository": "repo", "File path": "path", "Line": "line",
    "Permalink": "permalink", "Secret type": "secret_type", "Severity": "severity",
    "Masked value": "masked_value", "Repo stars": "repo_stars", "Detected at": "detected_at",
}
_FINDINGS_SCHEMA = [
    {"name": "Organization", "type": "singleLineText"},
    {"name": "Repository", "type": "singleLineText"},
    {"name": "File path", "type": "singleLineText"},
    {"name": "Line", "type": "number", "options": {"precision": 0}},
    {"name": "Permalink", "type": "url"},
    {"name": "Secret type", "type": "singleLineText"},
    {"name": "Severity", "type": "singleLineText"},
    {"name": "Masked value", "type": "singleLineText"},
    {"name": "Repo stars", "type": "number", "options": {"precision": 0}},
    {"name": "Detected at", "type": "singleLineText"},
    {"name": "Status", "type": "singleLineText"},
]
_SUMMARY_MAP = {
    "Organization": "org", "Repos scanned": "repos_scanned", "Files scanned": "files_scanned",
    "Total findings": "total_findings", "Critical": "critical", "High": "high",
    "Medium": "medium", "Low": "low", "Exposure score": "exposure_score",
    "Risk band": "risk_band", "Top secret type": "top_secret_type", "Scan errors": "scan_errors",
}
_SUMMARY_SCHEMA = (
    [{"name": "Organization", "type": "singleLineText"}]
    + [{"name": n, "type": "number", "options": {"precision": 0}}
       for n in ["Repos scanned", "Files scanned", "Total findings", "Critical",
                 "High", "Medium", "Low", "Exposure score", "Scan errors"]]
    + [{"name": "Risk band", "type": "singleLineText"},
       {"name": "Top secret type", "type": "singleLineText"}]
)


async def _field_records(obj: Object, field_names: list[str], mapping: dict[str, str]) -> list[dict]:
    rows = await obj.data(orient=ORIENT_DICT)
    n = len(rows[next(iter(mapping.values()))]) if rows else 0
    records = []
    for i in range(n):
        fields = {fname: rows[col][i] for fname, col in mapping.items() if col in rows}
        records.append({"fields": fields})
    return records


async def _validate_impl() -> AirtableValidationResult:
    api_key = os.environ.get("AIRTABLE_API_KEY")
    raw_base_id = os.environ.get("AIRTABLE_BASE_ID")
    if not (api_key and raw_base_id):
        return AirtableValidationResult(status="skipped", reason="AIRTABLE_API_KEY/BASE_ID not set")
    base_id = _parse_base_id(raw_base_id)
    await _arequest("GET", "https://api.airtable.com/v0/meta/whoami", api_key)
    await _arequest("GET", f"https://api.airtable.com/v0/meta/bases/{base_id}/tables", api_key)
    return AirtableValidationResult(status="ok", base=base_id)


async def _publish_impl(obj: Object, validation: AirtableValidationResult, table: str,
                        schema: list[dict], mapping: dict[str, str]) -> AirtablePublishResult:
    if validation.status == "skipped":
        return AirtablePublishResult(status="skipped", reason=validation.reason, table=table)
    api_key = os.environ["AIRTABLE_API_KEY"]
    base_id = _parse_base_id(os.environ["AIRTABLE_BASE_ID"])
    records = await _field_records(obj, [f["name"] for f in schema], mapping)
    await _ensure_table_schema_with(api_key, base_id, table, schema)
    existing = await _list_all_record_ids(api_key, base_id, table)
    for batch in _chunks(existing, AIRTABLE_BATCH):
        await _delete_records(api_key, base_id, table, batch)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)
    for batch in _chunks(records, AIRTABLE_BATCH):
        await _create_records(api_key, base_id, table, batch)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)
    return AirtablePublishResult(status="published", base=base_id, table=table, rows=len(records))


@task
async def validate_airtable_credentials() -> AirtableValidationResult:
    return await _validate_impl()


@task
async def publish_findings(findings: Object, validation: AirtableValidationResult) -> AirtablePublishResult:
    return await _publish_impl(findings, validation, FINDINGS_TABLE, _FINDINGS_SCHEMA, _FINDINGS_MAP)


@task
async def publish_summary(summary: Object, validation: AirtableValidationResult) -> AirtablePublishResult:
    return await _publish_impl(summary, validation, SUMMARY_TABLE, _SUMMARY_SCHEMA, _SUMMARY_MAP)
```

Rename the copied `_ensure_table_schema` to a generic `_ensure_table_schema_with(api_key, base_id, table, schema)` that takes the schema list as a parameter (the imdb version hardcodes `_FIELD_SCHEMA`). In the test, call `_validate_impl()` directly (not the `@task`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd github-exposure-scanner && uv run pytest tests/test_airtable.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/airtable.py github-exposure-scanner/tests/test_airtable.py
git commit -m "github-exposure-scanner: Airtable publishing (findings + summary)"
```

---

### Task 8: Report rendering task

**Files:**
- Create: `github-exposure-scanner/github_exposure_scanner/report.py`
- Test: `github-exposure-scanner/tests/test_report.py`

**Interfaces:**
- Consumes: repos/findings/summary `Object`s, `AirtablePublishResult`s.
- Produces: `@task generate_report(repos, findings, summary, findings_publish=None, summary_publish=None) -> dict`. Writes to `AAICLICK_REPORT_FILE` if set, else stdout (same pattern as imdb). Returns a small dict summary.

- [ ] **Step 1: Write the failing test**

`github-exposure-scanner/tests/test_report.py`:
```python
import os

from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.report import render_report
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl
from github_exposure_scanner.score import score_exposure_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_report_contains_sections_and_no_raw_secret():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17")
        findings = await scan_repos_impl(repos, 512, client)
        summary = await score_exposure_impl(repos, findings)
        text = await render_report(repos, findings, summary, None, None)
    assert "GitHub Exposure" in text
    assert "AWS Key" in text
    assert "AKIAIOSFODNN7EXAMPLE" not in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_report.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `report.py`**

`github-exposure-scanner/github_exposure_scanner/report.py`:
```python
"""Report rendering for the GitHub exposure scanner."""

import os
from pathlib import Path

from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult


async def render_report(
    repos: Object,
    findings: Object,
    summary: Object,
    findings_publish: AirtablePublishResult | None,
    summary_publish: AirtablePublishResult | None,
) -> str:
    lines: list[str] = ["# Company Cyber Profile", "", "## GitHub Exposure", ""]

    lines.append("### Exposure Summary")
    lines.append("")
    lines.append(await summary.markdown())
    lines.append("")

    lines.append("### Scanned Repositories")
    lines.append("")
    repos_view = repos[["org", "repo", "stars", "language", "files_to_scan", "list_error"]]
    lines.append(await repos_view.markdown(truncate={"list_error": 40}))
    lines.append("")

    lines.append("### Findings (redacted)")
    lines.append("")
    total_findings = await (await findings["org"].count()).data()
    if total_findings:
        findings_view = findings[
            ["org", "repo", "path", "line", "secret_type", "severity", "masked_value", "permalink"]
        ].view(order_by="repo_stars DESC", limit=100)
        lines.append(await findings_view.markdown(truncate={"path": 40, "permalink": 60}))
    else:
        lines.append("_No leaked secrets detected._")
    lines.append("")

    lines.append("### Airtable")
    lines.append("")
    for label, pub in [("Findings", findings_publish), ("Summary", summary_publish)]:
        if pub is None:
            lines.append(f"- {label}: not requested")
        elif pub.status == "published":
            lines.append(f"- {label}: published {pub.rows} rows to {pub.base}/{pub.table}")
        else:
            lines.append(f"- {label}: skipped ({pub.reason})")
    lines.append("")
    lines.append("> Secrets are shown only as masked fingerprints. This scan reads public data for defensive attack-surface assessment.")
    return "\n".join(lines)


@task
async def generate_report(
    repos: Object,
    findings: Object,
    summary: Object,
    findings_publish: AirtablePublishResult | None = None,
    summary_publish: AirtablePublishResult | None = None,
) -> dict:
    rendered = await render_report(repos, findings, summary, findings_publish, summary_publish)
    report_file = os.environ.get("AAICLICK_REPORT_FILE")
    if report_file:
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        Path(report_file).write_text(rendered)
    else:
        print(rendered)

    total_findings = await (await findings["org"].count()).data()
    return {
        "repos_scanned": await (await repos["org"].count()).data(),
        "total_findings": total_findings,
        "findings_airtable": findings_publish.status if findings_publish else "skipped",
        "summary_airtable": summary_publish.status if summary_publish else "skipped",
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd github-exposure-scanner && uv run pytest tests/test_report.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/report.py github-exposure-scanner/tests/test_report.py
git commit -m "github-exposure-scanner: report rendering"
```

---

### Task 9: Job wiring and end-to-end fixture test

**Files:**
- Modify: `github-exposure-scanner/github_exposure_scanner/__init__.py`
- Test: `github-exposure-scanner/tests/test_e2e.py`

**Interfaces:**
- Consumes: all tasks from Tasks 5–8.
- Produces: `@job("github_exposure_scanner") def exposure_pipeline(targets=..., max_repos=25, max_file_kb=512, publish_airtable=False)`; `async def main(**kwargs)` registers it.

- [ ] **Step 1: Write the failing end-to-end test**

`github-exposure-scanner/tests/test_e2e.py` (exercises the whole data flow offline by chaining the `_impl` functions — no Postgres/worker needed):
```python
import os

from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.report import render_report
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl
from github_exposure_scanner.score import score_exposure_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_full_flow_offline():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme"], 25, client, "2026-07-17")
        findings = await scan_repos_impl(repos, 512, client)
        summary = await score_exposure_impl(repos, findings)
        report = await render_report(repos, findings, summary, None, None)
    assert "acme" in report
    assert "Exposure Summary" in report
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd github-exposure-scanner && uv run pytest tests/test_e2e.py -v`
Expected: FAIL only if imports resolve but flow errors; if `exposure_pipeline` isn't referenced here it should pass once impls exist. (This test does not import `__init__` job wiring; it validates the flow. It will pass after Step 3 is done, but write it first per TDD — expect PASS immediately since impls already exist, which is acceptable for an integration test.)

- [ ] **Step 3: Wire the job in `__init__.py`**

Replace `github-exposure-scanner/github_exposure_scanner/__init__.py`:
```python
"""GitHub Exposure Scanner — cyber-bot step 1 (GitHub attack-surface exposure).

Given a GitHub organization (or explicit ``org/repo`` targets), enumerate the
org's public repositories, scan current-HEAD file contents for leaked secrets
via a built-in regex rule library, score each org's exposure, and render a
redacted report. Optionally publish findings + a per-org summary to Airtable.

DAG::

    list_repos ─► scan_repos ─┬─► score_exposure ─┐
                              │                    ├─► generate_report
    validate_airtable_credentials ─┬─► publish_findings ─┘
                                   └─► publish_summary ───┘

Environment variables:
    GITHUB_TOKEN         — raises GitHub API rate limit (optional)
    GHX_FIXTURE_DIR      — offline fixture mode (tests/CI)
    AIRTABLE_API_KEY     — Airtable PAT (required with publish_airtable=True)
    AIRTABLE_BASE_ID     — Airtable base id (required with publish_airtable=True)
    AIRTABLE_FINDINGS_TABLE / AIRTABLE_SUMMARY_TABLE — table name overrides
"""

import asyncio

from aaiclick.orchestration import job

from .airtable import publish_findings, publish_summary, validate_airtable_credentials
from .report import generate_report
from .scan import list_repos, scan_repos
from .score import score_exposure

DEFAULT_TARGETS = ["octocat/Hello-World"]


@job("github_exposure_scanner")
def exposure_pipeline(
    targets: list[str] | None = None,
    max_repos: int = 25,
    max_file_kb: int = 512,
    publish_airtable: bool = False,
):
    targets = targets or DEFAULT_TARGETS
    repos = list_repos(targets=targets, max_repos=max_repos)
    findings = scan_repos(repos=repos, max_file_kb=max_file_kb)
    summary = score_exposure(repos=repos, findings=findings)

    if publish_airtable:
        validation = validate_airtable_credentials()
        findings_pub = publish_findings(findings=findings, validation=validation)
        summary_pub = publish_summary(summary=summary, validation=validation)
    else:
        findings_pub = None
        summary_pub = None

    return generate_report(
        repos=repos,
        findings=findings,
        summary=summary,
        findings_publish=findings_pub,
        summary_publish=summary_pub,
    )


async def main(**kwargs):
    created_job = await exposure_pipeline(**kwargs)
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    return created_job


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Run the full test suite**

Run: `cd github-exposure-scanner && uv run pytest -v`
Expected: PASS (all tests across all modules).

- [ ] **Step 5: Commit**

```bash
git add github-exposure-scanner/github_exposure_scanner/__init__.py github-exposure-scanner/tests/test_e2e.py
git commit -m "github-exposure-scanner: wire @job DAG and end-to-end test"
```

---

### Task 10: Shell runner, SPEC, and docs

**Files:**
- Create: `github-exposure-scanner/github-exposure-scanner.sh`
- Create: `github-exposure-scanner/SPEC.md`
- Modify: `docs/example_projects.md` (if it exists — add a snippet include for the new README)

**Interfaces:**
- Produces: an executable `.sh` that registers the job, runs a worker, polls, and prints the report — mirroring `imdb-dataset-builder.sh`.

- [ ] **Step 1: Write the shell runner**

`github-exposure-scanner/github-exposure-scanner.sh` (model on `imdb-dataset-builder.sh`; parse `--targets "a,b"`, `--max-repos N`, `--max-file-kb N`, `--airtable`, `--local-setup`; build a `--params` JSON with `targets` as a JSON array). Key differences from imdb: the `--targets` flag splits a comma-separated list into a JSON array, e.g.:
```bash
--targets)
    IFS=',' read -ra _TARGS <<< "$2"
    _JSON_TARGS=$(printf '"%s",' "${_TARGS[@]}"); _JSON_TARGS="[${_JSON_TARGS%,}]"
    PARAMS_PARTS+=("\"targets\": $_JSON_TARGS")
    shift 2 ;;
--airtable)
    PARAMS_PARTS+=('"publish_airtable": true'); shift ;;
```
Reuse the imdb runner's env-var block (`AAICLICK_SQL_URL`, `AAICLICK_CH_URL`, `AAICLICK_LOG_DIR`), `AAICLICK_REPORT_FILE="tmp/ghx_report.md"`, worker start/poll/stop, and final `cat "$AAICLICK_REPORT_FILE"`. Set the module name to `github_exposure_scanner`.

- [ ] **Step 2: Make it executable and lint the bash**

Run: `cd github-exposure-scanner && chmod +x github-exposure-scanner.sh && bash -n github-exposure-scanner.sh`
Expected: no syntax errors.

- [ ] **Step 3: Write SPEC.md**

`github-exposure-scanner/SPEC.md`: document the rule catalog (table of rule id / secret type / severity / weight), the scoring formula (`base = Σ weight×count`, `score = round(base × (1 + log10(1 + flagged_stars)))`), risk-band thresholds, the fixture-mode contract (directory layout), the GitHub API endpoints used, rate-limit behavior, and a responsible-use note (public data only, secrets redacted, defensive ASM framing).

- [ ] **Step 4: Wire docs include (only if `docs/example_projects.md` exists)**

Check: `test -f docs/example_projects.md && echo exists`. If it exists, add a snippet include following the existing pattern in that file (per `CLAUDE.md`: READMEs are included via `pymdownx.snippets`). If it does not exist, skip this step.

- [ ] **Step 5: Run the full suite one more time**

Run: `cd github-exposure-scanner && uv run pytest -v`
Expected: PASS.

- [ ] **Step 6: Optional live smoke (manual, network required)**

Run: `cd github-exposure-scanner && GHX_FIXTURE_DIR="$(pwd)/tests/fixtures" AAICLICK_REPORT_FILE=/tmp/ghx.md uv run python -c "import asyncio; from github_exposure_scanner import main; asyncio.run(main(targets=['acme']))"` is a registration-only check (needs Postgres to execute). The authoritative offline validation is the pytest suite; the `.sh` is for real distributed runs.

- [ ] **Step 7: Commit**

```bash
git add github-exposure-scanner/github-exposure-scanner.sh github-exposure-scanner/SPEC.md docs/
git commit -m "github-exposure-scanner: shell runner, SPEC, and docs"
```

---

## Self-Review Notes

**Spec coverage:** Input parsing (Task 4) ✓; org→repos + org/repo (Task 5) ✓; regex rules (Task 2) ✓; current-HEAD API scan, no clone (Tasks 3, 5) ✓; redaction everywhere (Tasks 2, 5, 8) ✓; exposure score + risk band (Task 6) ✓; two Airtable tables gated (Tasks 7, 9) ✓; stdout report via `Object.markdown()` (Task 8) ✓; fixture mode + tests (Tasks 3, 5, 6, 8, 9) ✓; error isolation via `list_error`/`scan_errors` (Tasks 5, 6) ✓; `.sh` + README + SPEC (Tasks 1, 10) ✓.

**Type consistency:** `REPO_FIELDS`/`FINDING_FIELDS`/`SUMMARY_FIELDS` defined once in Tasks 5–6 and referenced by exact name downstream; `list_error` is `Nullable(String)` and `count()` over it yields the non-null (error) count used as `scan_errors`; `Object`s created with `scope="job"` so they persist across tasks within the orchestrated job; `parse_target` / `Target` names match between Tasks 4 and 5; Airtable `mapping` dicts key Airtable field → `Object` column and match `FINDING_FIELDS`/`SUMMARY_FIELDS`.

**Open defaults chosen:** demo target = `octocat/Hello-World` (tiny, safe, well-known); severity weights `Critical/High/Medium/Low = 10/5/2/1`; popularity multiplier `1 + log10(1 + flagged_stars)`; risk bands `Clean/Low/Medium/High/Critical` at `0 / <10 / <30 / <80 / ≥80`.
