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
