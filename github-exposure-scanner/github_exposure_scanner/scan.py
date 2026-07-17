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


async def list_repos_impl(
    targets: list[str], max_repos: int, client: GitHubClient, now: str, scope: str | None = "job"
) -> Object:
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

    return await create_object_from_value(cols, name="ghx_repos", scope=scope, fields=_REPO_TYPES)


async def scan_repos_impl(
    repos: Object, max_file_kb: int, client: GitHubClient, scope: str | None = "job"
) -> Object:
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

    return await create_object_from_value(cols, name="ghx_findings", scope=scope, fields=_FINDING_TYPES)


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
