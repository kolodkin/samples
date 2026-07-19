"""Repo enumeration and secret-scanning building blocks.

``list_repos_impl`` turns targets into a repos ``Object`` (one row per repo,
with a ``list_error`` column for repos that couldn't be listed). ``scan_one_repo``
is the per-repo task the job fans out — it fetches that repo's files, runs the
regex rules, and appends its redacted findings into a shared findings ``Object``.
File content is scanned in Python and discarded — never stored.

``scan_repos_impl`` performs the same scan inline (no orchestration) — used by
the offline unit tests.
"""

from datetime import UTC, datetime

from aaiclick import ORIENT_DICT, FieldSpec, create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .github_api import GitHubClient, is_scannable, make_client
from .models import parse_target
from .rules import classify_context, scan_text

REPO_FIELDS = [
    "org", "repo", "repo_url", "default_branch", "head_sha", "stars",
    "pushed_at", "language", "size_kb", "files_to_scan", "list_error",
]
FINDING_FIELDS = [
    "org", "repo", "path", "line", "rule_id", "secret_type", "severity",
    "masked_value", "permalink", "repo_stars", "detected_at", "context", "confidence",
]

_REPO_TYPES = {
    "stars": FieldSpec(type="Int64"),
    "size_kb": FieldSpec(type="Int64"),
    "files_to_scan": FieldSpec(type="Int64"),
    "list_error": FieldSpec(type="String", nullable=True),
}
_FINDING_TYPES = {
    "line": FieldSpec(type="UInt32"),
    "repo_stars": FieldSpec(type="Int64"),
    "confidence": FieldSpec(type="Float64"),
}


def _empty_columns(fields: list[str]) -> dict[str, list]:
    return {f: [] for f in fields}


def _append_repo_row(
    cols: dict[str, list], *, org: str, repo: str, branch: str = "", sha: str = "", stars: int = 0,
    pushed_at: str = "", language: str = "", size_kb: int = 0, files_to_scan: int = 0,
    list_error: str | None = None,
) -> None:
    """Append one fully-populated repo row — the single writer for every column,
    so success and listing-error rows can't drift out of sync with REPO_FIELDS."""
    row = {
        "org": org, "repo": repo, "repo_url": f"https://github.com/{org}/{repo}",
        "default_branch": branch, "head_sha": sha, "stars": int(stars),
        "pushed_at": str(pushed_at), "language": str(language or ""),
        "size_kb": int(size_kb), "files_to_scan": files_to_scan, "list_error": list_error,
    }
    for key, value in row.items():
        cols[key].append(value)


async def list_repos_impl(
    targets: list[str], max_repos: int, client: GitHubClient, now: str, scope: str | None = "job"
) -> tuple[Object, dict[str, list[dict]]]:
    """Resolve targets → (repos Object, trees).

    ``trees`` maps ``"org/repo"`` to the git-tree entries fetched here, so the
    per-repo scan can reuse them instead of calling ``get_tree`` a second time.
    """
    cols = _empty_columns(REPO_FIELDS)
    trees: dict[str, list[dict]] = {}

    async def _add_repo(org: str, repo_meta: dict) -> None:
        repo = repo_meta["name"]
        branch = repo_meta.get("default_branch", "main")
        try:
            sha = await client.get_head_sha(org, repo, branch)
            tree = await client.get_tree(org, repo, sha)
            files_to_scan = sum(1 for e in tree if is_scannable(e["path"], e.get("size", 0), 512 * 1024))
            trees[f"{org}/{repo}"] = tree
            error = None
        except Exception as exc:  # noqa: BLE001 — per-repo isolation
            sha, files_to_scan, error = "", 0, f"{type(exc).__name__}: {exc}"
        _append_repo_row(
            cols, org=org, repo=repo, branch=branch, sha=sha,
            stars=repo_meta.get("stargazers_count", 0), pushed_at=repo_meta.get("pushed_at", ""),
            language=repo_meta.get("language"), size_kb=repo_meta.get("size", 0),
            files_to_scan=files_to_scan, list_error=error,
        )

    for raw in targets:
        target = parse_target(raw)
        try:
            if target.repo:
                metas = [await client.get_repo(target.org, target.repo)]
            else:
                metas = await client.list_org_repos(target.org, max_repos)
        except Exception as exc:  # noqa: BLE001 — record listing failure as a row
            _append_repo_row(cols, org=target.org, repo=target.repo or "*",
                             list_error=f"{type(exc).__name__}: {exc}")
            continue
        for meta in metas:
            await _add_repo(target.org, meta)

    obj = await create_object_from_value(cols, name="ghx_repos", scope=scope, fields=_REPO_TYPES)
    return obj, trees


async def scan_repo_findings(
    client: GitHubClient, org: str, repo: str, sha: str, stars: int, max_file_kb: int,
    detected_at: str, tree: list[dict] | None = None,
) -> dict[str, list]:
    """Scan a single repo's current-HEAD files → redacted finding columns.

    Shared by the inline path (``scan_repos_impl``) and the per-repo task
    (``scan_one_repo``) so both produce identical findings. ``tree`` may be the
    entries already fetched during listing; when omitted they are fetched here.
    """
    max_bytes = max_file_kb * 1024
    cols = _empty_columns(FINDING_FIELDS)
    if tree is None:
        try:
            tree = await client.get_tree(org, repo, sha)
        except Exception:  # noqa: BLE001 — skip repos whose tree can't be refetched
            return cols
    tree_paths = [e["path"] for e in tree]
    for entry in tree:
        path, size = entry["path"], entry.get("size", 0)
        if not is_scannable(path, size, max_bytes):
            continue
        try:
            text = await client.get_raw(org, repo, sha, path)
        except Exception:  # noqa: BLE001 — skip unreadable files
            continue
        for f in scan_text(path, text):
            context, confidence = classify_context(f.secret_type, f.path, tree_paths)
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
            cols["context"].append(context)
            cols["confidence"].append(confidence)
    return cols


async def scan_repos_impl(
    repos: Object, max_file_kb: int, client: GitHubClient, scope: str | None = "job"
) -> Object:
    """Inline (non-orchestrated) scan of every repo into one findings Object."""
    repo_rows = await repos.data(orient=ORIENT_DICT)
    n = len(repo_rows["repo"])
    detected_at = datetime.now(UTC).strftime("%Y-%m-%d")
    cols = _empty_columns(FINDING_FIELDS)

    for i in range(n):
        if repo_rows["list_error"][i]:
            continue
        one = await scan_repo_findings(
            client,
            repo_rows["org"][i],
            repo_rows["repo"][i],
            repo_rows["head_sha"][i],
            repo_rows["stars"][i],
            max_file_kb,
            detected_at,
        )
        for k, v in one.items():
            cols[k].extend(v)

    return await create_object_from_value(cols, name="ghx_findings", scope=scope, fields=_FINDING_TYPES)


async def new_findings_object(scope: str | None = "job") -> Object:
    """Create an empty findings Object with the correct schema for children to fill."""
    return await create_object_from_value(
        _empty_columns(FINDING_FIELDS), name="ghx_findings", scope=scope, fields=_FINDING_TYPES
    )


@task
async def scan_one_repo(
    org: str, repo: str, head_sha: str, stars: int, max_file_kb: int, out: Object,
    tree: list[dict] | None = None,
) -> None:
    """Scan one repo and append its redacted findings into the shared ``out`` Object.

    This is the per-repo unit the job fans out to — one task instance per
    discovered repository. ``tree`` carries the entries already fetched during
    listing so the scan doesn't re-fetch the git tree (kwargs are persisted, so
    this trades a redundant API call for a larger task row — fine at this scale).
    """
    client = make_client()
    try:
        cols = await scan_repo_findings(
            client, org, repo, head_sha, stars, max_file_kb, datetime.now(UTC).strftime("%Y-%m-%d"), tree=tree
        )
    finally:
        await client.aclose()
    if cols["org"]:  # only touch the table when this repo actually had findings
        part = await create_object_from_value(cols, fields=_FINDING_TYPES)
        await out.insert(part)
