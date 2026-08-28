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

Usage:
    # With a worker running, run the pipeline and block on its progress
    # (requires PostgreSQL or SQLite orchestration backend):
    python -m aaiclick execution-worker start &
    python -m aaiclick run-job github_exposure_scanner.exposure_pipeline \
        --set 'targets=["octocat"]' --progress

    # Or execute the whole DAG in-process against embedded chdb + SQLite, no
    # worker — handy for debugging:
    python -m aaiclick setup
    python -m github_exposure_scanner --params '{"targets": ["octocat"]}'

Environment variables:
    GITHUB_TOKEN         — raises GitHub API rate limit (optional)
    GITHUB_REPOS         — default targets when no explicit ``targets`` are
                           passed: comma-separated ``org|repo`` or bare ``org``
                           entries (e.g. ``"acme|widgets,octocat"``). An explicit
                           ``--targets``/``params`` overrides it.
    GHX_FIXTURE_DIR      — offline fixture mode (tests/CI)
    AIRTABLE_API_KEY     — Airtable PAT (required with publish_airtable=True)
    AIRTABLE_BASE_ID     — Airtable base id (required with publish_airtable=True)
    AIRTABLE_FINDINGS_TABLE / AIRTABLE_SUMMARY_TABLE — table name overrides
"""

import asyncio
from datetime import UTC, datetime

from aaiclick import ORIENT_DICT
from aaiclick.orchestration import job, tasks_list

from .airtable import publish_findings, publish_summary, validate_airtable_credentials
from .github_api import make_client
from .models import targets_from_env
from .report import generate_report
from .scan import list_repos_impl, new_findings_object, scan_one_repo
from .score import score_exposure

DEFAULT_TARGETS = ["octocat/Hello-World"]


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
    """Entry task: discover repos, then fan out one scan task per repo.

    The entry runs on a worker with a live context, so it resolves the repo
    list inline and builds the DAG dynamically — one ``scan_one_repo`` task per
    discovered repo, all writing into a shared findings ``Object``. Scoring,
    report, and Airtable tasks fan in via ``depends_on`` so they run only once
    every per-repo scan has completed.
    """
    # Explicit targets win; else fall back to the GITHUB_REPOS env var; else default.
    targets = targets or targets_from_env() or DEFAULT_TARGETS

    client = make_client()
    try:
        repos, trees = await list_repos_impl(
            targets, max_repos, client, datetime.now(UTC).strftime("%Y-%m-%d"),
            scope="job", max_repo_mb=None if head_only else max_repo_mb,
            allow_clone_fallback=not head_only,
        )
    finally:
        await client.aclose()

    findings = await new_findings_object(scope="job")
    rows = await repos.data(orient=ORIENT_DICT)
    children = [
        scan_one_repo(
            org=rows["org"][i],
            repo=rows["repo"][i],
            head_sha=rows["head_sha"][i],
            stars=int(rows["stars"][i]),
            max_file_kb=max_file_kb,
            out=findings,
            tree=trees.get(f'{rows["org"][i]}/{rows["repo"][i]}'),  # reuse the tree from listing
            head_only=head_only,
            max_commits=max_commits,
            max_blobs=max_blobs,
            clone_timeout=clone_timeout,
        )
        for i in range(len(rows["repo"]))
        if not rows["list_error"][i]
    ]

    summary = score_exposure(repos=repos, findings=findings)
    for child in children:
        summary.depends_on(child)  # fan-in: score only after every repo is scanned

    findings_pub = summary_pub = None
    extra = []
    if publish_airtable:
        validation = validate_airtable_credentials()
        findings_pub = publish_findings(findings=findings, validation=validation)
        summary_pub = publish_summary(summary=summary, validation=validation)
        for child in children:
            findings_pub.depends_on(child)  # publish findings only after all scans
        extra = [validation, findings_pub, summary_pub]

    report = generate_report(
        repos=repos,
        findings=findings,
        summary=summary,
        findings_publish=findings_pub,
        summary_publish=summary_pub,
    )

    return tasks_list(*children, summary, report, *extra)


async def main(**kwargs):
    created_job = await exposure_pipeline(**kwargs)
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    return created_job


if __name__ == "__main__":
    asyncio.run(main())
