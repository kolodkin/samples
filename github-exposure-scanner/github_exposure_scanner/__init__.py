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
