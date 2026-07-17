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
        repos = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, 512, client, scope=None)
        summary = await score_exposure_impl(repos, findings, scope=None)
        text = await render_report(repos, findings, summary, None, None)
    assert "GitHub Exposure" in text
    assert "AWS Key" in text
    assert "AKIAIOSFODNN7EXAMPLE" not in text
