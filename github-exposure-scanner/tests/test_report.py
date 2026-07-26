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
        repos, _ = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, 512, client, scope=None)
        summary = await score_exposure_impl(repos, findings, scope=None)
        text = await render_report(repos, findings, summary, None, None)
    assert "GitHub Exposure" in text
    assert "AWS Key" in text
    assert "AKIAIOSFODNN7EXAMPLE" not in text


async def test_report_splits_live_and_historical(tmp_path):
    from aaiclick import create_object_from_value

    from github_exposure_scanner import git_history as gh
    from github_exposure_scanner.scan import _FINDING_TYPES
    from gitutil import make_repo

    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "leak", "date": "2020-01-01",
         "files": {"live.py": "AWS = 'AKIAIOSFODNN7EXAMPLE'\n"}},
        {"message": "leak2", "date": "2020-02-01",
         "files": {"gone.py": "AWS2 = 'AKIA1234567890ABCDEF'\n"}},
        {"message": "scrub", "date": "2020-03-01", "files": {"gone.py": None}},
    ])
    cols = gh.scan_history(repo, "acme", "widgets", 1200, 512, "2026-07-20")

    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await create_object_from_value(cols, name="f", scope=None, fields=_FINDING_TYPES)
        summary = await score_exposure_impl(repos, findings, scope=None)
        text = await render_report(repos, findings, summary, None, None)

    assert "Live at HEAD" in text
    assert "Historical-only" in text
    assert "live.py" in text          # still-present secret
    assert "gone.py" in text          # removed but in history
    assert "AKIAIOSFODNN7EXAMPLE" not in text  # redacted
