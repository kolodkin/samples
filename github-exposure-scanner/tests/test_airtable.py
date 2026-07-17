import os

from aaiclick.data.data_context import data_context

from github_exposure_scanner.airtable import _FINDINGS_MAP, _field_records, _validate_impl
from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_validate_skips_without_credentials(monkeypatch):
    monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
    monkeypatch.delenv("AIRTABLE_BASE_ID", raising=False)
    result = await _validate_impl()
    assert result.status == "skipped"


async def test_field_records_shape():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, 512, client, scope=None)
        records = await _field_records(findings, _FINDINGS_MAP)
        assert records and records[0]["fields"]["Organization"] == "acme"
        assert "AKIAIOSFODNN7EXAMPLE" not in str(records)
