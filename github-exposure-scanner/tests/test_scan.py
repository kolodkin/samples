import os

from aaiclick import ORIENT_DICT
from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.scan import list_repos_impl, scan_repos_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_list_repos_expands_org():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme"], max_repos=25, client=client, now="2026-07-17", scope=None)
        data = await repos.data(orient=ORIENT_DICT)
        assert set(data["repo"]) == {"widgets", "docs"}
        widgets = data["repo"].index("widgets")
        assert data["stars"][widgets] == 1200
        assert data["files_to_scan"][widgets] == 2  # config.py + README.md, png excluded


async def test_scan_finds_planted_secret_redacted():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos = await list_repos_impl(["acme/widgets"], max_repos=25, client=client, now="2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, max_file_kb=512, client=client, scope=None)
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
        repos = await list_repos_impl(["ghost/missing"], max_repos=25, client=client, now="2026-07-17", scope=None)
        data = await repos.data(orient=ORIENT_DICT)
        assert data["list_error"][0] is not None and data["list_error"][0] != ""
