import os

from aaiclick import ORIENT_DICT
from aaiclick.data.data_context import data_context

from github_exposure_scanner.github_api import GitHubClient
from github_exposure_scanner.scan import list_repos_impl, scan_repo_findings, scan_repos_impl

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_list_repos_expands_org():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme"], max_repos=25, client=client, now="2026-07-17", scope=None)
        data = await repos.data(orient=ORIENT_DICT)
        assert set(data["repo"]) == {"widgets", "docs"}
        widgets = data["repo"].index("widgets")
        assert data["stars"][widgets] == 1200
        assert data["files_to_scan"][widgets] == 2  # config.py + README.md, png excluded


async def test_scan_finds_planted_secret_redacted():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme/widgets"], max_repos=25, client=client, now="2026-07-17", scope=None)
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
        repos, _ = await list_repos_impl(["ghost/missing"], max_repos=25, client=client, now="2026-07-17", scope=None)
        data = await repos.data(orient=ORIENT_DICT)
        assert data["list_error"][0] is not None and data["list_error"][0] != ""


async def test_head_path_fills_attribution_fields():
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        repos, _ = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)
        findings = await scan_repos_impl(repos, max_file_kb=512, client=client, scope=None)
        data = await findings.data(orient=ORIENT_DICT)
        idx = data["secret_type"].index("AWS Key")
        assert data["still_present_at_head"][idx] == 1   # HEAD findings are live by definition
        assert data["commit_sha"][idx] != ""             # head sha recorded
        assert data["first_seen"][idx] == ""             # unknown on the HEAD path


async def test_scan_reuses_listing_tree_without_refetch():
    # The tree fetched during listing is reused by the scan — no second get_tree.
    client = GitHubClient(fixture_dir=FIXTURES)
    async with data_context():
        _, trees = await list_repos_impl(["acme/widgets"], 25, client, "2026-07-17", scope=None)

        async def _boom(*args, **kwargs):
            raise AssertionError("get_tree must not be called when a tree is provided")

        client.get_tree = _boom
        cols = await scan_repo_findings(
            client, "acme", "widgets", "HEAD", 1200, 512, "2026-07-17", tree=trees["acme/widgets"]
        )
        assert "AWS Key" in cols["secret_type"]
