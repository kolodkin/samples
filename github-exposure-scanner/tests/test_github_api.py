import os

from github_exposure_scanner.github_api import GitHubClient, is_scannable

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def test_is_scannable_rejects_large_and_binary():
    assert is_scannable("src/a.py", 100, 512 * 1024)
    assert not is_scannable("assets/logo.png", 100, 512 * 1024)
    assert not is_scannable("src/a.py", 900_000, 512 * 1024)


async def test_list_org_repos_sorted_and_capped():
    client = GitHubClient(fixture_dir=FIXTURES)
    repos = await client.list_org_repos("acme", max_repos=1)
    assert [r["name"] for r in repos] == ["widgets"]  # top by stars, capped to 1


async def test_get_tree_and_raw():
    client = GitHubClient(fixture_dir=FIXTURES)
    tree = await client.get_tree("acme", "widgets", "HEAD")
    assert {"path": "src/config.py", "size": 120} in tree
    raw = await client.get_raw("acme", "widgets", "HEAD", "src/config.py")
    assert "AKIAIOSFODNN7EXAMPLE" in raw
