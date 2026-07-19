import pytest

from github_exposure_scanner.models import (
    Target,
    parse_repos_env,
    parse_target,
    targets_from_env,
)


def test_parse_bare_org():
    assert parse_target("acme") == Target(org="acme")


def test_parse_org_repo():
    assert parse_target("acme/widgets") == Target(org="acme", repo="widgets")


def test_parse_strips_whitespace():
    assert parse_target("  acme/widgets  ") == Target(org="acme", repo="widgets")


def test_parse_rejects_empty():
    with pytest.raises(ValueError):
        parse_target("   ")


# --- GITHUB_REPOS env parsing ---


def test_parse_repos_env_pipe_becomes_slash_target():
    assert parse_repos_env("acme|widgets") == ["acme/widgets"]


def test_parse_repos_env_bare_org_passes_through():
    assert parse_repos_env("octocat") == ["octocat"]


def test_parse_repos_env_mixed_entries():
    assert parse_repos_env("acme|widgets,octocat,acme|proxytool") == [
        "acme/widgets",
        "octocat",
        "acme/proxytool",
    ]


def test_parse_repos_env_trims_whitespace_and_skips_blanks():
    assert parse_repos_env("  acme | widgets , , octocat ,") == [
        "acme/widgets",
        "octocat",
    ]


def test_parse_repos_env_empty_is_empty_list():
    assert parse_repos_env("") == []
    assert parse_repos_env("   ,  , ") == []


def test_targets_from_env_unset_is_none(monkeypatch):
    monkeypatch.delenv("GITHUB_REPOS", raising=False)
    assert targets_from_env() is None


def test_targets_from_env_blank_is_none(monkeypatch):
    monkeypatch.setenv("GITHUB_REPOS", "  , ,")
    assert targets_from_env() is None


def test_targets_from_env_parses_value(monkeypatch):
    monkeypatch.setenv("GITHUB_REPOS", "acme|widgets,octocat")
    assert targets_from_env() == ["acme/widgets", "octocat"]
