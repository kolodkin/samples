import pytest

from github_exposure_scanner.models import Target, parse_target


def test_parse_bare_org():
    assert parse_target("acme") == Target(org="acme")


def test_parse_org_repo():
    assert parse_target("acme/widgets") == Target(org="acme", repo="widgets")


def test_parse_strips_whitespace():
    assert parse_target("  acme/widgets  ") == Target(org="acme", repo="widgets")


def test_parse_rejects_empty():
    with pytest.raises(ValueError):
        parse_target("   ")
