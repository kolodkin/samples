from urllib.parse import parse_qs, urlparse

from google_health_airtable_sync.auth import consent_url, token_request_form


def test_consent_url_requests_offline_access_with_both_scopes():
    url = consent_url("client-123", "http://localhost:8765/")
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    assert parsed.hostname == "accounts.google.com"
    assert qs["client_id"] == ["client-123"]
    assert qs["redirect_uri"] == ["http://localhost:8765/"]
    assert qs["access_type"] == ["offline"]
    assert qs["prompt"] == ["consent"]  # force a refresh token on re-consent
    scopes = qs["scope"][0].split(" ")
    assert any("activity_and_fitness" in s for s in scopes)
    assert any("health_metrics_and_measurements" in s for s in scopes)


def test_token_request_form_exchanges_auth_code():
    form = parse_qs(token_request_form("client-123", "secret", "code-xyz",
                                       "http://localhost:8765/"))
    assert form["grant_type"] == ["authorization_code"]
    assert form["code"] == ["code-xyz"]
