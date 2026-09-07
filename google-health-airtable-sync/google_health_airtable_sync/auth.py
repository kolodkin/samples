"""One-time OAuth consent helper: mint a Google Health refresh token.

``python -m google_health_airtable_sync --auth`` prints the consent URL,
catches the redirect on a local loopback server, exchanges the code, and
prints the refresh token to export as ``GOOGLE_HEALTH_REFRESH_TOKEN``.
Requires ``GOOGLE_HEALTH_CLIENT_ID`` / ``GOOGLE_HEALTH_CLIENT_SECRET`` from a
Google Cloud OAuth client (Desktop or Web with the loopback redirect URI).
"""

import json
import os
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

from .health_api import SCOPES, TOKEN_URL

CONSENT_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
DEFAULT_PORT = 8765


def consent_url(client_id: str, redirect_uri: str) -> str:
    query = urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",  # ask for a refresh token
        "prompt": "consent",       # ...even if the user consented before
    })
    return f"{CONSENT_ENDPOINT}?{query}"


def token_request_form(client_id: str, client_secret: str, code: str, redirect_uri: str) -> str:
    return urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    })


def _wait_for_code(port: int) -> str:
    """Serve one request on localhost and return the ``code`` query param."""
    captured: dict = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - http.server API
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            captured["code"] = (params.get("code") or [""])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Authorized - you can close this tab and return to the terminal.")

        def log_message(self, *args):  # keep the terminal quiet
            pass

    with HTTPServer(("localhost", port), Handler) as server:
        while not captured.get("code"):
            server.handle_request()
    return captured["code"]


def run_auth_flow() -> None:
    client_id = os.environ.get("GOOGLE_HEALTH_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_HEALTH_CLIENT_SECRET")
    if not (client_id and client_secret):
        raise SystemExit(
            "Set GOOGLE_HEALTH_CLIENT_ID and GOOGLE_HEALTH_CLIENT_SECRET first "
            "(create an OAuth client in Google Cloud console with the Health API enabled)."
        )
    port = int(os.environ.get("GHS_AUTH_PORT", DEFAULT_PORT))
    redirect_uri = f"http://localhost:{port}/"

    print("Open this URL in your browser and grant access:\n")
    print(consent_url(client_id, redirect_uri))
    print(f"\nWaiting for the OAuth redirect on {redirect_uri} ...")
    code = _wait_for_code(port)

    form = token_request_form(client_id, client_secret, code, redirect_uri).encode("utf-8")
    req = urllib.request.Request(
        TOKEN_URL, data=form, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        raise SystemExit(f"No refresh_token in token response: {payload}")
    print("\nSuccess! Export this before running the sync:\n")
    print(f"export GOOGLE_HEALTH_REFRESH_TOKEN='{refresh_token}'")
