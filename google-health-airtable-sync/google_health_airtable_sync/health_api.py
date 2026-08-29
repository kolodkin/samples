"""Google Health API client, with an offline fixture mode.

Live mode calls ``health.googleapis.com/v4`` with an OAuth 2.0 access token
minted from a long-lived refresh token (``GOOGLE_HEALTH_CLIENT_ID`` /
``GOOGLE_HEALTH_CLIENT_SECRET`` / ``GOOGLE_HEALTH_REFRESH_TOKEN`` — obtain the
refresh token once with ``python -m google_health_airtable_sync --auth``).
Fixture mode (``GHS_FIXTURE_DIR``) reads canned responses instead — used by
the test suite and CI so no network or consent flow is involved.

``urllib.request`` is used directly (same as the Airtable module) so no extra
dependency is needed.
"""

import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

API_BASE = "https://health.googleapis.com/v4"
TOKEN_URL = "https://oauth2.googleapis.com/token"
BACKOFF_SECONDS = (2, 4, 8)
PAGE_SIZE = 500

SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
]

_OAUTH_ENV = ("GOOGLE_HEALTH_CLIENT_ID", "GOOGLE_HEALTH_CLIENT_SECRET",
              "GOOGLE_HEALTH_REFRESH_TOKEN")


def _request_json(method: str, url: str, *, headers: dict, body: bytes | None = None) -> dict:
    """One HTTP call with exponential backoff on 429 / 5xx / network error."""
    last_exc: Exception | None = None
    for backoff in (0,) + BACKOFF_SECONDS:
        if backoff:
            time.sleep(backoff)
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload) if payload else {}
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            if e.code == 429 or 500 <= e.code < 600:
                last_exc = RuntimeError(f"Google Health {e.code}: {body_text}")
                continue
            raise RuntimeError(f"Google Health {e.code}: {body_text}") from e
        except urllib.error.URLError as e:
            last_exc = e
            continue
    raise RuntimeError(f"Google Health request failed after retries: {last_exc}")


class GoogleHealthClient:
    def __init__(self, fixture_dir: str | None = None):
        self._fixture_dir = fixture_dir
        self._access_token: str | None = None

    # --- fixture helpers ---
    def _read_fixture_json(self, name: str) -> dict:
        with open(os.path.join(self._fixture_dir, name), encoding="utf-8") as f:
            return json.load(f)

    # --- auth ---
    def _require_credentials(self) -> tuple[str, str, str]:
        missing = [v for v in _OAUTH_ENV if not os.environ.get(v)]
        if missing:
            raise RuntimeError(
                f"Missing Google Health OAuth env vars: {', '.join(missing)} "
                "(run `python -m google_health_airtable_sync --auth` to obtain a refresh token)"
            )
        return tuple(os.environ[v] for v in _OAUTH_ENV)  # type: ignore[return-value]

    def _refresh_access_token(self) -> str:
        client_id, client_secret, refresh_token = self._require_credentials()
        form = urllib.parse.urlencode({
            "client_id": client_id, "client_secret": client_secret,
            "refresh_token": refresh_token, "grant_type": "refresh_token",
        }).encode("utf-8")
        payload = _request_json(
            "POST", TOKEN_URL, body=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"Token refresh returned no access_token: {payload}")
        return token

    def _auth_headers(self) -> dict:
        if self._access_token is None:
            self._access_token = self._refresh_access_token()
        return {"Authorization": f"Bearer {self._access_token}",
                "Accept": "application/json", "Content-Type": "application/json"}

    # --- API calls ---
    def _get_paginated(self, url: str, items_key: str) -> list[dict]:
        items: list[dict] = []
        headers = self._auth_headers()
        next_url = url
        while True:
            payload = _request_json("GET", next_url, headers=headers)
            items.extend(payload.get(items_key, []))
            token = payload.get("nextPageToken")
            if not token:
                return items
            next_url = url + "&pageToken=" + urllib.parse.quote(token, safe="")

    async def list_exercise_datapoints(self, start_civil: str) -> list[dict]:
        """Exercise sessions whose civil start time is on/after ``start_civil``
        (an RFC3339 local timestamp like ``2026-08-18T00:00:00``)."""
        if self._fixture_dir:
            return self._read_fixture_json("exercise_datapoints.json").get("dataPoints", [])
        flt = urllib.parse.quote(
            f'exercise.interval.civil_start_time >= "{start_civil}"', safe="")
        url = (f"{API_BASE}/users/me/dataTypes/exercise/dataPoints"
               f"?pageSize={PAGE_SIZE}&filter={flt}")
        return await asyncio.to_thread(self._get_paginated, url, "dataPoints")

    async def daily_rollup(self, data_type: str, start: date, end: date) -> list[dict]:
        """Per-day rollups for ``data_type`` over the closed-open [start, end) range."""
        if self._fixture_dir:
            return self._read_fixture_json(f"{data_type}_daily_rollup.json").get(
                "rollupDataPoints", [])

        def _civil(d: date) -> dict:
            return {"year": d.year, "month": d.month, "day": d.day}

        def _post() -> list[dict]:
            url = f"{API_BASE}/users/me/dataTypes/{data_type}/dataPoints:dailyRollUp"
            body = {"range": {"start": _civil(start), "end": _civil(end)}, "pageSize": 100}
            items: list[dict] = []
            headers = self._auth_headers()
            while True:
                payload = _request_json(
                    "POST", url, headers=headers, body=json.dumps(body).encode("utf-8"))
                items.extend(payload.get("rollupDataPoints", []))
                token = payload.get("nextPageToken")
                if not token:
                    return items
                body["pageToken"] = token

        return await asyncio.to_thread(_post)


def make_client() -> GoogleHealthClient:
    return GoogleHealthClient(fixture_dir=os.environ.get("GHS_FIXTURE_DIR"))
