import os
from datetime import date

import pytest

from google_health_airtable_sync.health_api import GoogleHealthClient, make_client

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


async def test_fixture_mode_lists_exercise_datapoints():
    client = GoogleHealthClient(fixture_dir=FIXTURES)
    points = await client.list_exercise_datapoints("2026-08-18T00:00:00")
    assert len(points) == 2
    assert points[0]["exercise"]["exerciseType"] == "RUNNING"


async def test_fixture_mode_daily_rollup_reads_per_datatype_file():
    client = GoogleHealthClient(fixture_dir=FIXTURES)
    steps = await client.daily_rollup("steps", date(2026, 8, 18), date(2026, 8, 25))
    weight = await client.daily_rollup("weight", date(2026, 8, 18), date(2026, 8, 25))
    assert steps[0]["steps"]["countSum"] == "9425"
    assert weight[-1]["weight"]["weightGramsAvg"] == 78100.0


def test_make_client_uses_fixture_env(monkeypatch):
    monkeypatch.setenv("GHS_FIXTURE_DIR", FIXTURES)
    client = make_client()
    assert client._fixture_dir == FIXTURES


def test_live_client_requires_oauth_env(monkeypatch):
    for var in ("GOOGLE_HEALTH_CLIENT_ID", "GOOGLE_HEALTH_CLIENT_SECRET",
                "GOOGLE_HEALTH_REFRESH_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv("GHS_FIXTURE_DIR", raising=False)
    client = make_client()
    with pytest.raises(RuntimeError, match="GOOGLE_HEALTH_"):
        client._require_credentials()
