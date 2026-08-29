import json
import os

from aaiclick.data.data_context import data_context

from google_health_airtable_sync.airtable import (
    _ACTIVITIES_MAP,
    _DAILY_MAP,
    _field_records,
    _upsert_body,
    _validate_impl,
)
from google_health_airtable_sync.transform import activity_columns, daily_columns
from google_health_airtable_sync.sync import new_activities_object, new_daily_object

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return json.load(f)


async def test_validate_skips_without_credentials(monkeypatch):
    monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
    monkeypatch.delenv("AIRTABLE_BASE_ID", raising=False)
    result = await _validate_impl()
    assert result.status == "skipped"


def test_upsert_body_merges_on_key():
    records = [{"fields": {"Activity ID": "42", "Type": "RUNNING"}}]
    body = _upsert_body(records, merge_on=["Activity ID"])
    assert body["performUpsert"] == {"fieldsToMergeOn": ["Activity ID"]}
    assert body["records"] == records
    assert body["typecast"] is True


async def test_activity_field_records_shape():
    cols = activity_columns(_load("exercise_datapoints.json")["dataPoints"])
    async with data_context():
        obj = await new_activities_object(cols, scope=None)
        records = await _field_records(obj, _ACTIVITIES_MAP)
    assert records[0]["fields"]["Activity ID"] == "8896720705097069096"
    assert records[0]["fields"]["Type"] == "RUNNING"
    assert records[0]["fields"]["Distance (km)"] == 5.23


async def test_daily_field_records_drop_null_metrics():
    cols = daily_columns(
        _load("steps_daily_rollup.json")["rollupDataPoints"],
        _load("weight_daily_rollup.json")["rollupDataPoints"],
    )
    async with data_context():
        obj = await new_daily_object(cols, scope=None)
        records = await _field_records(obj, _DAILY_MAP)
    by_date = {r["fields"]["Date"]: r["fields"] for r in records}
    assert by_date["2026-08-22"]["Steps"] == 9425
    assert by_date["2026-08-22"]["Weight (kg)"] == 78.45
    # a day with no weight reading must not overwrite Airtable with a null
    assert "Weight (kg)" not in by_date["2026-08-23"]
    assert "Steps" not in by_date["2026-08-24"]
