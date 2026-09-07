"""Airtable upsert publishing for the Google Health sync.

Two tables are kept in sync (gated on ``publish_airtable=True`` and
``AIRTABLE_API_KEY`` / ``AIRTABLE_BASE_ID``): ``Health Activities`` (one row
per exercise session, merged on ``Activity ID``) and ``Health Daily Metrics``
(one row per civil day, merged on ``Date``).

Unlike the delete-all-recreate publishing in ``github-exposure-scanner``, a
daily sync must preserve history, so records are written with Airtable's
native upsert (``performUpsert.fieldsToMergeOn``): re-running a window
updates existing rows in place and never drops older ones. ``None`` metrics
are omitted per-record so a day without a weigh-in doesn't null an existing
value. The REST helpers (retry/backoff, batching, schema ensure) follow the
scanner's Airtable module; ``urllib.request`` keeps it dependency-free.
"""

import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator, Sequence

from aaiclick import ORIENT_DICT
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult, AirtableValidationResult

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH = 10  # Airtable max records per upsert request
AIRTABLE_THROTTLE_SECONDS = 0.2  # 5 req/sec per base
AIRTABLE_BACKOFF_SECONDS = (2, 4, 8)

ACTIVITIES_TABLE = os.environ.get("AIRTABLE_ACTIVITIES_TABLE", "Health Activities")
DAILY_TABLE = os.environ.get("AIRTABLE_DAILY_TABLE", "Health Daily Metrics")

# (Airtable field name -> Object column) mappings and matching field schemas.
_ACTIVITIES_MAP = {
    "Activity ID": "activity_id", "Date": "date", "Type": "type", "Name": "display_name",
    "Start time": "start_time", "End time": "end_time", "Duration (min)": "duration_min",
    "Distance (km)": "distance_km", "Calories (kcal)": "calories_kcal", "Steps": "steps",
    "Source": "source",
}
_ACTIVITIES_MERGE_ON = ["Activity ID"]
_ACTIVITIES_SCHEMA = [
    {"name": "Activity ID", "type": "singleLineText"},
    {"name": "Date", "type": "date", "options": {"dateFormat": {"name": "iso"}}},
    {"name": "Type", "type": "singleLineText"},
    {"name": "Name", "type": "singleLineText"},
    {"name": "Start time", "type": "singleLineText"},
    {"name": "End time", "type": "singleLineText"},
    {"name": "Duration (min)", "type": "number", "options": {"precision": 2}},
    {"name": "Distance (km)", "type": "number", "options": {"precision": 3}},
    {"name": "Calories (kcal)", "type": "number", "options": {"precision": 0}},
    {"name": "Steps", "type": "number", "options": {"precision": 0}},
    {"name": "Source", "type": "singleLineText"},
]
_DAILY_MAP = {"Date": "date", "Steps": "steps", "Weight (kg)": "weight_kg"}
_DAILY_MERGE_ON = ["Date"]
_DAILY_SCHEMA = [
    {"name": "Date", "type": "date", "options": {"dateFormat": {"name": "iso"}}},
    {"name": "Steps", "type": "number", "options": {"precision": 0}},
    {"name": "Weight (kg)", "type": "number", "options": {"precision": 2}},
]


def _chunks(items: Sequence, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def _parse_base_id(value: str) -> str:
    """Strip a stray ``/tblXXX[/...]`` suffix copied from an Airtable UI URL."""
    return value.split("/", 1)[0]


def _airtable_request(method: str, url: str, api_key: str, *, body: dict | None = None) -> dict:
    """One Airtable REST call with exponential backoff on 429 / 5xx / network error."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    last_exc: Exception | None = None
    for backoff in (0,) + AIRTABLE_BACKOFF_SECONDS:
        if backoff:
            time.sleep(backoff)
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload) if payload else {}
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            if e.code == 429 or 500 <= e.code < 600:
                last_exc = RuntimeError(f"Airtable {e.code}: {body_text}")
                continue
            raise RuntimeError(f"Airtable {e.code}: {body_text}") from e
        except urllib.error.URLError as e:
            last_exc = e
            continue
    raise RuntimeError(f"Airtable request failed after retries: {last_exc}")


async def _arequest(method: str, url: str, api_key: str, *, body: dict | None = None) -> dict:
    return await asyncio.to_thread(_airtable_request, method, url, api_key, body=body)


def _table_url(base_id: str, table: str) -> str:
    return f"{AIRTABLE_API_BASE}/{base_id}/{urllib.parse.quote(table, safe='')}"


def _upsert_body(records: list[dict], merge_on: list[str]) -> dict:
    return {
        "performUpsert": {"fieldsToMergeOn": merge_on},
        "records": records,
        "typecast": True,
    }


async def _upsert_records(
    api_key: str, base_id: str, table: str, records: list[dict], merge_on: list[str],
) -> None:
    await _arequest("PATCH", _table_url(base_id, table), api_key,
                    body=_upsert_body(records, merge_on))


async def _ensure_table_schema_with(api_key: str, base_id: str, table: str, schema: list[dict]) -> None:
    """Ensure ``table`` exists with all fields in ``schema``.

    Creates the table when missing (first field becomes primary); otherwise
    adds any missing fields in place. The Airtable Web API cannot delete
    fields or tables, so pre-existing extra fields are left alone.
    """
    meta_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    payload = await _arequest("GET", meta_url, api_key)
    tables = {t["name"]: t for t in payload.get("tables", [])}

    if table not in tables:
        await _arequest("POST", meta_url, api_key, body={"name": table, "fields": schema})
        return

    existing = {f["name"] for f in tables[table].get("fields", [])}
    missing = [f for f in schema if f["name"] not in existing]
    if not missing:
        return
    table_id = tables[table]["id"]
    fields_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables/{table_id}/fields"
    for field in missing:
        await _arequest("POST", fields_url, api_key, body=field)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)


async def _field_records(obj: Object, mapping: dict[str, str]) -> list[dict]:
    """Turn an Object's rows into Airtable ``{"fields": {...}}`` records,
    omitting ``None`` values so upserts never overwrite data with nulls."""
    rows = await obj.data(orient=ORIENT_DICT)
    if not rows:
        return []
    n = len(rows[next(iter(mapping.values()))])
    records = []
    for i in range(n):
        fields = {fname: rows[col][i] for fname, col in mapping.items()
                  if col in rows and rows[col][i] is not None}
        records.append({"fields": fields})
    return records


async def _validate_impl() -> AirtableValidationResult:
    api_key = os.environ.get("AIRTABLE_API_KEY")
    raw_base_id = os.environ.get("AIRTABLE_BASE_ID")
    if not (api_key and raw_base_id):
        return AirtableValidationResult(status="skipped", reason="AIRTABLE_API_KEY/BASE_ID not set")
    base_id = _parse_base_id(raw_base_id)
    await _arequest("GET", "https://api.airtable.com/v0/meta/whoami", api_key)
    await _arequest("GET", f"https://api.airtable.com/v0/meta/bases/{base_id}/tables", api_key)
    return AirtableValidationResult(status="ok", base=base_id)


async def _publish_impl(
    obj: Object, validation: AirtableValidationResult, table: str,
    schema: list[dict], mapping: dict[str, str], merge_on: list[str],
) -> AirtablePublishResult:
    if validation.status == "skipped":
        return AirtablePublishResult(status="skipped", reason=validation.reason, table=table)
    api_key = os.environ["AIRTABLE_API_KEY"]
    base_id = validation.base  # already parsed by validate_airtable_credentials
    records = await _field_records(obj, mapping)
    await _ensure_table_schema_with(api_key, base_id, table, schema)
    for batch in _chunks(records, AIRTABLE_BATCH):
        await _upsert_records(api_key, base_id, table, batch, merge_on)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)
    return AirtablePublishResult(status="published", base=base_id, table=table, rows=len(records))


@task
async def validate_airtable_credentials() -> AirtableValidationResult:
    return await _validate_impl()


@task
async def upsert_activities(activities: Object, validation: AirtableValidationResult) -> AirtablePublishResult:
    return await _publish_impl(
        activities, validation, ACTIVITIES_TABLE, _ACTIVITIES_SCHEMA,
        _ACTIVITIES_MAP, _ACTIVITIES_MERGE_ON)


@task
async def upsert_daily_metrics(daily: Object, validation: AirtableValidationResult) -> AirtablePublishResult:
    return await _publish_impl(
        daily, validation, DAILY_TABLE, _DAILY_SCHEMA, _DAILY_MAP, _DAILY_MERGE_ON)
