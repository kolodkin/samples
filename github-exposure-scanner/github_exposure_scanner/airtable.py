"""Airtable publishing for the GitHub exposure scanner.

Two tables are published (both opt-in, gated on ``publish_airtable=True`` and
``AIRTABLE_API_KEY`` / ``AIRTABLE_BASE_ID``): ``GitHub Findings`` (one row per
redacted finding) and ``GitHub Exposure Summary`` (one row per org).

The REST helpers (retry/backoff, batched create/delete, schema ensure) are
adapted from the ``imdb-dataset-builder`` Airtable module. ``urllib.request``
is used directly so no extra dependency is needed.
"""

import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Sequence

from aaiclick import ORIENT_DICT
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult, AirtableValidationResult

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH = 10  # Airtable max records per create / delete request
AIRTABLE_THROTTLE_SECONDS = 0.2  # 5 req/sec per base
AIRTABLE_BACKOFF_SECONDS = (2, 4, 8)

FINDINGS_TABLE = os.environ.get("AIRTABLE_FINDINGS_TABLE", "GitHub Findings")
SUMMARY_TABLE = os.environ.get("AIRTABLE_SUMMARY_TABLE", "GitHub Exposure Summary")

# (Airtable field name -> Object column) mappings and matching field schemas.
_FINDINGS_MAP = {
    "Organization": "org", "Repository": "repo", "File path": "path", "Line": "line",
    "Permalink": "permalink", "Secret type": "secret_type", "Severity": "severity",
    "Masked value": "masked_value", "Repo stars": "repo_stars", "Detected at": "detected_at",
}
_FINDINGS_SCHEMA = [
    {"name": "Organization", "type": "singleLineText"},
    {"name": "Repository", "type": "singleLineText"},
    {"name": "File path", "type": "singleLineText"},
    {"name": "Line", "type": "number", "options": {"precision": 0}},
    {"name": "Permalink", "type": "url"},
    {"name": "Secret type", "type": "singleLineText"},
    {"name": "Severity", "type": "singleLineText"},
    {"name": "Masked value", "type": "singleLineText"},
    {"name": "Repo stars", "type": "number", "options": {"precision": 0}},
    {"name": "Detected at", "type": "singleLineText"},
    {"name": "Status", "type": "singleLineText"},
]
_SUMMARY_MAP = {
    "Organization": "org", "Repos scanned": "repos_scanned", "Files scanned": "files_scanned",
    "Total findings": "total_findings", "Critical": "critical", "High": "high",
    "Medium": "medium", "Low": "low", "Exposure score": "exposure_score",
    "Risk band": "risk_band", "Top secret type": "top_secret_type", "Scan errors": "scan_errors",
}
_SUMMARY_SCHEMA = (
    [{"name": "Organization", "type": "singleLineText"}]
    + [{"name": n, "type": "number", "options": {"precision": 0}}
       for n in ["Repos scanned", "Files scanned", "Total findings", "Critical",
                 "High", "Medium", "Low", "Exposure score", "Scan errors"]]
    + [{"name": "Risk band", "type": "singleLineText"},
       {"name": "Top secret type", "type": "singleLineText"}]
)


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


async def _list_all_record_ids(api_key: str, base_id: str, table: str) -> list[str]:
    ids: list[str] = []
    offset: str | None = None
    while True:
        url = _table_url(base_id, table) + "?pageSize=100"
        if offset:
            url += "&offset=" + urllib.parse.quote(offset, safe="")
        payload = await _arequest("GET", url, api_key)
        ids.extend(rec["id"] for rec in payload.get("records", []))
        offset = payload.get("offset")
        if not offset:
            return ids
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)


async def _delete_records(api_key: str, base_id: str, table: str, ids: Iterable[str]) -> None:
    qs = "&".join(f"records[]={urllib.parse.quote(rid, safe='')}" for rid in ids)
    url = _table_url(base_id, table) + "?" + qs
    await _arequest("DELETE", url, api_key)


async def _create_records(api_key: str, base_id: str, table: str, records: list[dict]) -> None:
    body = {"records": records, "typecast": True}
    await _arequest("POST", _table_url(base_id, table), api_key, body=body)


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
    """Turn an Object's rows into Airtable ``{"fields": {...}}`` records."""
    rows = await obj.data(orient=ORIENT_DICT)
    if not rows:
        return []
    n = len(rows[next(iter(mapping.values()))])
    records = []
    for i in range(n):
        fields = {fname: rows[col][i] for fname, col in mapping.items() if col in rows}
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
    schema: list[dict], mapping: dict[str, str],
) -> AirtablePublishResult:
    if validation.status == "skipped":
        return AirtablePublishResult(status="skipped", reason=validation.reason, table=table)
    api_key = os.environ["AIRTABLE_API_KEY"]
    base_id = validation.base  # already parsed by validate_airtable_credentials
    records = await _field_records(obj, mapping)
    await _ensure_table_schema_with(api_key, base_id, table, schema)
    existing = await _list_all_record_ids(api_key, base_id, table)
    for batch in _chunks(existing, AIRTABLE_BATCH):
        await _delete_records(api_key, base_id, table, batch)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)
    for batch in _chunks(records, AIRTABLE_BATCH):
        await _create_records(api_key, base_id, table, batch)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)
    return AirtablePublishResult(status="published", base=base_id, table=table, rows=len(records))


@task
async def validate_airtable_credentials() -> AirtableValidationResult:
    return await _validate_impl()


@task
async def publish_findings(findings: Object, validation: AirtableValidationResult) -> AirtablePublishResult:
    return await _publish_impl(findings, validation, FINDINGS_TABLE, _FINDINGS_SCHEMA, _FINDINGS_MAP)


@task
async def publish_summary(summary: Object, validation: AirtableValidationResult) -> AirtablePublishResult:
    return await _publish_impl(summary, validation, SUMMARY_TABLE, _SUMMARY_SCHEMA, _SUMMARY_MAP)
