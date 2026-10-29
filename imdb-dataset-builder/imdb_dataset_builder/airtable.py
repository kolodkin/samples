"""Airtable showcase publishing for the IMDb dataset builder.

Stratified-by-genre sample: top ``PER_GENRE_LIMIT`` rows per genre by
plot length (longest plots first), deduped by ``tconst``. The deduped
sample (~150-200 rows) is uploaded in *replace* mode: existing records
are listed and deleted in batches of 10, then the new sample is inserted
in batches of 10. Airtable's 5 req/sec per-base rate limit is honored
via ``asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)`` between calls.
``urllib.request`` is used directly so no extra dependency is needed.
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
from aaiclick.data.models import GB_ANY
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult, AirtableValidationResult

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH = 10  # Airtable max records per create / delete request
AIRTABLE_THROTTLE_SECONDS = 0.2  # 5 req/sec per base
AIRTABLE_BACKOFF_SECONDS = (2, 4, 8)
PER_GENRE_LIMIT = 10

_SAMPLE_COLUMNS = [
    "tconst",
    "primaryTitle",
    "startYear",
    "genres",
    "runtimeMinutes",
    "plot",
]

# Field schema for the Airtable table. Order matters — the first field
# becomes the primary field (the row identifier shown in card views).
# The Airtable Web API can create fields and tables, but CANNOT delete
# them, so any pre-existing extra fields will stay (they just sit empty
# next to our data). Requires the PAT to carry the `schema.bases:write`
# scope in addition to `data.records:read/write`.
_FIELD_SCHEMA: list[dict] = [
    {"name": "tconst", "type": "singleLineText"},
    {"name": "primaryTitle", "type": "singleLineText"},
    {"name": "startYear", "type": "singleLineText"},
    {"name": "genres", "type": "multipleSelects", "options": {"choices": []}},
    {"name": "runtimeMinutes", "type": "singleLineText"},
    {"name": "plot", "type": "multilineText"},
]


def _ch_quote(value: str) -> str:
    """Escape a string for safe interpolation inside a ClickHouse SQL literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _chunks(items: Sequence, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def _parse_base_id(value: str) -> str:
    """Strip a stray ``/tblXXX[/...]`` suffix copied from an Airtable UI URL.

    The base id is always the leading ``app...`` segment; everything after
    a slash is a table/view/record id that doesn't belong in this env var.
    """
    return value.split("/", 1)[0]


@task
async def validate_airtable_credentials() -> AirtableValidationResult:
    """Verify the configured Airtable PAT and base before any other Airtable task runs.

    Skipped when ``AIRTABLE_API_KEY`` / ``AIRTABLE_BASE_ID`` are unset (gates
    the whole Airtable branch in the same way ``publish_to_huggingface`` is
    gated on ``HF_TOKEN``). Otherwise probes the endpoints the publish path
    actually uses — capability is the source of truth, since
    ``/meta/whoami`` only reports ``scopes`` for tokens issued through the
    scoped-PAT flow (service-account / legacy PATs authenticate fine but
    omit the field):

    * ``GET /v0/meta/whoami`` — confirms the token authenticates at all.
    * ``GET /v0/meta/bases/{baseId}/tables`` — confirms base accessibility.
      A 404 here means the base id is wrong or the PAT's base-access list
      doesn't include this base.
    """
    api_key = os.environ.get("AIRTABLE_API_KEY")
    raw_base_id = os.environ.get("AIRTABLE_BASE_ID")
    if not (api_key and raw_base_id):
        return AirtableValidationResult(
            status="skipped",
            reason="AIRTABLE_API_KEY/BASE_ID not set",
        )
    base_id = _parse_base_id(raw_base_id)

    print("[validate_airtable_credentials] verifying token via /meta/whoami", flush=True)
    whoami = await _arequest("GET", "https://api.airtable.com/v0/meta/whoami", api_key)
    scopes = list(whoami.get("scopes", []))

    print(f"[validate_airtable_credentials] verifying base {base_id} access via /meta/bases", flush=True)
    await _arequest("GET", f"https://api.airtable.com/v0/meta/bases/{base_id}/tables", api_key)

    print(
        f"[validate_airtable_credentials] ok: scopes={scopes or 'not reported by /whoami'}, base={base_id}", flush=True
    )
    return AirtableValidationResult(status="ok", scopes=scopes, base=base_id)


@task
async def sample_for_airtable(
    plots: Object,
    genre_balance: Object,
    validation: AirtableValidationResult,
) -> Object:
    """Build a stratified-by-genre showcase sample from ``plots``.

    For each distinct genre in ``genre_balance``, take the top
    ``PER_GENRE_LIMIT`` rows from ``plots`` with the longest ``plot``
    text. A film tagged ``Drama,Romance,War`` lands in 3 buckets, so
    the union is deduped on ``tconst``. Final size: ~150-200 rows.

    ``validation`` is taken as a dependency (not used at runtime) so the
    framework blocks this expensive sample build until the cheap Airtable
    credentials check has passed.
    """
    genres: list[str] = await genre_balance["genres"].data()
    if not genres:
        # Fall back to the empty plots view so downstream code still has an Object.
        return await plots.where("0 = 1").copy()

    per_genre_views = [
        plots.where(f"has(genres, {_ch_quote(g)})").view(order_by="length(plot) DESC", limit=PER_GENRE_LIMIT)
        for g in genres
    ]

    # Concatenate per-genre views via SQL UNION ALL, then dedup by tconst
    # (a film tagged in multiple genres lands in multiple buckets).
    combined = per_genre_views[0]
    for v in per_genre_views[1:]:
        combined = await combined.concat(v)
    materialized = await combined.copy()
    deduped = await materialized.group_by("tconst").agg({col: GB_ANY for col in _SAMPLE_COLUMNS if col != "tconst"})
    return await deduped.copy()


@task
async def publish_to_airtable(
    sample: Object,
    validation: AirtableValidationResult,
) -> AirtablePublishResult:
    """Replace the configured Airtable table's contents with the showcase sample.

    Upstream ``validate_airtable_credentials`` has already verified the PAT
    scopes and base accessibility, so by the time we get here the env vars
    are guaranteed to be set and the credentials are known good. The only
    remaining read of the environment is ``AIRTABLE_TABLE_NAME`` (default
    ``"IMDB"``), which the validator doesn't touch.
    """
    api_key = os.environ["AIRTABLE_API_KEY"]
    base_id = _parse_base_id(os.environ["AIRTABLE_BASE_ID"])
    table = os.environ.get("AIRTABLE_TABLE_NAME", "IMDB")
    if validation.status == "skipped":
        return AirtablePublishResult(
            status="skipped",
            reason=validation.reason,
            table=table,
        )

    rows = await sample.data(orient=ORIENT_DICT)
    n = len(rows.get("tconst", []))
    records = [{"fields": {col: rows[col][i] for col in _SAMPLE_COLUMNS if col in rows}} for i in range(n)]
    print(f"[publish_to_airtable] sample size: {n} records to upload to {base_id}/{table}", flush=True)

    print("[publish_to_airtable] ensuring table schema...", flush=True)
    await _ensure_table_schema(api_key, base_id, table)

    print("[publish_to_airtable] listing existing records...", flush=True)
    existing_ids = await _list_all_record_ids(api_key, base_id, table)
    print(f"[publish_to_airtable] {len(existing_ids)} existing records to delete", flush=True)
    for i, batch in enumerate(_chunks(existing_ids, AIRTABLE_BATCH)):
        await _delete_records(api_key, base_id, table, batch)
        print(f"[publish_to_airtable] deleted batch {i + 1} ({len(batch)} records)", flush=True)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)

    for i, batch in enumerate(_chunks(records, AIRTABLE_BATCH)):
        await _create_records(api_key, base_id, table, batch)
        print(f"[publish_to_airtable] created batch {i + 1} ({len(batch)} records)", flush=True)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)

    return AirtablePublishResult(
        status="published",
        base=base_id,
        table=table,
        rows=n,
    )


def _airtable_request(
    method: str,
    url: str,
    api_key: str,
    *,
    body: dict | None = None,
) -> dict:
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
    """Page through every record id in the table (Airtable returns up to 100/page)."""
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


async def _ensure_table_schema(api_key: str, base_id: str, table: str) -> None:
    """Ensure the configured Airtable table exists with our 6 required fields.

    Creates the table from scratch (with the right field order so ``tconst``
    becomes primary) when it's missing. Otherwise adds any missing fields
    in place. Extra pre-existing fields are left alone — the Airtable Web
    API does not support deleting fields or tables, so a one-time manual
    cleanup of stray fields in the UI is the only way to remove them.
    """
    meta_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    payload = await _arequest("GET", meta_url, api_key)
    tables = {t["name"]: t for t in payload.get("tables", [])}

    if table not in tables:
        print(f"[publish_to_airtable] table {table!r} not found; creating with full schema", flush=True)
        await _arequest(
            "POST",
            meta_url,
            api_key,
            body={"name": table, "fields": _FIELD_SCHEMA},
        )
        return

    existing_field_names = {f["name"] for f in tables[table].get("fields", [])}
    missing = [f for f in _FIELD_SCHEMA if f["name"] not in existing_field_names]
    if not missing:
        print("[publish_to_airtable] table schema already complete", flush=True)
        return

    table_id = tables[table]["id"]
    fields_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables/{table_id}/fields"
    for field in missing:
        await _arequest("POST", fields_url, api_key, body=field)
        print(f"[publish_to_airtable] added missing field: {field['name']} ({field['type']})", flush=True)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)
