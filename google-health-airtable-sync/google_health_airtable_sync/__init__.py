"""Google Health → Airtable daily sync.

Fetch the last ``days`` days of Google Health data — reported exercise
sessions (runs, walks, …) and per-day rollups (total steps, average weight) —
and upsert them into two Airtable tables. Designed to run once a day: the
lookback window overlaps previous runs and the upsert is keyed, so a missed
day self-heals on the next run and history is never dropped.

DAG::

    fetch_activities ──────────────► upsert_activities ─┐
    fetch_daily_metrics ───────────► upsert_daily_metrics ├─► generate_report
    validate_airtable_credentials ─┴─────────────────────┘

Environment variables:
    GOOGLE_HEALTH_CLIENT_ID / _CLIENT_SECRET / _REFRESH_TOKEN
                         — OAuth credentials for the Google Health API
                           (``python -m google_health_airtable_sync --auth``
                           prints the refresh token after a one-time consent)
    GHS_FIXTURE_DIR      — offline fixture mode (tests/CI)
    AIRTABLE_API_KEY     — Airtable PAT (required with publish_airtable=True)
    AIRTABLE_BASE_ID     — Airtable base id (required with publish_airtable=True)
    AIRTABLE_ACTIVITIES_TABLE / AIRTABLE_DAILY_TABLE — table name overrides
"""

import asyncio

from aaiclick.orchestration import job

from .airtable import upsert_activities, upsert_daily_metrics, validate_airtable_credentials
from .report import generate_report
from .sync import fetch_activities, fetch_daily_metrics


@job("google_health_sync")
async def health_sync_pipeline(days: int = 7, publish_airtable: bool = True):
    """Sync the last ``days`` days of Google Health data into Airtable."""
    activities = fetch_activities(days=days)
    daily = fetch_daily_metrics(days=days)

    activities_pub = daily_pub = None
    if publish_airtable:
        validation = validate_airtable_credentials()
        activities_pub = upsert_activities(activities=activities, validation=validation)
        daily_pub = upsert_daily_metrics(daily=daily, validation=validation)

    return generate_report(
        activities=activities,
        daily=daily,
        activities_publish=activities_pub,
        daily_publish=daily_pub,
    )


async def main(**kwargs):
    created_job = await health_sync_pipeline(**kwargs)
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    return created_job


if __name__ == "__main__":
    asyncio.run(main())
