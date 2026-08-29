Google Health → Airtable Sync
---

A daily sync job for personal fitness data: fetches the last N days from the [Google Health API](https://developers.google.com/health) — reported exercise sessions (runs, walks, …) and per-day rollups (total steps, average weight) — and upserts them into two Airtable tables, `Health Activities` (one row per session, merged on Activity ID) and `Health Daily Metrics` (one row per day, merged on Date). Re-running a window updates rows in place and never drops history, so a cron-scheduled daily run self-heals missed days. It demonstrates aaiclick external-API ingestion with OAuth, a static fan-in DAG, nullable-column `Object`s, and keyed Airtable upserts.

```bash
./google-health-airtable-sync.sh --auth   # one-time: prints GOOGLE_HEALTH_REFRESH_TOKEN
./google-health-airtable-sync.sh --days 7
```
