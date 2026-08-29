# Google Health → Airtable Sync — Technical Notes

## Why the Google Health API

"Google Health" data is served by the [Google Health API](https://developers.google.com/health)
(`health.googleapis.com/v4`), the next generation of the Fitbit Web API. The
older Google Fit REST API is deprecated with end-of-service in late 2026 and
Health Connect has no cloud API, so this is the one supported server-side way
to read activities, steps, and weight.

## Two tables, two data natures

Reported activities and daily aggregates differ in nature, so they land in
separate tables with different merge keys:

| Table | Row | Merge key | Source call |
|---|---|---|---|
| `Health Activities` | one exercise session (run, walk, …) | `Activity ID` | `GET /v4/users/me/dataTypes/exercise/dataPoints` with a `civil_start_time` filter |
| `Health Daily Metrics` | one civil day | `Date` | `POST /v4/users/me/dataTypes/{steps,weight}/dataPoints:dailyRollUp` |

Rollup values: steps → `StepsRollupValue.countSum`, weight →
`WeightRollupValue.weightGramsAvg` (converted to kg). Activity dates use the
session's own UTC offset (`startUtcOffset`), not the server timezone.

## Upsert, not replace

Records are written with Airtable's native upsert
(`PATCH … {"performUpsert": {"fieldsToMergeOn": [...]}}`) in batches of 10
with `typecast`. Consequences:

- Re-running any window is idempotent; history outside the window is untouched.
- A day with steps but no weigh-in omits the `Weight (kg)` field entirely
  (rather than sending `null`), so an existing value is never wiped.
- Tables and missing fields are auto-created via the meta API; extra
  user-added fields are left alone (the Web API cannot delete fields).

## Daily scheduling

The job is one-shot by design; "daily" comes from the scheduler around it
(cron example in the shell script header, or any CI scheduler). The default
7-day lookback overlaps runs so an outage shorter than a week self-heals.
`--days` accepts up to 90 (the API's `dailyRollUp` range cap for these types).

## OAuth

Live mode needs `GOOGLE_HEALTH_CLIENT_ID` / `GOOGLE_HEALTH_CLIENT_SECRET`
(an OAuth client in a Google Cloud project with the Health API enabled) and a
long-lived `GOOGLE_HEALTH_REFRESH_TOKEN`. The `--auth` flow prints the consent
URL, catches the loopback redirect on `localhost:8765` (`GHS_AUTH_PORT`
overrides), and prints the refresh token. Scopes requested:
`googlehealth.activity_and_fitness.readonly` and
`googlehealth.health_metrics_and_measurements.readonly`. Access tokens are
minted per run from the refresh token; nothing is persisted to disk.

## Offline fixture mode

`GHS_FIXTURE_DIR` short-circuits the client to canned JSON responses
(`exercise_datapoints.json`, `steps_daily_rollup.json`,
`weight_daily_rollup.json`) mirroring the documented response shapes — the
test suite and CI run fully offline, including an in-process end-to-end job
run via `ajob_test`.
