IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies (1950+, 40–300 min runtime, non-adult), normalizes genres via explode, enriches each title with Wikipedia plot text via Wikidata `P345` title resolution plus an `AggregatingMergeTree` merge against the Hugging Face `wikimedia/wikipedia` Parquet dump, and optionally publishes a curated Parquet dataset to Hugging Face. Runs on a distributed backend by default (local ClickHouse server + PostgreSQL orchestration). Pass `--local-setup` to auto-provision both locally via the repo's `scripts/setup_clickhouse` and `scripts/setup_postgres`; otherwise the databases are assumed to already exist at `AAICLICK_CH_URL` / `AAICLICK_SQL_URL` (override these to target an existing cluster). It can also be run from the GitHub UI via the `imdb-dataset-builder` workflow (manual dispatch, databases provided as service containers).

```bash
# Full dataset (~12.6M rows, default) — auto-provision the databases locally
./imdb-dataset-builder.sh --local-setup

# Quick sample (500k rows)
./imdb-dataset-builder.sh --local-setup --sample

# Against an already-running cluster (no provisioning)
AAICLICK_CH_URL=... AAICLICK_SQL_URL=... ./imdb-dataset-builder.sh --sample
```

Publishing is opt-in. Pass `--publish` (sets `publish_hf=True`) with `HF_TOKEN` set to publish the curated dataset to Hugging Face Hub — the flag requires the token, so registration fails fast if it is unset. Pass `--airtable` (sets `publish_airtable=True`) with `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID` (table defaults to `IMDB`) to publish a ~200-row stratified-by-genre showcase sample to Airtable.
