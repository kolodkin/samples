IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies (1950+, 40–300 min runtime, non-adult), normalizes genres via explode, enriches each title with Wikipedia plot text via Wikidata `P345` title resolution plus an `AggregatingMergeTree` merge against the Hugging Face `wikimedia/wikipedia` Parquet dump, and optionally publishes a curated Parquet dataset to Hugging Face. It runs on a distributed backend (ClickHouse server + PostgreSQL orchestration), which the runner provisions through `scripts/setup_aaiclick` — a backend already answering at `AAICLICK_CH_URL` / `AAICLICK_SQL_URL` is detected and reused, so pointing those at an existing cluster skips provisioning entirely. It can also be run from the GitHub UI via the `imdb-dataset-builder` workflow (manual dispatch, databases provided as service containers).

```bash
# Full dataset (~12.6M rows, default)
./imdb-dataset-builder.sh

# Quick sample (500k rows)
./imdb-dataset-builder.sh --sample

# Against an already-running cluster (probed, not provisioned)
AAICLICK_CH_URL=... AAICLICK_SQL_URL=... ./imdb-dataset-builder.sh --sample
```

Publishing is opt-in. Pass `--publish` (sets `publish_hf=True`) with `HF_TOKEN` set to publish the curated dataset to Hugging Face Hub — the flag requires the token, so the pipeline fails fast if it is unset. Pass `--airtable` (sets `publish_airtable=True`) with `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID` (table defaults to `IMDB`) to publish a ~200-row stratified-by-genre showcase sample to Airtable.
