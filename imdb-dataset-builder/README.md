IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies (1980+, 40–300 min runtime, non-adult), normalizes genres via explode, enriches each title with Wikipedia plot text via Wikidata `P345` title resolution plus an `AggregatingMergeTree` merge against the Hugging Face `wikimedia/wikipedia` Parquet dump, and optionally publishes a curated Parquet dataset to Hugging Face.

```bash
# Demo mode (500k rows)
./imdb-dataset-builder.sh

# Full dataset (~10M rows)
./imdb-dataset-builder.sh --full
```

Set `HF_TOKEN` to publish the curated dataset to Hugging Face Hub. Set `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID` (table defaults to `IMDB`) to publish a ~200-row stratified-by-genre showcase sample to Airtable.
