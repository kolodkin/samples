# python-data-libs: Python Data Library Benchmark

Python, NumPy, Pandas, PyArrow, Polars, SQLite (plain + indexed), DuckDB, chdb, aaiclick — 1M rows, 10 runs averaged.

## Operations

- **Ingest** — convert from Python `dict[str, list]` to library format
- **Column** — sum, multiply, filter, sort, count distinct
- **Group-by** — sum, count, multi-agg (sum/mean/min/max), multi-key, high cardinality (1000 groups)

## Guidelines

- No for-loops unless unavoidable (native Python exempt)
- Measure compute only — materialize large results into temp tables (chdb/duckdb/sqlite) instead of fetching
- One file per library (`bench_<lib>.py`), shared runner (`run.py`)
- Each `(library, op)` pair runs in a fresh `multiprocessing.spawn` child; peak memory is `getrusage(RUSAGE_SELF).ru_maxrss` delta sampled after setup (raw_data load + import + `convert()`), so it reports the op's incremental high-water mark

## Per-library optimizations

### sqlite (both flavors)

- **Ingest** — 50-row batched `INSERT ... VALUES (...),(...),...` with a single parameter bind; fewer round trips than row-at-a-time inserts.

### sqlite+idx (only)

- **Count distinct**, **Group-by sum**, **Group-by count**, **Group-by multi-agg**, **Multi-key group-by** — covering index `(category, subcategory, amount)` built after bulk insert; planner does an index-only scan, skipping the main table.
- **High-card group-by** — covering index `(subcategory, amount)` for index-only scan over the 1000-group aggregation.
- **ANALYZE** runs after index creation so the planner has stats to pick the right covering index per query.
- Indexes are created *after* bulk insert rather than before — one-shot B-tree build is much faster than incremental maintenance across 1M inserts.
- Trade-off: Ingest costs ~4.5× more (index build) but group-by/count-distinct queries get 4–13× faster.

### duckdb

- **Ingest** — PyArrow zero-copy via `CREATE TABLE data AS SELECT * FROM arrow_table`; no row-wise marshalling.
- **Column multiply**, **Filter rows**, **Sort** — wrapped in `CREATE TABLE sink AS <query>` then `DROP TABLE sink`, so we measure compute without paying to fetch a large result set back to Python.

### chdb

- **Ingest** — PyArrow zero-copy via `INSERT INTO bench.data SELECT * FROM Python(arrow_table)` using the Python table function.
- **Schema** — `LowCardinality(String)` on `category` and `subcategory` dictionary-encodes the low-cardinality string columns, shrinking scans and speeding up group-by.
- **Count distinct** — rewritten as `SELECT count() FROM (SELECT category FROM bench.data GROUP BY category)`; ~2.5× faster than `count(DISTINCT category)` on ClickHouse.
- **Column multiply**, **Filter rows**, **Sort** — materialize into a `Memory`-engine sink table via `CREATE TABLE ... AS SELECT`, then drop, to measure compute without fetch overhead.
