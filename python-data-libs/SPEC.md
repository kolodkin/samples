# python-data-libs: Python Data Library Benchmark

Python, NumPy, Pandas, PyArrow, Polars, SQLite, chdb, aaiclick — 1M rows, 10 runs averaged.

## Operations

**Ingest:** convert from Python dict[str, list] to library format
**Column:** sum, multiply, filter, sort, count distinct
**Group-by:** sum, count, multi-agg (sum/mean/min/max), multi-key, high cardinality (1000 groups)

## Guidelines

- No for-loops unless unavoidable (native Python exempt)
- Measure compute only — no materialization for chdb/aaiclick
- One file per library (`bench_<lib>.py`), shared runner (`run.py`)

## chdb optimizations

- **Ingest:** PyArrow zero-copy via `INSERT INTO ... SELECT * FROM Python(arrow_table)`
- **ORDER BY (category, subcategory):** table sorted by group-by keys enables streaming aggregation
- **optimize_aggregation_in_order=1:** uses sorted order to avoid hash table, ~2x faster on group-by
- **LowCardinality(String):** dictionary-encoded strings for category/subcategory columns
- **COUNT DISTINCT:** rewritten as `SELECT count() FROM (... GROUP BY ...)` (~2.5x faster)
- **FORMAT Null:** used for filter/sort/multiply to skip result serialization and materialization
