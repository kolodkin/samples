# python-data-libs: Python Data Library Benchmark

Python, NumPy, Pandas, PyArrow, Polars, SQLite, DuckDB, chdb, aaiclick — 1M rows, 10 runs averaged.

## Operations

**Ingest:** convert from Python dict[str, list] to library format
**Column:** sum, multiply, filter, sort, count distinct
**Group-by:** sum, count, multi-agg (sum/mean/min/max), multi-key, high cardinality (1000 groups)

## Guidelines

- No for-loops unless unavoidable (native Python exempt)
- Measure compute only — materialize large results into temp tables (chdb/duckdb) instead of fetching
- One file per library (`bench_<lib>.py`), shared runner (`run.py`)

## chdb optimizations

- **Ingest:** PyArrow zero-copy via `INSERT INTO ... SELECT * FROM Python(arrow_table)`
- **LowCardinality(String):** dictionary-encoded strings for category/subcategory columns
- **COUNT DISTINCT:** rewritten as `SELECT count() FROM (... GROUP BY ...)` (~2.5x faster)
- **CREATE TABLE AS SELECT:** materializes filter/sort/multiply into a new table, then drops it

## duckdb optimizations

- **Ingest:** PyArrow zero-copy via `CREATE TABLE AS SELECT * FROM arrow_table`
- **CREATE TABLE AS SELECT:** materializes filter/sort/multiply into a new table, then drops it
- **`PRAGMA preserve_insertion_order=false`:** lets DuckDB reorder rows freely, speeding up filter/sort/CTAS and group-by
- **COUNT DISTINCT:** rewritten as `SELECT count() FROM (... GROUP BY ...)` (matches chdb)
