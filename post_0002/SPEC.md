# post_0002: Python Data Library Benchmark

Python, NumPy, Pandas, PyArrow, Polars, chdb, aaiclick — 1M rows, 10 runs averaged.

## Operations

**Column:** sum, multiply, filter, sort, count distinct
**Group-by:** sum, count, multi-agg (sum/mean/min/max), multi-key, high cardinality (1000 groups)

## Guidelines

- No for-loops unless unavoidable (native Python exempt)
- Measure compute only — no materialization for chdb/aaiclick
- One file per library (`bench_<lib>.py`), shared runner (`run.py`)
