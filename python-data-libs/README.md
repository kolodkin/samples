Python Data Library Benchmark
---

Benchmarks 10 Python data libraries (native Python, NumPy, Pandas, PyArrow, Polars, SQLite plain + indexed, DuckDB, chdb, aaiclick) across 11 common operations on 1M rows and 10 runs per operation, measuring average time and per-op peak memory in a fresh process per measurement. See `SPEC.md` for measurement methodology and per-library optimizations.

```bash
./python-data-libs.sh
```
