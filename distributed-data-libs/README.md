Distributed Data Library Benchmark
---

Benchmarks 3 distributed data engines (aaiclick → ClickHouse + Postgres, Apache Spark via PySpark, Dask) across 17 common operations on 10M rows — 11 standard ops, 3 paginated companions that measure mid-range `LIMIT/OFFSET`, and 3 no-materialize companions that run the operator pipeline server-side and discard the result. Each runner runs in its own Docker container; aaiclick's distributed mode connects to a ClickHouse server (data engine) and a Postgres server (metadata catalog) — both as long-lived sidecar containers. Per-operation wall-clock time, peak memory (cgroup `memory.peak` delta), and CPU utilization (cgroup `cpu.stat`) are measured at the container level so distributed workers, JVM heaps, and engine processes are all accounted for. Ray Data was tried and dropped — its single-node hash-shuffle makes 4-CPU group-by 100-1000× slower than the others and the numbers said more about Ray Data's design target (ML preprocessing, not OLAP) than about Ray as a system. See `SPEC.md` for measurement methodology and per-framework optimizations.

```bash
./distributed-data-libs.sh
```
