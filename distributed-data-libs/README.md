Distributed Data Library Benchmark
---

Benchmarks 4 distributed/parallel data engines (aaiclick → ClickHouse + Postgres, Apache Spark via PySpark, Dask, Ray Data) across 11 common operations on 10M rows. Each runner runs in its own Docker container; aaiclick's distributed mode connects to a ClickHouse server (data engine) and a Postgres server (metadata catalog) — both as long-lived sidecar containers. Per-operation wall-clock time, peak memory (cgroup `memory.peak` delta), and CPU utilization (cgroup `cpu.stat`) are measured at the container level so distributed workers, JVM heaps, and engine processes are all accounted for. See `SPEC.md` for measurement methodology and per-framework optimizations.

```bash
./distributed-data-libs.sh
```
