Distributed Data Library Benchmark
---

Benchmarks 5 server-backed data engines (aaiclick → ClickHouse, Apache Spark via PySpark, Dask, Ray Data, Postgres) across 11 common operations on 10M rows. Each runner runs in its own Docker container; engines that need a server (ClickHouse, Postgres) live in long-lived sidecar containers. Per-operation wall-clock time, peak memory (cgroup `memory.peak` delta), and CPU utilization (cgroup `cpu.stat`) are measured at the container level so distributed workers, JVM heaps, and engine processes are all accounted for. See `SPEC.md` for measurement methodology and per-framework optimizations.

```bash
./distributed-data-libs.sh
```
