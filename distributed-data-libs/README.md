Distributed Data Library Benchmark
---

Benchmarks 4 distributed/parallel Python data frameworks (aaiclick, Apache Spark via PySpark, Dask, Ray Data) across 11 common operations on 10M rows. Each framework runs in its own Docker container; per-operation wall-clock time, peak memory (cgroup `memory.peak`), and CPU utilization (cgroup `cpu.stat`) are measured at the container level so distributed workers, JVM heaps, and object stores are all accounted for. See `SPEC.md` for measurement methodology and per-framework optimizations.

```bash
./distributed-data-libs.sh
```
