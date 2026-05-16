# distributed-data-libs: Distributed Data Library Benchmark

aaiclick, PySpark, Dask, Ray Data — 10M rows, 3 runs averaged. Runs entirely inside Docker; cgroup v2 provides exact resource attribution.

## Operations

Same 11 ops as `python-data-libs` so results are directly comparable at scale:

- **Ingest** — convert raw `dict[str, list]` to the framework's native dataset
- **Column** — sum, multiply, filter, sort, count distinct
- **Group-by** — sum, count, multi-agg (sum/mean/min/max), multi-key, high cardinality (1000 groups)

## Architecture

One `docker-compose.yml` declares one service per framework plus an orchestrator. The orchestrator generates `raw.parquet` once on a shared bind-mounted `./data/` volume. Then for each framework:

1. `docker compose up <fwk>` starts the runner container
2. The runner inside the container loads `raw.parquet`, loops through all 11 ops in a single long-lived process, and writes `/data/results-<fwk>.json`
3. `docker compose down <fwk>` tears down the service

Container-per-framework (not per-op) means each framework pays cluster/JVM cold-start exactly once. Per-op measurement isolation is preserved by resetting `memory.peak` between ops.

## Measurement methodology

All measurements come from cgroup v2 pseudo-files that the kernel maintains for the container. No polling, no sampling drift.

| Metric | Source | Notes |
|---|---|---|
| Wall-clock | `time.perf_counter()` inside the container, averaged over `NUM_RUNS=3` | Excludes framework startup, raw-data load, and `convert()`. Measures compute only. |
| Peak memory | `/sys/fs/cgroup/memory.peak` delta (read before and after each op) | Kernel-tracked monotonic high-water across the entire container. Delta = *incremental* peak this op pushed above the running max. |
| CPU time | `/sys/fs/cgroup/cpu.stat` (`usage_usec`), delta around each op | Sum across all cores. |
| CPU utilization | `cpu_usec / wall_time / NUM_RUNS / 1e6` | Reported as cores-worth used on average. |

`memory.peak` is read-only on most Docker hosts (including GitHub Actions runners) because `/sys/fs/cgroup` is bind-mounted RO. We can't reset it between ops. Instead the runner uses a delta-of-monotonic-peak model:

- The first op's `peak_mem` is its true peak (running max was 0).
- A later op that allocates more than any previous op reports the *new* high-water minus the previous max.
- A later op that uses less memory than an earlier op reports **0** — semantically "this op did not push the running peak higher." This is accurate but coarser than per-op absolute peaks would be.

When comparing frameworks, the **Ingest** row (always first) is the most directly comparable per-op memory number.

## Per-framework optimizations

### aaiclick

- **Schema** — `LowCardinality(String)` on `category` and `subcategory`, mirroring the chdb adapter in `python-data-libs`.
- **Backend** — embedded chdb via `data_context()`; no external ClickHouse server.

### Spark (PySpark)

- **JVM warm across ops** — one SparkSession serves all 11 ops. The Dockerfile pre-warms Ivy/Maven caches at image build time so the first SparkSession boot inside the runner does not pay a download cost.
- **Cache + localCheckpoint** — `convert()` materializes the dataset into a Spark RDD that ops reuse.
- **Noop sink** — large-result ops (`Column multiply`, `Filter rows`, `Sort`) use `df.write.format("noop")` to force execution without paying to collect rows to the driver.
- **Aggregations** — small-result ops use `.collect()` since the result is bounded.

### Dask

- **LocalCluster** — 4 workers, 1 thread each, 2 GB memory limit per worker.
- **Persisted partitions** — `convert()` returns `.persist()`ed dataframe; subsequent ops read from in-memory partitions.
- **Categorical columns** — `category` and `subcategory` cast to pandas `category` dtype before `from_pandas`; cuts group-by time substantially.
- **Materialize without collect** — `_materialize` uses `map_partitions(len).compute().sum()` to force execution of large-result ops without pulling rows to the driver.

### Ray Data

- **Local cluster** — `ray.init(num_cpus=4)` in the container.
- **8 blocks** — `override_num_blocks=8` on `from_items` for parallelism.
- **`.materialize()`** — forces lazy ops to land in the object store; `.count()` triggers metadata fetch without pulling data.
- **Group-by `.take_all()`** — bounded result, safe to collect.

## CI

`.github/workflows/distributed-data-libs.yml` runs the benchmark on `ubuntu-latest` (cgroup v2). Docker layer cache via `actions/cache` keeps subsequent runs fast.

## Caveats

- Container memory limits (`mem_limit: 6g`) cap each framework. If a framework OOMs at 10M rows, raise the limit or downscale `NUM_ROWS` in `config.py`.
- Spark JVM cold-start is amortized across 11 ops but still skews the `Ingest` measurement upward by ~5-10s relative to the other frameworks. Subsequent ops are clean.
- Per-op `memory.peak` reset captures the peak *during the op*, not the framework's resident baseline. The first op may report a larger peak because it triggers lazy framework subsystems.
