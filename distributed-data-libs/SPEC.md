# distributed-data-libs: Distributed Data Library Benchmark

aaiclick (→ ClickHouse + Postgres), PySpark, Dask, Ray Data — 10M rows, 3 runs averaged. Runs entirely inside Docker; cgroup v2 provides exact resource attribution.

## Operations

Same 11 ops as `python-data-libs` so non-ingest results are directly comparable at scale. **Ingest semantics differ**: in `python-data-libs`, ingest converts a Python `dict[str, list]` into the library's native format; here, ingest reads `/data/raw.parquet` into the framework's native dataset. Distributed frameworks are designed to read from files, not from in-memory Python — going through `dict` → py4j / `from_pandas` / `from_items` is unrealistic and OOMs the Spark JVM at 10M rows.

- **Ingest** — read `/data/raw.parquet` (Snappy-compressed) into the framework's native dataset
- **Column** — sum, multiply, filter, sort, count distinct
- **Group-by** — sum, count, multi-agg (sum/mean/min/max), multi-key, high cardinality (1000 groups)

## Architecture

One `docker-compose.yml` declares one service per framework plus three sidecars that back aaiclick's distributed mode — `clickhouse` (data engine), `postgres` (metadata catalog), and `fileserver` (nginx serving `/data/` over HTTP, required because `create_object_from_url` uses CH's `url()` table function which speaks HTTP only) — and an orchestrator. The orchestrator generates `raw.parquet` once on a shared bind-mounted `./data/` volume. Then for each framework:

1. `docker compose up <framework>` starts the runner container — engine sidecars auto-start via `depends_on` + healthcheck (first iteration only)
2. The runner inside the container loops through all 11 ops in a single long-lived process and writes `/data/results-<framework>.json`
3. The runner container is stopped; engine sidecars stay up across framework iterations (cheap to leave running, expensive to restart per-framework)
4. After all 4 frameworks finish, the orchestrator renders the report and the script tears down sidecars

Per-op measurement isolation comes from the kernel-tracked monotonic `memory.peak` (see below) plus `cpu.stat` deltas.

| Framework | Runner image | Engine sidecar(s) | Topology |
|---|---|---|---|
| aaiclick | `aaiclick` python client | `clickhouse-server:latest` (data) + `postgres:16` (metadata catalog) + `nginx:alpine` (fileserver) | client ↔ HTTP ↔ CH, client ↔ TCP ↔ Postgres, CH ↔ HTTP ↔ nginx (parquet pull) |
| Spark | PySpark + JDK 17 | (in-process JVM) | driver + executors in one container |
| Dask | `dask[complete]` | (in-process) | LocalCluster: 1 worker × 4 threads × 4 GiB |
| Ray | `ray[data]` | (in-process) | `ray.init(num_cpus=4)` |

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

- **Data engine** — `clickhouse/clickhouse-server:latest` sidecar; runner connects via `CLICKHOUSE_HOST=clickhouse` env vars (standard aaiclick discovery path).
- **Metadata catalog** — `postgres:16` sidecar holds aaiclick's object/schema metadata; runner connects via `PGHOST=postgres` + `PGUSER/PGPASSWORD/PGDATABASE=aaiclick`. Required for aaiclick's distributed mode (embedded chdb mode used the local SQLite catalog instead, but that doesn't scale across processes).
- **Ingest** — `aaiclick.create_object_from_url("http://fileserver/raw.parquet", columns=…, column_types=…)`. This is aaiclick's CH-native fast path: it emits a CH query backed by the `url()` table function, so CH itself does the HTTP fetch + parquet parse + insert in its server process. Zero Python memory footprint, no dict round-trip (which was the 60s tax dominating embedded-chdb Ingest in `python-data-libs`).
- **Why HTTP and not file://** — CH's `url()` table function speaks HTTP only; `file()` is a different table function that aaiclick's `create_object_from_url` doesn't target. Hence the nginx sidecar.
- **Types** — `column_types` pins `Int64` / `Float64` / `LowCardinality(String)` so CH's `DESCRIBE`-based inference doesn't widen the integer columns; mirrors the chdb adapter in `python-data-libs`.

### Spark (PySpark)

- **JVM warm across ops** — one SparkSession serves all 11 ops. The Dockerfile pre-warms Ivy/Maven caches at image build time so the first SparkSession boot inside the runner does not pay a download cost.
- **Native Parquet read** — `spark.read.parquet(path).cache(); count()` materializes the dataset in executors via Arrow, no py4j marshaling.
- **Noop sink** — large-result ops (`Column multiply`, `Filter rows`, `Sort`) use `df.write.format("noop")` to force execution without paying to collect rows to the driver.
- **Aggregations** — small-result ops use `.collect()` since the result is bounded.

### Dask

- **LocalCluster** — 1 worker × 4 threads × 4 GiB memory_limit. Single-process multi-threaded fits in the 6 GB container and avoids inter-worker serialization. Multi-worker setups deadlock on Sort's shuffle at 10M rows under a 6 GB container cap.
- **Native Parquet read** — `dd.read_parquet(...).repartition(8).categorize(...).persist()`. Cast `category`/`subcategory` to dask categorical dtype before persisting; cuts group-by time substantially.
- **Materialize without collect** — `_materialize` uses `map_partitions(len).compute().sum()` to force execution of large-result ops without pulling rows to the driver.

### Ray Data

- **Local cluster** — `ray.init(num_cpus=4)` in the container.
- **Native Parquet read** — `rd.read_parquet(path, override_num_blocks=8).materialize()` lands blocks in the object store via Arrow.
- **`.materialize()`** — forces lazy ops to land in the object store; `.count()` triggers metadata fetch without pulling data.
- **Group-by `.take_all()`** — bounded result, safe to collect.

## CI

`.github/workflows/distributed-data-libs.yml` runs the benchmark on `ubuntu-latest` (cgroup v2). Docker layer cache via `actions/cache` keeps subsequent runs fast.

## Caveats

- Container memory limits (`mem_limit: 6g`) cap each framework. If a framework OOMs at 10M rows, raise the limit or downscale `NUM_ROWS` in `config.py`.
- Spark JVM cold-start is amortized across 11 ops but still skews the `Ingest` measurement upward by ~5-10s relative to the other frameworks. Subsequent ops are clean.
- Per-op `memory.peak` reset captures the peak *during the op*, not the framework's resident baseline. The first op may report a larger peak because it triggers lazy framework subsystems.
