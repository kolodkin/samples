# distributed-data-libs: aaiclick 0.0.16 upgrade + execute (compute-and-discard) companions

## Goal

Update the `distributed-data-libs` benchmark to aaiclick 0.0.16 and add a new
class of benchmark — **execute (compute-and-discard)** companions — to the three
full-materialize operations, across all three frameworks (aaiclick, Spark, Dask).
The execute companions run alongside (not instead of) the existing materialize
benchmarks.

## Background

aaiclick 0.0.16 introduces two relevant changes:

1. **`LazyOperator`** — binary operators, aggregations (`sum`, `mean`,
   `nunique`, …) and unary transforms now return a lazy `LazyOperator` instead
   of eagerly hitting the DB. It materializes on `await` or `.data()`. Because
   the existing benchmarks already `await` their operations, awaiting a
   `LazyOperator` still materializes a result table — so the current
   *materialize* benchmarks keep working under 0.0.16 without changes.

2. **`Object.execute()`** (object.py:668) — a discard terminal. It rebuilds the
   exact `SELECT` that `.data()` would issue (honoring `where`, `order_by`,
   `limit`, `offset`, computed columns, renames, field selection), appends
   `FORMAT Null`, and runs it server-side. ClickHouse runs the full pipeline and
   emits zero rows — nothing is materialized or transported back. Returns the
   run's `QueryStats`. `View` overrides `_build_select`, so a View's
   `execute()` automatically measures the View's projection.

### Why this resolves a prior design objection

`SPEC.md` (and git history) record that a discard benchmark for aaiclick was
attempted and reverted. The objection: discard then required dropping to raw
`ch_client.command("… FORMAT Null")`, which measures *ClickHouse the engine,
not aaiclick the library*, breaking the library-vs-library framing. The
materialize approach (`.copy()` / operator result table) was adopted instead.

`Object.execute()` is a first-class **library** API for the same `FORMAT Null`
discard, so the objection no longer holds. Meanwhile Spark and Dask already had
discard paths before the project switched to materialize (git commit `0d5f036`):
Spark's `df.write.format("noop")` sink and Dask's `map_partitions(len)` row
count. The execute companions revive those exact paths and add aaiclick's new
library-level equivalent — now running side-by-side with materialize.

## Scope

- **Frameworks:** all three (aaiclick, Spark, Dask).
- **Operations:** only the three full-materialize ops — **Column multiply**,
  **Filter rows**, **Sort**. These are the ops where retain-vs-discard actually
  differs (each produces ~10M rows). Aggregations/group-bys produce tiny results,
  so a discard variant would carry no new signal; the page companions are left
  untouched.
- **New benchmark entries:** 3 ops × 3 frameworks = 9.

## Design

### New operation names

Appended to `config.py:BENCH_NAMES`, after the existing page companions
(keeping all companions grouped at the bottom: 11 standard, then 3 page, then
3 execute):

- `Column multiply execute`
- `Filter rows execute`
- `Sort execute`

The `execute` suffix mirrors the existing `page` companion convention and the
aaiclick `.execute()` API name.

### Per-framework implementations

**aaiclick** (`bench_aaiclick.py`) — uses `Object.execute()` via the View path
so nothing materializes (using a `LazyOperator` would call `_materialize()`
first and create a table, defeating the discard):

```python
async def _col_mul_execute(obj):
    return await obj.with_columns(
        {"product": Computed("Float64", "amount * quantity")}
    ).execute()

async def _filter_execute(obj):
    return await obj.where(f"amount > {FILTER_THRESHOLD}").execute()

async def _sort_execute(obj):
    return await obj.view(order_by="amount DESC").execute()
```

`with_columns()` and `where()` return a `View`; `obj.view(order_by=…)` returns a
`View`. `View.execute()` issues the projection + `FORMAT Null`.

**Spark** (`bench_spark.py`) — revive the noop write sink as a `_discard`
helper, mirroring the existing `_materialize`/`_sample` helpers:

```python
def _discard(df):
    df.write.format("noop").mode("overwrite").save()
```

```python
"Column multiply execute": lambda df: _discard(df.select((F.col("amount") * F.col("quantity")).alias("p"))),
"Filter rows execute":     lambda df: _discard(df.filter(F.col("amount") > FILTER_THRESHOLD)),
"Sort execute":            lambda df: _discard(df.orderBy(F.col("amount").desc())),
```

**Dask** (`bench_dask.py`) — revive the row-count discard as a `_discard`
helper:

```python
def _discard(ddf):
    ddf.map_partitions(len).compute().sum()
```

```python
"Column multiply execute": lambda ddf: _discard((ddf["amount"] * ddf["quantity"]).to_frame()),
"Filter rows execute":     lambda ddf: _discard(ddf[ddf["amount"] > FILTER_THRESHOLD]),
"Sort execute":            lambda ddf: _discard(ddf.sort_values("amount", ascending=False)),
```

### Report

No change to `report.py`. It iterates `BENCH_NAMES` and renders one row per op
across the three tables (time, peak memory, CPU); the new rows appear
automatically. Cells for a framework that lacks an entry already render `—`
(not applicable here — all three implement all three execute ops).

### Version bump

- `Dockerfile.aaiclick`: `pip install … aaiclick` → `aaiclick==0.0.16` (pin for a
  reproducible benchmark image).
- Root `pyproject.toml`: `aaiclick>=0.0.9` → `aaiclick>=0.0.16`.
- `uv.lock`: regenerate via `uv lock` so the locked aaiclick is 0.0.16.

### Documentation

- **SPEC.md:**
  - Add an "execute companions" subsection under Operations, parallel to the
    paginated-companions subsection: same compute as the materialize ops, result
    discarded (`FORMAT Null` / noop sink / row count), isolating pure compute from
    result-retention cost.
  - Bump op counts: 14 → 17 (11 standard + 3 page + 3 execute); update the
    "loops through all 14 ops" architecture line.
  - aaiclick per-framework section: note `Object.execute()` as the library-level
    `FORMAT Null` discard that resolves the prior raw-`ch_client` objection.
  - Spark/Dask sections: note the revived discard paths (noop sink / row count)
    now run alongside materialize.
- **README.md:** update the "14 common operations … 3 paginated companions"
  sentence to include the 3 execute companions (17 ops total).

## Files changed

| File | Change |
|---|---|
| `distributed-data-libs/distributed_data_libs/config.py` | +3 op names in `BENCH_NAMES` |
| `distributed-data-libs/distributed_data_libs/bench_aaiclick.py` | +3 execute fns, +3 `BENCHMARKS` entries |
| `distributed-data-libs/distributed_data_libs/bench_spark.py` | `_discard` helper, +3 `BENCHMARKS` entries |
| `distributed-data-libs/distributed_data_libs/bench_dask.py` | `_discard` helper, +3 `BENCHMARKS` entries |
| `distributed-data-libs/Dockerfile.aaiclick` | pin `aaiclick==0.0.16` |
| `pyproject.toml` | `aaiclick>=0.0.16` |
| `uv.lock` | regenerate |
| `distributed-data-libs/SPEC.md` | execute-companion subsection, counts, per-framework notes |
| `distributed-data-libs/README.md` | op-count sentence |

No changes: `report.py`, `runner.py`, `stats.py`, `generate_data.py`,
`docker-compose.yml`, `.github/workflows/`.

## Verification

The full benchmark requires Docker + cgroup v2 and is heavy (10M rows, 3
frameworks). Verification approach:

- **Static:** import/lint each adapter; confirm `BENCHMARKS` keys match the new
  `BENCH_NAMES` entries for every framework.
- **API check:** confirm against the installed aaiclick 0.0.16 that
  `View.execute()` returns `QueryStats` and creates no table (mirrors
  `test_execute_stats.py`).
- **Full run:** `./distributed-data-libs.sh` (Docker) is the integration test;
  note explicitly if it cannot be run in the current environment.

## Risks / notes

- **Dask sort discard:** `sort_values(...).map_partitions(len).compute().sum()`
  relies on Dask not pruning the shuffle when only a count is requested. This is
  the same construct the project shipped previously, so it is treated as an
  accepted precedent rather than a new risk.
- **aaiclick must use the View path**, not `LazyOperator.execute()`, for the
  multiply discard — the latter materializes a table first.
