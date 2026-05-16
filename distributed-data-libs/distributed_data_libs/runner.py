"""Per-framework runner — executes inside the framework's container.

Loads /data/raw.parquet, loops through all ops, and for each op:
  - resets cgroup memory.peak
  - snapshots cpu.stat usage_usec
  - times NUM_RUNS invocations with perf_counter (compute only)
  - reads memory.peak and cpu.stat delta
Writes /data/results-<framework>.json. The container exits when done; the
host orchestrator brings the service down and renders the report."""

import asyncio
import importlib
import json
import os
import time

from .config import BENCH_NAMES, INGEST, NUM_RUNS, RAW_DATA_PATH, RESULTS_DIR
from .stats import (
    cgroup_v2_available,
    read_cpu_usec,
    read_memory_peak,
    reset_memory_peak,
)


def _load_raw():
    import pyarrow.parquet as pq
    table = pq.read_table(RAW_DATA_PATH)
    return {col: table.column(col).to_pylist() for col in table.column_names}


def _measurement_envelope():
    """Returns (snapshot_before, snapshot_after) closures that bracket the op."""
    if not cgroup_v2_available():
        raise RuntimeError("cgroup v2 not available — run on cgroup-v2 host")

    def before():
        reset_memory_peak()
        return read_cpu_usec(), time.perf_counter()

    def after(state):
        cpu_before, t0 = state
        elapsed = (time.perf_counter() - t0) / NUM_RUNS
        return {
            "time": elapsed,
            "peak_mem": read_memory_peak(),
            "cpu_usec": read_cpu_usec() - cpu_before,
        }

    return before, after


def _run_sync(mod):
    before, after = _measurement_envelope()
    raw = _load_raw()
    results = {}

    state = before()
    for _ in range(NUM_RUNS):
        mod.convert(raw)
    results[INGEST] = after(state)

    dataset = mod.convert(raw)
    for op_name in BENCH_NAMES:
        if op_name == INGEST or op_name not in mod.BENCHMARKS:
            continue
        print(f"  {op_name}...", flush=True)
        fn = mod.BENCHMARKS[op_name]
        state = before()
        for _ in range(NUM_RUNS):
            fn(dataset)
        results[op_name] = after(state)

    return results


async def _run_async(mod):
    before, after = _measurement_envelope()
    raw = _load_raw()
    results = {}

    state = before()
    for _ in range(NUM_RUNS):
        await mod.convert(raw)
    results[INGEST] = after(state)

    dataset = await mod.convert(raw)
    for op_name in BENCH_NAMES:
        if op_name == INGEST or op_name not in mod.BENCHMARKS:
            continue
        print(f"  {op_name}...", flush=True)
        fn = mod.BENCHMARKS[op_name]
        state = before()
        for _ in range(NUM_RUNS):
            await fn(dataset)
        results[op_name] = after(state)

    return results


def main():
    fwk = os.environ["FRAMEWORK"]
    print(f"[runner] framework={fwk}", flush=True)
    mod = importlib.import_module(f"distributed_data_libs.bench_{fwk}")
    is_async = getattr(mod, "IS_ASYNC", False)
    has_ctx = hasattr(mod, "context")
    async_ctx = getattr(mod, "ASYNC_CONTEXT", False)

    if is_async:
        async def driver():
            if has_ctx and async_ctx:
                async with mod.context():
                    return await _run_async(mod)
            elif has_ctx:
                with mod.context():
                    return await _run_async(mod)
            return await _run_async(mod)

        results = asyncio.run(driver())
    else:
        if has_ctx:
            with mod.context():
                results = _run_sync(mod)
        else:
            results = _run_sync(mod)

    out_path = os.path.join(RESULTS_DIR, f"results-{fwk}.json")
    with open(out_path, "w") as f:
        json.dump({
            "framework": fwk,
            "version": getattr(mod, "VERSION", ""),
            "results": results,
        }, f, indent=2)
    print(f"wrote {out_path}", flush=True)


if __name__ == "__main__":
    main()
