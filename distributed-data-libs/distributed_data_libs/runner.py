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
    resolve_measurement_targets,
    sum_cpu_usec,
    sum_memory_peak,
)


def _measurement_envelope():
    """Returns (snapshot_before, snapshot_after) closures that bracket the op.
    Reads memory.peak/cpu.stat from one cgroup (self) or many (when
    COMPUTE_CONTAINERS is set and we're measuring a client+sidecars
    topology). Uses delta of monotonic memory.peak — see stats.py."""
    if not cgroup_v2_available():
        raise RuntimeError("cgroup v2 not available — run on cgroup-v2 host")

    targets = resolve_measurement_targets()
    print(f"[runner] measuring cgroups: {targets}", flush=True)

    def before():
        return sum_memory_peak(targets), sum_cpu_usec(targets), time.perf_counter()

    def after(state):
        peak_before, cpu_before, t0 = state
        elapsed = (time.perf_counter() - t0) / NUM_RUNS
        return {
            "time": elapsed,
            "peak_mem": max(0, sum_memory_peak(targets) - peak_before),
            "cpu_usec": sum_cpu_usec(targets) - cpu_before,
        }

    return before, after


def _run_sync(mod):
    before, after = _measurement_envelope()
    results = {}

    state = before()
    for _ in range(NUM_RUNS):
        mod.convert(RAW_DATA_PATH)
    results[INGEST] = after(state)

    dataset = mod.convert(RAW_DATA_PATH)
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
    results = {}

    state = before()
    for _ in range(NUM_RUNS):
        await mod.convert(RAW_DATA_PATH)
    results[INGEST] = after(state)

    dataset = await mod.convert(RAW_DATA_PATH)
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

    # Adapters can override the entire flow by defining main() - used by
    # ray, which ships the actual benchmark to ray-head via the Ray Jobs
    # API and lets ray_job.py write results-ray.json itself (since the
    # runner can't drive Ray Data over the network - see bench_ray.py).
    if hasattr(mod, "main") and callable(mod.main):
        mod.main()
        return

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
