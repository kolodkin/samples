#!/usr/bin/env python3
"""Subprocess task runner — accepts a task ID, runs a single benchmark, prints JSON result.

Task ID format: ``module_name:bench_name``  (e.g. ``pandas:Ingest``).

Output on stdout (last line):
    {"time": <float>, "memory": <int>}

All other output (Rich console, library warnings) goes to stderr so the
orchestrator can parse the JSON cleanly.
"""

import asyncio
import json
import sys

# Redirect rich / library output to stderr so stdout stays clean for JSON.
import os
os.environ.setdefault("COLUMNS", "200")

from config import BENCH_NAMES, NUM_ROWS, NUM_RUNS  # noqa: E402

# ── module registry (matches run.py MODULES order) ──────────────────────
MODULE_MAP = {
    "python": "bench_python",
    "numpy": "bench_numpy",
    "pandas": "bench_pandas",
    "pyarrow": "bench_pyarrow",
    "polars": "bench_polars",
    "sqlite": "bench_sqlite",
    "chdb": "bench_chdb",
    "aaiclick": "bench_aaiclick",
}


def _parse_task_id(task_id: str):
    """Return (module_name, bench_name) from a task ID string."""
    sep = task_id.index(":")
    mod_key = task_id[:sep]
    bench_name = task_id[sep + 1:]
    if mod_key not in MODULE_MAP:
        raise ValueError(f"Unknown module key: {mod_key!r}")
    if bench_name not in BENCH_NAMES:
        raise ValueError(f"Unknown benchmark: {bench_name!r}")
    return mod_key, bench_name


# ── measurement helpers (duplicated from run.py to keep subprocess self-contained) ──

import contextlib  # noqa: E402
import random  # noqa: E402
import time  # noqa: E402


def _get_rss():
    with open("/proc/self/statm") as f:
        pages = int(f.read().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE")


def measure_sync(fn, data, num_runs):
    fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        rss_before = _get_rss()
        t0 = time.perf_counter()
        fn(data)
        elapsed = time.perf_counter() - t0
        rss_after = _get_rss()
        times.append(elapsed)
        peak_mem = max(peak_mem, rss_after - rss_before)
    return sum(times) / num_runs, peak_mem


async def measure_async(fn, data, num_runs):
    await fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        rss_before = _get_rss()
        t0 = time.perf_counter()
        await fn(data)
        elapsed = time.perf_counter() - t0
        rss_after = _get_rss()
        times.append(elapsed)
        peak_mem = max(peak_mem, rss_after - rss_before)
    return sum(times) / num_runs, peak_mem


async def _run_in_ctx(ctx_fn, coro_fn):
    if ctx_fn is None:
        return await coro_fn()
    ctx = ctx_fn()
    if hasattr(ctx, "__aenter__"):
        async with ctx:
            return await coro_fn()
    else:
        with ctx:
            return await coro_fn()


def generate_raw_data(num_rows):
    random.seed(42)
    return {
        "id": list(range(num_rows)),
        "category": [random.choice([f"cat_{i}" for i in range(10)]) for _ in range(num_rows)],
        "subcategory": [random.choice([f"sub_{i}" for i in range(1000)]) for _ in range(num_rows)],
        "amount": [random.uniform(0, 1000) for _ in range(num_rows)],
        "quantity": [random.randint(1, 100) for _ in range(num_rows)],
    }


# ── main entry point ────────────────────────────────────────────────────

async def run_task(task_id: str):
    mod_key, bench_name = _parse_task_id(task_id)

    # Import the benchmark module
    import importlib
    mod = importlib.import_module(MODULE_MAP[mod_key])

    is_async = getattr(mod, "IS_ASYNC", False)
    ctx_fn = getattr(mod, "context", None)

    raw_data = generate_raw_data(NUM_ROWS)

    if bench_name == "Ingest":
        async def _ingest():
            if is_async:
                return await measure_async(mod.convert, raw_data, NUM_RUNS)
            return measure_sync(mod.convert, raw_data, NUM_RUNS)

        avg_time, peak_mem = await _run_in_ctx(ctx_fn, _ingest)
    else:
        if bench_name not in mod.BENCHMARKS:
            # Module doesn't implement this benchmark
            print(json.dumps(None))
            return

        fn = mod.BENCHMARKS[bench_name]

        async def _bench(fn=fn):
            if is_async:
                dataset = await mod.convert(raw_data)
                return await measure_async(fn, dataset, NUM_RUNS)
            dataset = mod.convert(raw_data)
            return measure_sync(fn, dataset, NUM_RUNS)

        avg_time, peak_mem = await _run_in_ctx(ctx_fn, _bench)

    print(json.dumps({"time": avg_time, "memory": peak_mem}))


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <module_name>:<bench_name>", file=sys.stderr)
        sys.exit(1)
    task_id = sys.argv[1]
    asyncio.run(run_task(task_id))


if __name__ == "__main__":
    main()
