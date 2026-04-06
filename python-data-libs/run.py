#!/usr/bin/env python3
"""Benchmark runner — generates data, measures all libraries, prints report.

Libraries may define a context() that wraps convert + benchmarks.
This lets chdb release its :memory: engine before aaiclick claims its own.
"""

import asyncio
import contextlib
import os
import random
import time

import bench_aaiclick
import bench_chdb
import bench_numpy
import bench_pandas
import bench_polars
import bench_pyarrow
import bench_python
import bench_sqlite
from config import BENCH_NAMES, CATEGORIES, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from report import console, print_results

MODULES = [
    bench_python,
    bench_numpy,
    bench_pandas,
    bench_pyarrow,
    bench_polars,
    bench_sqlite,
    bench_chdb,
    bench_aaiclick,
]


def generate_raw_data(num_rows):
    random.seed(42)
    return {
        "id": list(range(num_rows)),
        "category": [random.choice(CATEGORIES) for _ in range(num_rows)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(num_rows)],
        "amount": [random.uniform(0, 1000) for _ in range(num_rows)],
        "quantity": [random.randint(1, 100) for _ in range(num_rows)],
    }


def _get_rss():
    """Return current RSS in bytes via /proc/self/statm (Linux)."""
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


@contextlib.contextmanager
def _nullctx():
    yield


async def _run_in_ctx(ctx_fn, coro_fn):
    """Run a coroutine inside a sync or async context (fresh session each time)."""
    if ctx_fn is None:
        return await coro_fn()
    ctx = ctx_fn()
    if hasattr(ctx, "__aenter__"):
        async with ctx:
            return await coro_fn()
    else:
        with ctx:
            return await coro_fn()


async def bench_module(mod, raw_data, results):
    """Run all benchmarks for one library, fresh context per operation."""
    is_async = getattr(mod, "IS_ASYNC", False)
    ctx_fn = getattr(mod, "context", None)

    for bench_name in BENCH_NAMES:
        if bench_name == "Ingest":
            console.print(f"  Ingest [{mod.NAME}]...")

            async def _ingest():
                if is_async:
                    return await measure_async(mod.convert, raw_data, NUM_RUNS)
                return measure_sync(mod.convert, raw_data, NUM_RUNS)

            avg_time, peak_mem = await _run_in_ctx(ctx_fn, _ingest)
            results["Ingest"][mod.NAME] = {"time": avg_time, "memory": peak_mem}
            continue
        if bench_name not in mod.BENCHMARKS:
            continue
        console.print(f"  {bench_name} [{mod.NAME}]...")
        fn = mod.BENCHMARKS[bench_name]

        async def _bench(fn=fn):
            if is_async:
                dataset = await mod.convert(raw_data)
                return await measure_async(fn, dataset, NUM_RUNS)
            dataset = mod.convert(raw_data)
            return measure_sync(fn, dataset, NUM_RUNS)

        avg_time, peak_mem = await _run_in_ctx(ctx_fn, _bench)
        results[bench_name][mod.NAME] = {"time": avg_time, "memory": peak_mem}


async def run():
    versions = [f"{m.NAME} {m.VERSION}" for m in MODULES]
    lib_names = [m.NAME for m in MODULES]

    console.print(f"\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"  {', '.join(versions)}\n")
    console.print(f"[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")

    raw_data = generate_raw_data(NUM_ROWS)
    results = {name: {} for name in BENCH_NAMES}

    for mod in MODULES:
        await bench_module(mod, raw_data, results)

    print_results(results, lib_names, NUM_ROWS)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
