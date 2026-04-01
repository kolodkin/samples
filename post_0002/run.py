#!/usr/bin/env python3
"""Benchmark runner — generates data, measures all libraries, prints report.

Libraries may define a context() that wraps convert + benchmarks.
This lets chdb release its :memory: engine before aaiclick claims its own.
"""

import asyncio
import contextlib
import random
import time
import tracemalloc

from . import (
    bench_aaiclick,
    bench_chdb,
    bench_numpy,
    bench_pandas,
    bench_polars,
    bench_pyarrow,
    bench_python,
)
from .config import BENCH_NAMES, CATEGORIES, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from .report import console, print_results

MODULES = [
    bench_python,
    bench_numpy,
    bench_pandas,
    bench_pyarrow,
    bench_polars,
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


def measure_sync(fn, data, num_runs):
    fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        fn(data)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(elapsed)
        peak_mem = max(peak_mem, peak)
    return sum(times) / num_runs, peak_mem


async def measure_async(fn, data, num_runs):
    await fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        await fn(data)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(elapsed)
        peak_mem = max(peak_mem, peak)
    return sum(times) / num_runs, peak_mem


async def bench_module(mod, raw_data, results):
    """Run all benchmarks for one library, inside its context if defined."""
    is_async = getattr(mod, "IS_ASYNC", False)

    if is_async:
        dataset = await mod.convert(raw_data)
    else:
        dataset = mod.convert(raw_data)

    for bench_name in BENCH_NAMES:
        if bench_name not in mod.BENCHMARKS:
            continue
        console.print(f"  {bench_name} [{mod.NAME}]...")
        fn = mod.BENCHMARKS[bench_name]
        if is_async:
            avg_time, peak_mem = await measure_async(fn, dataset, NUM_RUNS)
        else:
            avg_time, peak_mem = measure_sync(fn, dataset, NUM_RUNS)
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
        ctx_fn = getattr(mod, "context", None)
        if ctx_fn is None:
            await bench_module(mod, raw_data, results)
        else:
            ctx = ctx_fn()
            if hasattr(ctx, "__aenter__"):
                async with ctx:
                    await bench_module(mod, raw_data, results)
            else:
                with ctx:
                    await bench_module(mod, raw_data, results)

    print_results(results, lib_names, NUM_ROWS)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
