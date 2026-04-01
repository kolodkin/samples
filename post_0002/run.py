#!/usr/bin/env python3
"""Benchmark runner — generates data, measures all libraries, prints report."""

import asyncio
import random
import time
import tracemalloc

from aaiclick.data.data_context import data_context

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


async def run():
    lib_names = [m.NAME for m in MODULES]
    versions = [f"{m.NAME} {m.VERSION}" for m in MODULES]

    console.print(f"\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"  {', '.join(versions)}\n")
    console.print(f"[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")

    raw_data = generate_raw_data(NUM_ROWS)

    # Convert data for each library
    datasets = {}
    for mod in MODULES:
        is_async = getattr(mod, "IS_ASYNC", False)
        if is_async:
            datasets[mod.NAME] = await mod.convert(raw_data)
        else:
            datasets[mod.NAME] = mod.convert(raw_data)

    # Run benchmarks
    results = {}
    for bench_name in BENCH_NAMES:
        console.print(f"  {bench_name}...")
        results[bench_name] = {}
        for mod in MODULES:
            if bench_name not in mod.BENCHMARKS:
                continue
            fn = mod.BENCHMARKS[bench_name]
            is_async = getattr(mod, "IS_ASYNC", False)
            if is_async:
                avg_time, peak_mem = await measure_async(fn, datasets[mod.NAME], NUM_RUNS)
            else:
                avg_time, peak_mem = measure_sync(fn, datasets[mod.NAME], NUM_RUNS)
            results[bench_name][mod.NAME] = {"time": avg_time, "memory": peak_mem}

    print_results(results, lib_names, NUM_ROWS)


async def main():
    async with data_context():
        await run()


if __name__ == "__main__":
    asyncio.run(main())
