#!/usr/bin/env python3
"""Benchmark runner — generates data, measures all libraries, prints report.

chdb runs first with :memory:, then its session is closed so aaiclick
can initialize its own chdb engine with a disk path.
"""

import asyncio
import random
import time
import tracemalloc

from . import (
    bench_chdb,
    bench_numpy,
    bench_pandas,
    bench_polars,
    bench_pyarrow,
    bench_python,
)
from .config import BENCH_NAMES, CATEGORIES, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from .report import console, print_results

# chdb runs first (before aaiclick claims the engine)
PHASE1_MODULES = [
    bench_python,
    bench_numpy,
    bench_pandas,
    bench_pyarrow,
    bench_polars,
    bench_chdb,
]

ALL_LIB_NAMES = ["python", "numpy", "pandas", "pyarrow", "polars", "chdb", "aaiclick"]


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
    import chdb
    from . import bench_aaiclick

    versions = [f"{m.NAME} {m.VERSION}" for m in PHASE1_MODULES]
    versions.append(f"aaiclick {bench_aaiclick.VERSION}")

    console.print(f"\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"  {', '.join(versions)}\n")
    console.print(f"[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")

    raw_data = generate_raw_data(NUM_ROWS)

    # Phase 1: all sync libraries including chdb (:memory:)
    datasets = {}
    for mod in PHASE1_MODULES:
        datasets[mod.NAME] = mod.convert(raw_data)

    results = {}
    for bench_name in BENCH_NAMES:
        console.print(f"  {bench_name}...")
        results[bench_name] = {}
        for mod in PHASE1_MODULES:
            if bench_name not in mod.BENCHMARKS:
                continue
            fn = mod.BENCHMARKS[bench_name]
            avg_time, peak_mem = measure_sync(fn, datasets[mod.NAME], NUM_RUNS)
            results[bench_name][mod.NAME] = {"time": avg_time, "memory": peak_mem}

    # Close chdb session to release the engine
    chdb_session = datasets["chdb"]
    chdb_session.cleanup()
    chdb_session.close()
    del datasets["chdb"]

    # Phase 2: aaiclick (initializes its own chdb engine)
    from aaiclick.data.data_context import data_context

    async with data_context():
        aaiclick_data = await bench_aaiclick.convert(raw_data)
        for bench_name in BENCH_NAMES:
            console.print(f"  {bench_name} [aaiclick]...")
            if bench_name not in bench_aaiclick.BENCHMARKS:
                continue
            fn = bench_aaiclick.BENCHMARKS[bench_name]
            avg_time, peak_mem = await measure_async(fn, aaiclick_data, NUM_RUNS)
            results[bench_name]["aaiclick"] = {"time": avg_time, "memory": peak_mem}

    print_results(results, ALL_LIB_NAMES, NUM_ROWS)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
