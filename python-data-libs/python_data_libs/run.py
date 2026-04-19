"""Each (library, op) pair runs in a fresh spawn child so memory measurements
don't bleed between libraries. Peak memory is `getrusage(RUSAGE_SELF).ru_maxrss`
delta sampled after setup (raw_data load + import + convert), giving the op's
incremental high-water mark."""

import multiprocessing as mp
import os
import pickle
import random
import resource
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor

from rich.markup import escape

from .config import BENCH_NAMES, CATEGORIES, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from .report import console, print_results

# Hardcoded so the parent never imports any bench module — otherwise every
# heavy library would load in the parent too.
LIBRARIES = [
    ("bench_python", "python"),
    ("bench_numpy", "numpy"),
    ("bench_pandas", "pandas"),
    ("bench_pyarrow", "pyarrow"),
    ("bench_polars", "polars"),
    ("bench_sqlite", "sqlite"),
    ("bench_sqlite_indexed", "sqlite+idx"),
    ("bench_duckdb", "duckdb"),
    ("bench_chdb", "chdb"),
    ("bench_aaiclick", "aaiclick"),
]


def _ru_maxrss_bytes():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


def _generate_raw_data():
    random.seed(42)
    return {
        "id": list(range(NUM_ROWS)),
        "category": [random.choice(CATEGORIES) for _ in range(NUM_ROWS)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(NUM_ROWS)],
        "amount": [random.uniform(0, 1000) for _ in range(NUM_ROWS)],
        "quantity": [random.randint(1, 100) for _ in range(NUM_ROWS)],
    }


def _child_measure(mod_name, bench_name, data_path):
    """Returns (avg_time, peak_bytes, version) or None if op unsupported."""
    import asyncio
    import importlib
    import time

    with open(data_path, "rb") as f:
        data = pickle.load(f)

    mod = importlib.import_module(f"python_data_libs.{mod_name}")
    is_async = getattr(mod, "IS_ASYNC", False)
    ctx_fn = getattr(mod, "context", None)

    async def call_fn(fn, *args):
        return await fn(*args) if is_async else fn(*args)

    async def time_loop(call):
        await call()  # warmup — not timed; ru_maxrss still captures its allocations
        times = []
        for _ in range(NUM_RUNS):
            t0 = time.perf_counter()
            await call()
            times.append(time.perf_counter() - t0)
        return sum(times) / NUM_RUNS

    async def run_bench():
        if bench_name == "Ingest":
            target, args = mod.convert, (data,)
        elif bench_name in mod.BENCHMARKS:
            dataset = await call_fn(mod.convert, data)
            target, args = mod.BENCHMARKS[bench_name], (dataset,)
        else:
            return None

        async def call():
            await call_fn(target, *args)

        baseline = _ru_maxrss_bytes()
        avg = await time_loop(call)
        return avg, max(0, _ru_maxrss_bytes() - baseline)

    async def with_ctx():
        if ctx_fn is None:
            return await run_bench()
        ctx = ctx_fn()
        if hasattr(ctx, "__aenter__"):
            async with ctx:
                return await run_bench()
        with ctx:
            return await run_bench()

    result = asyncio.run(with_ctx())
    if result is None:
        return None
    avg, peak = result
    return avg, peak, getattr(mod, "VERSION", "")


def main():
    console.print("\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")
    console.print("[dim]peak memory = ru_maxrss delta after setup (raw_data + import + convert)[/dim]\n")

    results = {b: {} for b in BENCH_NAMES}
    versions = {}

    fd, data_path = tempfile.mkstemp(prefix="pdl_raw_", suffix=".pkl")
    with os.fdopen(fd, "wb") as f:
        pickle.dump(_generate_raw_data(), f, protocol=pickle.HIGHEST_PROTOCOL)

    # max_tasks_per_child=1 forces a fresh worker per submit, so each
    # measurement gets a clean process and ru_maxrss starts from scratch.
    pool = ProcessPoolExecutor(
        max_workers=1,
        max_tasks_per_child=1,
        mp_context=mp.get_context("spawn"),
    )
    try:
        for mod_name, lib in LIBRARIES:
            for bench_name in BENCH_NAMES:
                console.print(f"  {bench_name} {escape(f'[{lib}]')}...")
                try:
                    r = pool.submit(_child_measure, mod_name, bench_name, data_path).result()
                except Exception as e:
                    console.print(f"    [red]skipped:[/red] {e}")
                    continue
                if r is None:
                    continue
                avg, peak, version = r
                results[bench_name][lib] = {"time": avg, "memory": peak}
                if version and lib not in versions:
                    versions[lib] = version
    finally:
        pool.shutdown(wait=True)
        os.unlink(data_path)

    if versions:
        console.print(
            "\n[bold]Versions:[/bold] "
            + ", ".join(f"{k} {v}" for k, v in versions.items())
        )
    print_results(results, [name for _, name in LIBRARIES], NUM_ROWS)


if __name__ == "__main__":
    main()
