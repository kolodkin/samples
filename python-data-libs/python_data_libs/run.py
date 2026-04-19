"""Benchmark runner.

Each (library, operation) pair runs in a fresh `spawn` child process so that:
  - allocator state is isolated between libraries (no fragmentation bleed);
  - peak memory is `getrusage(RUSAGE_SELF).ru_maxrss` of the child — a kernel-
    maintained high-water mark, no sampling, no missed spikes.

Reported memory is a *delta*: ru_maxrss at end minus baseline sampled after the
setup cost (raw_data load, library import, and for non-Ingest ops also the
one-shot convert()). ru_maxrss is monotonic, so the delta cleanly isolates
the op's incremental memory.
"""

import multiprocessing as mp
import os
import pickle
import random
import sys
import tempfile
import traceback
from queue import Empty

from .config import BENCH_NAMES, CATEGORIES, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from .report import console, print_results

# (module filename, display name). Hardcoded so the parent never imports any
# bench module — otherwise every heavy library would load in the parent too.
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
    import resource
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


def _child_measure(mod_name, bench_name, data_path, queue):
    import asyncio
    import importlib
    import time

    try:
        with open(data_path, "rb") as f:
            data = pickle.load(f)

        mod = importlib.import_module(f"python_data_libs.{mod_name}")
        is_async = getattr(mod, "IS_ASYNC", False)
        ctx_fn = getattr(mod, "context", None)

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
                async def call():
                    if is_async:
                        await mod.convert(data)
                    else:
                        mod.convert(data)
                baseline = _ru_maxrss_bytes()
                avg = await time_loop(call)
                return avg, max(0, _ru_maxrss_bytes() - baseline)

            if bench_name not in mod.BENCHMARKS:
                return None

            fn = mod.BENCHMARKS[bench_name]
            dataset = await mod.convert(data) if is_async else mod.convert(data)

            async def call():
                if is_async:
                    await fn(dataset)
                else:
                    fn(dataset)
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
            queue.put({"time": None, "memory": None, "version": "", "error": None})
            return
        avg_time, peak = result
        queue.put({
            "time": avg_time,
            "memory": peak,
            "version": getattr(mod, "VERSION", ""),
            "error": None,
        })
    except Exception:
        queue.put({
            "time": None,
            "memory": None,
            "version": "",
            "error": traceback.format_exc(),
        })


def _spawn_measure(mod_name, bench_name, data_path):
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    proc = ctx.Process(target=_child_measure, args=(mod_name, bench_name, data_path, queue))
    proc.start()
    proc.join()
    try:
        return queue.get(timeout=5)
    except Empty:
        return {
            "time": None,
            "memory": None,
            "version": "",
            "error": f"child exited with code {proc.exitcode} without reporting",
        }


def main():
    console.print("\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")
    console.print("[dim]peak memory = ru_maxrss delta after setup (raw_data + import + convert)[/dim]\n")

    lib_names = [name for _, name in LIBRARIES]
    results = {b: {} for b in BENCH_NAMES}
    versions = {}

    with tempfile.NamedTemporaryFile(prefix="pdl_raw_", suffix=".pkl", delete=False) as f:
        data_path = f.name
        pickle.dump(_generate_raw_data(), f, protocol=pickle.HIGHEST_PROTOCOL)

    try:
        for mod_name, lib in LIBRARIES:
            for bench_name in BENCH_NAMES:
                console.print(f"  {bench_name} [{lib}]...")
                r = _spawn_measure(mod_name, bench_name, data_path)
                if r["error"]:
                    console.print(f"    [red]skipped:[/red] {r['error'].strip().splitlines()[-1]}")
                    continue
                if r["time"] is None:
                    continue
                results[bench_name][lib] = {"time": r["time"], "memory": r["memory"]}
                if r["version"] and lib not in versions:
                    versions[lib] = r["version"]
    finally:
        try:
            os.unlink(data_path)
        except OSError:
            pass

    if versions:
        console.print(
            "\n[bold]Versions:[/bold] "
            + ", ".join(f"{k} {v}" for k, v in versions.items())
        )
    print_results(results, lib_names, NUM_ROWS)


if __name__ == "__main__":
    main()
