"""Each (library, op) pair runs in a fresh spawn child so memory measurements
don't bleed between libraries. Peak memory is `getrusage(RUSAGE_SELF).ru_maxrss`
delta sampled after setup (raw_data load + import + convert), giving the op's
incremental high-water mark."""

import multiprocessing as mp
import os
import pickle
import platform
import random
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor

from rich.markup import escape

from .config import BENCH_NAMES, CATEGORIES, INGEST, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from .report import console, fmt_bytes, print_results

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


def _cpu_model():
    if sys.platform == "linux":
        try:
            with open("/proc/cpuinfo") as f:
                fields = {}
                for line in f:
                    if ":" not in line:
                        if fields:
                            break
                        continue
                    key, _, val = line.partition(":")
                    fields[key.strip()] = val.strip()
        except OSError:
            fields = {}
        name = fields.get("model name", "")
        if name and name.lower() != "unknown":
            return name
        vendor = fields.get("vendor_id", "")
        family = fields.get("cpu family", "")
        model = fields.get("model", "")
        parts = [p for p in (vendor,
                             f"family {family}" if family else "",
                             f"model {model}" if model else "") if p]
        if parts:
            return " ".join(parts)
    elif sys.platform == "darwin":
        r = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True, text=True, check=False,
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    return platform.processor() or "unknown"


def _print_machine_info():
    import psutil  # parent-only; keep out of spawn-child import cost
    disk = shutil.disk_usage(".")
    rows = [
        ("OS",   f"{platform.system()} {platform.release()}"),
        ("CPU",  f"{_cpu_model()} ({psutil.cpu_count(logical=True)} cores)"),
        ("RAM",  fmt_bytes(psutil.virtual_memory().total)),
        ("Disk", f"{fmt_bytes(disk.free)} free / {fmt_bytes(disk.total)} total"),
    ]
    console.print("[bold]Machine[/bold]")
    for label, value in rows:
        console.print(f"  {label:4s}  {value}")


def _generate_raw_data():
    random.seed(42)
    return {
        "id": list(range(NUM_ROWS)),
        "category": [random.choice(CATEGORIES) for _ in range(NUM_ROWS)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(NUM_ROWS)],
        "amount": [random.uniform(0, 1000) for _ in range(NUM_ROWS)],
        "quantity": [random.randint(1, 100) for _ in range(NUM_ROWS)],
    }


def _time_sync(target, args):
    baseline = _ru_maxrss_bytes()
    target(*args)
    times = []
    for _ in range(NUM_RUNS):
        t0 = time.perf_counter()
        target(*args)
        times.append(time.perf_counter() - t0)
    return sum(times) / NUM_RUNS, max(0, _ru_maxrss_bytes() - baseline)


async def _time_async(target, args):
    baseline = _ru_maxrss_bytes()
    await target(*args)
    times = []
    for _ in range(NUM_RUNS):
        t0 = time.perf_counter()
        await target(*args)
        times.append(time.perf_counter() - t0)
    return sum(times) / NUM_RUNS, max(0, _ru_maxrss_bytes() - baseline)


def _sync_flow(mod, bench_name, data):
    if bench_name == INGEST:
        target, args = mod.convert, (data,)
    elif bench_name in mod.BENCHMARKS:
        target, args = mod.BENCHMARKS[bench_name], (mod.convert(data),)
    else:
        return None
    return _time_sync(target, args)


def _sync_ctx_flow(mod, bench_name, data):
    with mod.context():
        return _sync_flow(mod, bench_name, data)


async def _async_flow(mod, bench_name, data):
    if bench_name == INGEST:
        target, args = mod.convert, (data,)
    elif bench_name in mod.BENCHMARKS:
        dataset = await mod.convert(data)
        target, args = mod.BENCHMARKS[bench_name], (dataset,)
    else:
        return None
    return await _time_async(target, args)


async def _async_ctx_flow(mod, bench_name, data):
    async with mod.context():
        return await _async_flow(mod, bench_name, data)


def _child_measure(mod_name, bench_name, data_path):
    """Spawn child entry point. Returns (avg_time, peak_bytes, version) or None."""
    import asyncio
    import importlib

    with open(data_path, "rb") as f:
        data = pickle.load(f)

    mod = importlib.import_module(f"python_data_libs.{mod_name}")
    is_async = getattr(mod, "IS_ASYNC", False)
    has_ctx = hasattr(mod, "context")

    if bench_name == INGEST and getattr(mod, "SKIP_INGEST", False):
        return None

    if is_async and has_ctx:
        result = asyncio.run(_async_ctx_flow(mod, bench_name, data))
    elif is_async:
        result = asyncio.run(_async_flow(mod, bench_name, data))
    elif has_ctx:
        result = _sync_ctx_flow(mod, bench_name, data)
    else:
        result = _sync_flow(mod, bench_name, data)

    if result is None:
        return None
    avg, peak = result
    return avg, peak, getattr(mod, "VERSION", "")


def main():
    console.print("\n[bold]Python Data Library Benchmark[/bold]")
    _print_machine_info()
    console.print(f"\n[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")
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
