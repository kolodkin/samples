#!/usr/bin/env python3
"""Benchmark orchestrator — claims tasks and runs each in a subprocess.

Each benchmark is executed via ``task_runner.py`` in a dedicated subprocess,
accepting only a task ID (``module:bench_name``).  This gives every task a
clean process with isolated memory, no import side-effects from other
libraries, and accurate RSS measurement.
"""

import asyncio
import json
import os
import subprocess
import sys

import bench_aaiclick
import bench_chdb
import bench_numpy
import bench_pandas
import bench_polars
import bench_pyarrow
import bench_python
import bench_sqlite
from config import BENCH_NAMES, NUM_ROWS, NUM_RUNS
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

# Map module NAME back to the key used by task_runner.py
_MODULE_KEY = {
    "python": "python",
    "numpy": "numpy",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "polars": "polars",
    "sqlite": "sqlite",
    "chdb": "chdb",
    "aaiclick": "aaiclick",
}

_TASK_RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task_runner.py")


def _run_task_subprocess(task_id: str) -> dict | None:
    """Spawn task_runner.py in a subprocess and return the JSON result."""
    result = subprocess.run(
        [sys.executable, _TASK_RUNNER, task_id],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    if result.returncode != 0:
        console.print(f"    [red]FAILED[/red] ({task_id})")
        if result.stderr:
            console.print(f"    {result.stderr.strip()}", style="dim")
        return None

    # The last non-empty line of stdout is the JSON result
    lines = [l for l in result.stdout.strip().splitlines() if l.strip()]
    if not lines:
        return None
    return json.loads(lines[-1])


async def bench_module(mod, results):
    """Claim all benchmark tasks for one library and run each in a subprocess."""
    mod_key = _MODULE_KEY[mod.NAME]

    for bench_name in BENCH_NAMES:
        if bench_name != "Ingest" and bench_name not in mod.BENCHMARKS:
            continue

        # Task claimed — run in subprocess
        task_id = f"{mod_key}:{bench_name}"
        console.print(f"  {bench_name} [{mod.NAME}]...")

        result = await asyncio.to_thread(_run_task_subprocess, task_id)

        if result is not None:
            results[bench_name][mod.NAME] = result


async def run():
    versions = [f"{m.NAME} {m.VERSION}" for m in MODULES]
    lib_names = [m.NAME for m in MODULES]

    console.print(f"\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"  {', '.join(versions)}\n")
    console.print(f"[bold]{NUM_ROWS:,} rows, {NUM_RUNS} runs per operation[/bold]")

    results = {name: {} for name in BENCH_NAMES}

    for mod in MODULES:
        await bench_module(mod, results)

    print_results(results, lib_names, NUM_ROWS)


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
