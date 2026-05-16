"""Render benchmark results as rich tables from /data/results-<framework>.json
files written by each runner container."""

import glob
import json
import os

from rich.console import Console
from rich.table import Table

from .config import BENCH_NAMES, FRAMEWORKS, NUM_ROWS, NUM_RUNS, RESULTS_DIR

console = Console(width=200)


def fmt_time(seconds):
    if seconds >= 3600:
        return f"{seconds / 3600:.2f} hour"
    if seconds >= 60:
        return f"{seconds / 60:.2f} min"
    if seconds >= 1:
        return f"{seconds:.2f} sec"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    if seconds >= 1e-6:
        return f"{seconds * 1e6:.2f} us"
    return f"{seconds * 1e9:.2f} ns"


def fmt_bytes(n):
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def fmt_cpu(cpu_usec, wall_seconds):
    """CPU utilization = cpu_time / wall_time / cores. Reported as a multiplier
    (e.g. 3.2x means 3.2 cores worth of CPU were used on average)."""
    if wall_seconds <= 0:
        return "—"
    cores_used = (cpu_usec / 1e6) / (wall_seconds * NUM_RUNS)
    return f"{cores_used:.1f}x"


def _load_results():
    data = {}
    versions = {}
    for path in sorted(glob.glob(os.path.join(RESULTS_DIR, "results-*.json"))):
        with open(path) as f:
            payload = json.load(f)
        fwk = payload["framework"]
        data[fwk] = payload["results"]
        if payload.get("version"):
            versions[fwk] = payload["version"]
    return data, versions


def _render_metric(data, metric, formatter, title):
    table = Table(title=title)
    table.add_column("Operation", style="bold", no_wrap=True)
    for fwk in FRAMEWORKS:
        table.add_column(fwk, justify="right", no_wrap=True)

    for op in BENCH_NAMES:
        row = [op]
        for fwk in FRAMEWORKS:
            entry = data.get(fwk, {}).get(op)
            if entry is None:
                row.append("—")
            elif metric == "cpu":
                row.append(fmt_cpu(entry["cpu_usec"], entry["time"]))
            else:
                row.append(formatter(entry[metric]))
        table.add_row(*row)

    console.print()
    console.print(table)


def main():
    data, versions = _load_results()
    if not data:
        console.print("[red]no results found in /data/results-*.json[/red]")
        return

    console.print(f"\n[bold]Distributed Data Libs Benchmark — {NUM_ROWS:,} rows, {NUM_RUNS} runs[/bold]")
    if versions:
        console.print(
            "[bold]Versions:[/bold] "
            + ", ".join(f"{k} {v}" for k, v in versions.items())
        )
    console.print(
        "[dim]peak memory = cgroup memory.peak (reset before each op); "
        "CPU = cores-used average over op duration (sum across container)[/dim]"
    )

    _render_metric(data, "time", fmt_time, "Wall-clock time per op (avg)")
    _render_metric(data, "peak_mem", fmt_bytes, "Peak memory (cgroup memory.peak)")
    _render_metric(data, "cpu", None, "CPU utilization (cores)")


if __name__ == "__main__":
    main()
