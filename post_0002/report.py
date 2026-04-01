"""Pretty-print benchmark results as rich tables."""

from rich.console import Console
from rich.table import Table

from .config import NUM_RUNS

console = Console()


def fmt_time(seconds):
    """Format time: s >= 2s, ms >= 2ms, us >= 2us, ns otherwise."""
    if seconds >= 2:
        return f"{seconds:.1f}s"
    if seconds >= 2e-3:
        return f"{seconds * 1e3:.1f}ms"
    if seconds >= 2e-6:
        return f"{seconds * 1e6:.1f}us"
    return f"{seconds * 1e9:.1f}ns"


def print_results(results, lib_names, num_rows):
    table = Table(title=f"Data Library Benchmark — {num_rows:,} rows, {NUM_RUNS} runs")
    table.add_column("Operation", style="bold", no_wrap=True)
    for lib in lib_names:
        table.add_column(lib, justify="right")
    table.add_column("Fastest", justify="right", style="green")

    for bench_name, lib_results in results.items():
        row = [bench_name]
        times = {}
        for lib in lib_names:
            if lib in lib_results:
                t = lib_results[lib]["time"]
                times[lib] = t
                row.append(fmt_time(t))
            else:
                row.append("—")

        fastest_lib = min(times, key=times.get)
        slowest_time = max(times.values())
        fastest_time = times[fastest_lib]
        if fastest_time > 0 and fastest_time < slowest_time:
            speedup = slowest_time / fastest_time
            row.append(f"{fastest_lib} ~x{speedup:.1f}")
        else:
            row.append(fastest_lib)

        table.add_row(*row)

    console.print()
    console.print(table)

    mem_table = Table(title="Peak Memory")
    mem_table.add_column("Operation", style="bold", no_wrap=True)
    for lib in lib_names:
        mem_table.add_column(lib, justify="right")

    for bench_name, lib_results in results.items():
        row = [bench_name]
        for lib in lib_names:
            if lib in lib_results:
                mem = lib_results[lib]["memory"]
                if mem < 1024:
                    row.append(f"{mem}B")
                elif mem < 1024 * 1024:
                    row.append(f"{mem / 1024:.1f}KB")
                else:
                    row.append(f"{mem / (1024 * 1024):.1f}MB")
            else:
                row.append("—")
        mem_table.add_row(*row)

    console.print()
    console.print(mem_table)
