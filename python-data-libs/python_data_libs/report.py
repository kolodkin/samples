"""Pretty-print benchmark results as rich tables."""

from rich.console import Console
from rich.table import Table

from .config import NUM_RUNS

console = Console(width=200)


def fmt_time(seconds):
    """Format time with full unit name and .2 precision."""
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


def print_results(results, lib_names, num_rows):
    table = Table(title=f"Data Library Benchmark — {num_rows:,} rows, {NUM_RUNS} runs")
    table.add_column("Operation", style="bold", no_wrap=True)
    for lib in lib_names:
        table.add_column(lib, justify="right", no_wrap=True)
    table.add_column("Fastest", justify="right", style="green", no_wrap=True)

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

    mem_table = Table(title="Peak RSS (ru_maxrss, fresh process per op)")
    mem_table.add_column("Operation", style="bold", no_wrap=True)
    for lib in lib_names:
        mem_table.add_column(lib, justify="right", no_wrap=True)

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
