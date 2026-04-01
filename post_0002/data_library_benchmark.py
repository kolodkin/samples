#!/usr/bin/env python3
"""Benchmark column operations and group-by across Python data libraries.

Compares Native Python, NumPy, Pandas, PyArrow, Polars, and aaiclick
on a 1M-row dataset. Speedup ratios are relative to the slowest library.
"""

import asyncio
import random
import time
import tracemalloc
from collections import Counter, defaultdict
from statistics import mean

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from rich.console import Console
from rich.table import Table

import aaiclick
from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context

NUM_RUNS = 10
FILTER_THRESHOLD = 500.0

CATEGORIES = [f"cat_{i}" for i in range(10)]
SUBCATEGORIES = [f"sub_{i}" for i in range(1000)]

ALL_LIBRARIES = ["python", "numpy", "pandas", "pyarrow", "polars", "aaiclick"]

console = Console()


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def generate_raw_data(num_rows):
    random.seed(42)
    return {
        "id": list(range(num_rows)),
        "category": [random.choice(CATEGORIES) for _ in range(num_rows)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(num_rows)],
        "amount": [random.uniform(0, 1000) for _ in range(num_rows)],
        "quantity": [random.randint(1, 100) for _ in range(num_rows)],
    }


def to_numpy(data):
    return {
        "id": np.array(data["id"]),
        "category": np.array(data["category"]),
        "subcategory": np.array(data["subcategory"]),
        "amount": np.array(data["amount"]),
        "quantity": np.array(data["quantity"]),
    }


def to_pandas(data):
    return pd.DataFrame(data)


def to_pyarrow(data):
    return pa.table(data)


def to_polars(data):
    return pl.DataFrame(data)


async def to_aaiclick(data):
    return await create_object_from_value(data)


# ---------------------------------------------------------------------------
# Column operations
# ---------------------------------------------------------------------------

# -- Column sum --

def col_sum_python(data):
    return sum(data["amount"])


def col_sum_numpy(data):
    return data["amount"].sum()


def col_sum_pandas(df):
    return df["amount"].sum()


def col_sum_pyarrow(table):
    return pc.sum(table.column("amount")).as_py()


def col_sum_polars(df):
    return df["amount"].sum()


async def col_sum_aaiclick(obj):
    result = await obj["amount"].sum()
    return await result.data()


# -- Column multiply --

def col_mul_python(data):
    return [a * q for a, q in zip(data["amount"], data["quantity"])]


def col_mul_numpy(data):
    return data["amount"] * data["quantity"]


def col_mul_pandas(df):
    return df["amount"] * df["quantity"]


def col_mul_pyarrow(table):
    return pc.multiply(table.column("amount"), table.column("quantity"))


def col_mul_polars(df):
    return (df["amount"] * df["quantity"])


async def col_mul_aaiclick(obj):
    result = await (obj["amount"] * obj["quantity"])
    return await result.data()


# -- Filter rows --

def filter_python(data):
    return {
        k: [v for v, a in zip(data[k], data["amount"]) if a > FILTER_THRESHOLD]
        for k in data
    }


def filter_numpy(data):
    mask = data["amount"] > FILTER_THRESHOLD
    return {k: v[mask] for k, v in data.items()}


def filter_pandas(df):
    return df[df["amount"] > FILTER_THRESHOLD]


def filter_pyarrow(table):
    return table.filter(pc.greater(table.column("amount"), FILTER_THRESHOLD))


def filter_polars(df):
    return df.filter(pl.col("amount") > FILTER_THRESHOLD)


async def filter_aaiclick(obj):
    result = obj.where(f"amount > {FILTER_THRESHOLD}")
    return await result.data()


# -- Sort --

def sort_python(data):
    idx = sorted(range(len(data["amount"])), key=lambda i: data["amount"][i], reverse=True)
    return {k: [data[k][i] for i in idx] for k in data}


def sort_numpy(data):
    idx = np.argsort(data["amount"])[::-1]
    return {k: v[idx] for k, v in data.items()}


def sort_pandas(df):
    return df.sort_values("amount", ascending=False)


def sort_pyarrow(table):
    return table.sort_by([("amount", "descending")])


def sort_polars(df):
    return df.sort("amount", descending=True)


async def sort_aaiclick(obj):
    result = obj.view(order_by="amount DESC")
    return await result.data()


# -- Count distinct --

def count_distinct_python(data):
    return len(set(data["category"]))


def count_distinct_numpy(data):
    return len(np.unique(data["category"]))


def count_distinct_pandas(df):
    return df["category"].nunique()


def count_distinct_pyarrow(table):
    return pc.count_distinct(table.column("category")).as_py()


def count_distinct_polars(df):
    return df["category"].n_unique()


async def count_distinct_aaiclick(obj):
    uniq = await obj["category"].unique()
    result = await uniq.count()
    return await result.data()


# ---------------------------------------------------------------------------
# Group-by operations
# ---------------------------------------------------------------------------

# -- Group-by + sum --

def groupby_sum_python(data):
    result = defaultdict(float)
    for cat, amt in zip(data["category"], data["amount"]):
        result[cat] += amt
    return dict(result)


def groupby_sum_numpy(data):
    cats = data["category"]
    amounts = data["amount"]
    uniq, inverse = np.unique(cats, return_inverse=True)
    sums = np.zeros(len(uniq))
    np.add.at(sums, inverse, amounts)
    return dict(zip(uniq, sums))


def groupby_sum_pandas(df):
    return df.groupby("category")["amount"].sum()


def groupby_sum_pyarrow(table):
    return table.group_by("category").aggregate([("amount", "sum")])


def groupby_sum_polars(df):
    return df.group_by("category").agg(pl.col("amount").sum())


async def groupby_sum_aaiclick(obj):
    result = await obj.group_by("category").sum("amount")
    return await result.data()


# -- Group-by + count --

def groupby_count_python(data):
    return dict(Counter(data["category"]))


def groupby_count_numpy(data):
    uniq, counts = np.unique(data["category"], return_counts=True)
    return dict(zip(uniq, counts))


def groupby_count_pandas(df):
    return df.groupby("category").size()


def groupby_count_pyarrow(table):
    return table.group_by("category").aggregate([("category", "count")])


def groupby_count_polars(df):
    return df.group_by("category").agg(pl.len().alias("count"))


async def groupby_count_aaiclick(obj):
    result = await obj.group_by("category").count()
    return await result.data()


# -- Group-by + multi-agg --

def groupby_multi_python(data):
    acc = defaultdict(list)
    for cat, amt in zip(data["category"], data["amount"]):
        acc[cat].append(amt)
    return {
        cat: {
            "sum": sum(vals),
            "mean": mean(vals),
            "min": min(vals),
            "max": max(vals),
        }
        for cat, vals in acc.items()
    }


def groupby_multi_numpy(data):
    cats = data["category"]
    amounts = data["amount"]
    uniq, inverse = np.unique(cats, return_inverse=True)
    n = len(uniq)
    sums = np.zeros(n)
    np.add.at(sums, inverse, amounts)
    counts = np.bincount(inverse, minlength=n).astype(float)
    means = sums / counts
    mins = np.full(n, np.inf)
    np.minimum.at(mins, inverse, amounts)
    maxs = np.full(n, -np.inf)
    np.maximum.at(maxs, inverse, amounts)
    return dict(zip(uniq, zip(sums, means, mins, maxs)))


def groupby_multi_pandas(df):
    return df.groupby("category")["amount"].agg(["sum", "mean", "min", "max"])


def groupby_multi_pyarrow(table):
    return table.group_by("category").aggregate([
        ("amount", "sum"),
        ("amount", "mean"),
        ("amount", "min"),
        ("amount", "max"),
    ])


def groupby_multi_polars(df):
    return df.group_by("category").agg(
        pl.col("amount").sum().alias("sum"),
        pl.col("amount").mean().alias("mean"),
        pl.col("amount").min().alias("min"),
        pl.col("amount").max().alias("max"),
    )


async def groupby_multi_aaiclick(obj):
    s = await obj.group_by("category").sum("amount")
    m = await obj.group_by("category").mean("amount")
    mn = await obj.group_by("category").min("amount")
    mx = await obj.group_by("category").max("amount")
    return (await s.data(), await m.data(), await mn.data(), await mx.data())


# -- Multi-key group-by --

def groupby_multikey_python(data):
    result = defaultdict(float)
    for cat, sub, amt in zip(data["category"], data["subcategory"], data["amount"]):
        result[(cat, sub)] += amt
    return dict(result)


def groupby_multikey_numpy(data):
    keys = np.array([f"{c}|{s}" for c, s in zip(data["category"], data["subcategory"])])
    amounts = data["amount"]
    uniq, inverse = np.unique(keys, return_inverse=True)
    sums = np.zeros(len(uniq))
    np.add.at(sums, inverse, amounts)
    return dict(zip(uniq, sums))


def groupby_multikey_pandas(df):
    return df.groupby(["category", "subcategory"])["amount"].sum()


def groupby_multikey_pyarrow(table):
    return table.group_by(["category", "subcategory"]).aggregate([("amount", "sum")])


def groupby_multikey_polars(df):
    return df.group_by(["category", "subcategory"]).agg(pl.col("amount").sum())


async def groupby_multikey_aaiclick(obj):
    result = await obj.group_by("category", "subcategory").sum("amount")
    return await result.data()


# -- High cardinality group-by (subcategory, ~1000 groups) --

def groupby_highcard_python(data):
    result = defaultdict(float)
    for sub, amt in zip(data["subcategory"], data["amount"]):
        result[sub] += amt
    return dict(result)


def groupby_highcard_numpy(data):
    subs = data["subcategory"]
    amounts = data["amount"]
    uniq, inverse = np.unique(subs, return_inverse=True)
    sums = np.zeros(len(uniq))
    np.add.at(sums, inverse, amounts)
    return dict(zip(uniq, sums))


def groupby_highcard_pandas(df):
    return df.groupby("subcategory")["amount"].sum()


def groupby_highcard_pyarrow(table):
    return table.group_by("subcategory").aggregate([("amount", "sum")])


def groupby_highcard_polars(df):
    return df.group_by("subcategory").agg(pl.col("amount").sum())


async def groupby_highcard_aaiclick(obj):
    result = await obj.group_by("subcategory").sum("amount")
    return await result.data()


# ---------------------------------------------------------------------------
# Benchmark registry
# ---------------------------------------------------------------------------

BENCHMARKS = {
    "Column sum": {
        "python": col_sum_python,
        "numpy": col_sum_numpy,
        "pandas": col_sum_pandas,
        "pyarrow": col_sum_pyarrow,
        "polars": col_sum_polars,
        "aaiclick": col_sum_aaiclick,
    },
    "Column multiply": {
        "python": col_mul_python,
        "numpy": col_mul_numpy,
        "pandas": col_mul_pandas,
        "pyarrow": col_mul_pyarrow,
        "polars": col_mul_polars,
        "aaiclick": col_mul_aaiclick,
    },
    "Filter rows": {
        "python": filter_python,
        "numpy": filter_numpy,
        "pandas": filter_pandas,
        "pyarrow": filter_pyarrow,
        "polars": filter_polars,
        "aaiclick": filter_aaiclick,
    },
    "Sort": {
        "python": sort_python,
        "numpy": sort_numpy,
        "pandas": sort_pandas,
        "pyarrow": sort_pyarrow,
        "polars": sort_polars,
        "aaiclick": sort_aaiclick,
    },
    "Count distinct": {
        "python": count_distinct_python,
        "numpy": count_distinct_numpy,
        "pandas": count_distinct_pandas,
        "pyarrow": count_distinct_pyarrow,
        "polars": count_distinct_polars,
        "aaiclick": count_distinct_aaiclick,
    },
    "Group-by sum": {
        "python": groupby_sum_python,
        "numpy": groupby_sum_numpy,
        "pandas": groupby_sum_pandas,
        "pyarrow": groupby_sum_pyarrow,
        "polars": groupby_sum_polars,
        "aaiclick": groupby_sum_aaiclick,
    },
    "Group-by count": {
        "python": groupby_count_python,
        "numpy": groupby_count_numpy,
        "pandas": groupby_count_pandas,
        "pyarrow": groupby_count_pyarrow,
        "polars": groupby_count_polars,
        "aaiclick": groupby_count_aaiclick,
    },
    "Group-by multi-agg": {
        "python": groupby_multi_python,
        "numpy": groupby_multi_numpy,
        "pandas": groupby_multi_pandas,
        "pyarrow": groupby_multi_pyarrow,
        "polars": groupby_multi_polars,
        "aaiclick": groupby_multi_aaiclick,
    },
    "Multi-key group-by": {
        "python": groupby_multikey_python,
        "numpy": groupby_multikey_numpy,
        "pandas": groupby_multikey_pandas,
        "pyarrow": groupby_multikey_pyarrow,
        "polars": groupby_multikey_polars,
        "aaiclick": groupby_multikey_aaiclick,
    },
    "High-card group-by": {
        "python": groupby_highcard_python,
        "numpy": groupby_highcard_numpy,
        "pandas": groupby_highcard_pandas,
        "pyarrow": groupby_highcard_pyarrow,
        "polars": groupby_highcard_polars,
        "aaiclick": groupby_highcard_aaiclick,
    },
}


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def measure_sync(fn, data, num_runs):
    """Run sync fn(data) num_runs+1 times (first is warmup). Return avg seconds and peak memory bytes."""
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
    """Run async fn(data) num_runs+1 times (first is warmup). Return avg seconds and peak memory bytes."""
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


async def run_benchmarks(raw_data, libraries):
    datasets = {}
    if "python" in libraries:
        datasets["python"] = raw_data
    if "numpy" in libraries:
        datasets["numpy"] = to_numpy(raw_data)
    if "pandas" in libraries:
        datasets["pandas"] = to_pandas(raw_data)
    if "pyarrow" in libraries:
        datasets["pyarrow"] = to_pyarrow(raw_data)
    if "polars" in libraries:
        datasets["polars"] = to_polars(raw_data)
    if "aaiclick" in libraries:
        datasets["aaiclick"] = await to_aaiclick(raw_data)

    results = {}
    for bench_name, funcs in BENCHMARKS.items():
        console.print(f"  {bench_name}...")
        results[bench_name] = {}
        for lib in libraries:
            if lib not in funcs:
                continue
            fn = funcs[lib]
            if asyncio.iscoroutinefunction(fn):
                avg_time, peak_mem = await measure_async(fn, datasets[lib], NUM_RUNS)
            else:
                avg_time, peak_mem = measure_sync(fn, datasets[lib], NUM_RUNS)
            results[bench_name][lib] = {"time": avg_time, "memory": peak_mem}

    return results


def fmt_time(seconds):
    """Format time with appropriate unit: s >= 2s, ms >= 2ms, us >= 2us, ns otherwise."""
    if seconds >= 2:
        return f"{seconds:.1f}s"
    if seconds >= 2e-3:
        return f"{seconds * 1e3:.1f}ms"
    if seconds >= 2e-6:
        return f"{seconds * 1e6:.1f}us"
    return f"{seconds * 1e9:.1f}ns"


def print_results(results, libraries, num_rows):
    table = Table(title=f"Data Library Benchmark — {num_rows:,} rows, {NUM_RUNS} runs")
    table.add_column("Operation", style="bold", no_wrap=True)
    for lib in libraries:
        table.add_column(lib, justify="right")
    table.add_column("Fastest", justify="right", style="green")

    for bench_name, lib_results in results.items():
        row = [bench_name]
        times = {}
        for lib in libraries:
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
    for lib in libraries:
        mem_table.add_column(lib, justify="right")

    for bench_name, lib_results in results.items():
        row = [bench_name]
        for lib in libraries:
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


async def main():
    console.print(f"\n[bold]Python Data Library Benchmark[/bold]")
    console.print(f"  Libraries: NumPy {np.__version__}, Pandas {pd.__version__}, "
                  f"PyArrow {pa.__version__}, Polars {pl.__version__}, "
                  f"aaiclick {aaiclick.__version__}\n")

    async with data_context():
        num_rows = 1_000_000
        console.print(f"[bold]{num_rows:,} rows, {NUM_RUNS} runs per operation[/bold]")
        raw_data = generate_raw_data(num_rows)
        results = await run_benchmarks(raw_data, ALL_LIBRARIES)
        print_results(results, ALL_LIBRARIES, num_rows)


if __name__ == "__main__":
    asyncio.run(main())
