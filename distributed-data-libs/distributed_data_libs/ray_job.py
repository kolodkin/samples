"""Runs INSIDE the ray-head container, launched by ray_runner via Ray
Jobs API. The runner can't drive Ray Data over the network (Ray Client
breaks Ray Data; direct ray.init fails to resolve node info; ray start
+ ray.init(auto) hangs on the first distributed task). So we ship the
benchmark to ray-head as a job, measure ray-head's own cgroup, and write
/data/results-ray.json from this process."""

import json
import os
import time

import ray
import ray.data as rd

from .config import (
    BENCH_NAMES,
    FILTER_THRESHOLD,
    INGEST,
    NUM_RUNS,
    RAW_DATA_PATH,
    RESULTS_DIR,
)
from .stats import (
    cgroup_v2_available,
    resolve_measurement_targets,
    sum_cpu_usec,
    sum_memory_peak,
)


def _ops():
    return {
        "Column sum":         lambda ds: ds.sum("amount"),
        "Column multiply":    lambda ds: ds.map(lambda r: {"p": r["amount"] * r["quantity"]}).materialize().count(),
        "Filter rows":        lambda ds: ds.filter(lambda r: r["amount"] > FILTER_THRESHOLD).materialize().count(),
        "Sort":               lambda ds: ds.sort("amount", descending=True).materialize().count(),
        "Count distinct":     lambda ds: ds.unique("category"),
        "Group-by sum":       lambda ds: ds.groupby("category").sum("amount").take_all(),
        "Group-by count":     lambda ds: ds.groupby("category").count().take_all(),
        "Group-by multi-agg": lambda ds: ds.groupby("category").aggregate(
            rd.aggregate.Sum("amount"),
            rd.aggregate.Mean("amount"),
            rd.aggregate.Min("amount"),
            rd.aggregate.Max("amount"),
        ).take_all(),
        "Multi-key group-by": lambda ds: ds.groupby(["category", "subcategory"]).sum("amount").take_all(),
        "High-card group-by": lambda ds: ds.groupby("subcategory").sum("amount").take_all(),
    }


def main():
    ray.init(address="auto", log_to_driver=False)
    rd.DataContext.get_current().execution_options.verbose_progress = False

    if not cgroup_v2_available():
        raise RuntimeError("cgroup v2 not available")
    targets = resolve_measurement_targets()
    print(f"[ray-job] measuring cgroups: {targets}", flush=True)

    def before():
        return sum_memory_peak(targets), sum_cpu_usec(targets), time.perf_counter()

    def after(state):
        peak_b, cpu_b, t0 = state
        return {
            "time": (time.perf_counter() - t0) / NUM_RUNS,
            "peak_mem": max(0, sum_memory_peak(targets) - peak_b),
            "cpu_usec": sum_cpu_usec(targets) - cpu_b,
        }

    results = {}

    state = before()
    for _ in range(NUM_RUNS):
        rd.read_parquet(RAW_DATA_PATH, override_num_blocks=8).materialize()
    results[INGEST] = after(state)

    dataset = rd.read_parquet(RAW_DATA_PATH, override_num_blocks=8).materialize()
    ops = _ops()
    for op_name in BENCH_NAMES:
        if op_name == INGEST or op_name not in ops:
            continue
        print(f"  {op_name}...", flush=True)
        fn = ops[op_name]
        state = before()
        for _ in range(NUM_RUNS):
            fn(dataset)
        results[op_name] = after(state)

    out = os.path.join(RESULTS_DIR, "results-ray.json")
    with open(out, "w") as f:
        json.dump({
            "framework": "ray",
            "version": ray.__version__,
            "results": results,
        }, f, indent=2)
    print(f"[ray-job] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
