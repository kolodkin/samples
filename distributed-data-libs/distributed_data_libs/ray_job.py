"""Ray Data benchmark, run inside ray-head via the Ray Jobs API.

See SPEC.md for why Ray uses a Jobs-API split rather than the default
in-container runner flow."""

import os
import sys

import ray
import ray.data as rd

from .config import FILTER_THRESHOLD
from . import runner

VERSION = ray.__version__

# Per-row `ds.map`/`ds.filter` lambdas are an order of magnitude slower
# than vectorized `ds.map_batches` - the per-block work goes from one
# Python call per row to a single numpy/pandas op per block.
BENCHMARKS = {
    "Column sum":         lambda ds: ds.sum("amount"),
    "Column multiply":    lambda ds: ds.map_batches(
        lambda b: {"p": b["amount"] * b["quantity"]},
        batch_format="numpy",
    ).materialize().count(),
    "Filter rows":        lambda ds: ds.map_batches(
        lambda b: b[b["amount"] > FILTER_THRESHOLD],
        batch_format="pandas",
    ).materialize().count(),
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


def convert(path):
    return rd.read_parquet(path, override_num_blocks=8).materialize()


def main():
    ray.init(address="auto", log_to_driver=False)
    rd.DataContext.get_current().execution_options.verbose_progress = False

    self_mod = sys.modules[__name__]
    results = runner._run_sync(self_mod)
    runner._write_results("ray", self_mod, results)


if __name__ == "__main__":
    main()
