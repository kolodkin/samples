"""Ray Data benchmark, run inside ray-head via the Ray Jobs API (see SPEC.md)."""

import os
import sys

import ray
import ray.data as rd

from .config import FILTER_THRESHOLD, SAMPLE_LIMIT, SAMPLE_OFFSET
from . import runner

VERSION = ray.__version__


# Stream batches and stop at offset — .limit(N).take(N) hung the
# LimitOperator's cross-block coordination at N=5M.
def _sample(ds):
    seen = 0
    for batch in ds.iter_batches(batch_format="pandas", batch_size=10_000):
        n = len(batch)
        if seen + n > SAMPLE_OFFSET:
            start = SAMPLE_OFFSET - seen
            return batch.iloc[start:start + SAMPLE_LIMIT]
        seen += n
    return None


# map_batches over per-row map/filter — ~30x on 10M rows.
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
    "Column multiply page": lambda ds: _sample(ds.map_batches(
        lambda b: {"p": b["amount"] * b["quantity"]},
        batch_format="numpy",
    )),
    "Filter rows page":     lambda ds: _sample(ds.map_batches(
        lambda b: b[b["amount"] > FILTER_THRESHOLD],
        batch_format="pandas",
    )),
    "Sort page":            lambda ds: _sample(ds.sort("amount", descending=True)),
}


def convert(path):
    return rd.read_parquet(path, override_num_blocks=8).materialize()


def main():
    ray.init(address="auto", log_to_driver=False)
    ctx = rd.DataContext.get_current()
    ctx.execution_options.verbose_progress = False
    ctx.execution_options.preserve_order = True  # for _sample's offset semantics

    self_mod = sys.modules[__name__]
    results = runner._run_sync(self_mod)
    runner._write_results("ray", self_mod, results)


if __name__ == "__main__":
    main()
