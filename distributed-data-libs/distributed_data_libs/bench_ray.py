"""Ray Data adapter — ray.init() local mode inside the container. Lazy ops
materialize via .materialize() (which lands blocks in the object store)."""

from contextlib import contextmanager

import ray
import ray.data as rd

from .config import FILTER_THRESHOLD

VERSION = ray.__version__
IS_ASYNC = False


@contextmanager
def context():
    ray.init(num_cpus=4, include_dashboard=False, log_to_driver=False)
    rd.DataContext.get_current().execution_options.verbose_progress = False
    try:
        yield None
    finally:
        ray.shutdown()


def convert(data):
    rows = [
        {"id": i, "category": c, "subcategory": s, "amount": a, "quantity": q}
        for i, c, s, a, q in zip(
            data["id"], data["category"], data["subcategory"],
            data["amount"], data["quantity"],
        )
    ]
    return rd.from_items(rows, override_num_blocks=8).materialize()


def _materialize(ds):
    return ds.materialize().count()


BENCHMARKS = {
    "Column sum":         lambda ds: ds.sum("amount"),
    "Column multiply":    lambda ds: _materialize(
        ds.map(lambda r: {"p": r["amount"] * r["quantity"]})
    ),
    "Filter rows":        lambda ds: _materialize(
        ds.filter(lambda r: r["amount"] > FILTER_THRESHOLD)
    ),
    "Sort":               lambda ds: _materialize(ds.sort("amount", descending=True)),
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
