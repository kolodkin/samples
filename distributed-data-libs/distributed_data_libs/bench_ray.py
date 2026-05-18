"""Ray Data adapter — client-server topology. The runner first joins the
cluster as a 0-CPU node via `ray start --address=ray-head:6379` (run as
part of the container command before python launches), then ray.init()
attaches to that local node. We can't use ray:// (Ray Client) because it
breaks Ray Data, and we can't use ray.init(address="<head>:<port>")
directly from a fresh container because Ray can't resolve its own node
info without a prior `ray start`. Lazy ops materialize via .materialize()."""

import os
from contextlib import contextmanager

import ray
import ray.data as rd

from .config import FILTER_THRESHOLD

VERSION = ray.__version__
IS_ASYNC = False

# "auto" means: attach to the local Ray node that the container's
# entrypoint joined to the cluster. The runner registers with
# --num-cpus=0 so all tasks go to ray-head (which has 4 CPUs).
_RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")


@contextmanager
def context():
    ray.init(address=_RAY_ADDRESS, log_to_driver=False)
    rd.DataContext.get_current().execution_options.verbose_progress = False
    try:
        yield None
    finally:
        ray.shutdown()


def convert(path):
    return rd.read_parquet(path, override_num_blocks=8).materialize()


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
