"""Dask adapter — client-server topology. The thin client (this container)
connects to a dask-scheduler sidecar, which routes work to a dask-worker
sidecar. Worker is sized to match the previous in-process LocalCluster
(1 worker × 4 threads × 4 GiB) so per-op numbers stay comparable. Small-
result ops use .compute(); large-result ops pull a mid-range 10-row slice
via head(N, npartitions=-1) — Dask has no native offset, so we walk
partitions until SAMPLE_OFFSET+SAMPLE_LIMIT rows accumulate, then keep
the last SAMPLE_LIMIT rows."""

import os
from contextlib import contextmanager

import dask
import dask.dataframe as dd
from dask.distributed import Client

from .config import FILTER_THRESHOLD, SAMPLE_LIMIT, SAMPLE_OFFSET

VERSION = dask.__version__
IS_ASYNC = False

_SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")


@contextmanager
def context():
    client = Client(_SCHEDULER)
    try:
        yield client
    finally:
        client.close()


def convert(path):
    ddf = dd.read_parquet(path, split_row_groups=False).repartition(npartitions=8)
    ddf = ddf.categorize(columns=["category", "subcategory"])
    return ddf.persist()


def _sample(ddf):
    head = ddf.head(SAMPLE_OFFSET + SAMPLE_LIMIT, npartitions=-1, compute=True)
    return head.iloc[-SAMPLE_LIMIT:]


BENCHMARKS = {
    "Column sum":         lambda ddf: ddf["amount"].sum().compute(),
    "Column multiply":    lambda ddf: _sample((ddf["amount"] * ddf["quantity"]).to_frame()),
    "Filter rows":        lambda ddf: _sample(ddf[ddf["amount"] > FILTER_THRESHOLD]),
    "Sort":               lambda ddf: _sample(ddf.sort_values("amount", ascending=False)),
    "Count distinct":     lambda ddf: ddf["category"].nunique().compute(),
    "Group-by sum":       lambda ddf: ddf.groupby("category", observed=True)["amount"].sum().compute(),
    "Group-by count":     lambda ddf: ddf.groupby("category", observed=True).size().compute(),
    "Group-by multi-agg": lambda ddf: ddf.groupby("category", observed=True)["amount"].agg(["sum", "mean", "min", "max"]).compute(),
    "Multi-key group-by": lambda ddf: ddf.groupby(["category", "subcategory"], observed=True)["amount"].sum().compute(),
    "High-card group-by": lambda ddf: ddf.groupby("subcategory", observed=True)["amount"].sum().compute(),
}
