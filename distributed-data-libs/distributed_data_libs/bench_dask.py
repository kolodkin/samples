"""Dask adapter — thin client → dask-scheduler → dask-worker (1×4 threads×4 GiB)."""

import os
from contextlib import contextmanager

import dask
import dask.dataframe as dd
from dask.distributed import Client, wait

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
    # split_row_groups=True reads the parquet's row groups in parallel (the
    # 10M-row file has 10), so we skip the single-partition read + reshuffle
    # that split_row_groups=False + repartition(8) forced.
    ddf = dd.read_parquet(path, split_row_groups=True)
    ddf = ddf.categorize(columns=["category", "subcategory"])
    return ddf.persist()


def _materialize(ddf):
    # Land the full result in worker memory (matches aaiclick keeping its
    # result in-engine), then release so each of the NUM_RUNS iterations starts
    # clean instead of stacking persisted copies.
    persisted = ddf.persist()
    wait(persisted)
    del persisted


def _discard(ddf):
    # Reduce to a per-partition row count and sum it — forces full server-side
    # compute without landing the result in worker memory or shipping rows to
    # the driver. The compute-only counterpart to _materialize / aaiclick's
    # Object.execute().
    ddf.map_partitions(len).compute().sum()


def _sample(ddf):
    head = ddf.head(SAMPLE_OFFSET + SAMPLE_LIMIT, npartitions=-1, compute=True)
    return head.iloc[-SAMPLE_LIMIT:]


BENCHMARKS = {
    "Column sum":         lambda ddf: ddf["amount"].sum().compute(),
    "Column multiply":    lambda ddf: _materialize((ddf["amount"] * ddf["quantity"]).to_frame()),
    "Filter rows":        lambda ddf: _materialize(ddf[ddf["amount"] > FILTER_THRESHOLD]),
    "Sort":               lambda ddf: _materialize(ddf.sort_values("amount", ascending=False)),
    "Count distinct":     lambda ddf: ddf["category"].nunique().compute(),
    "Group-by sum":       lambda ddf: ddf.groupby("category", observed=True)["amount"].sum().compute(),
    "Group-by count":     lambda ddf: ddf.groupby("category", observed=True).size().compute(),
    "Group-by multi-agg": lambda ddf: ddf.groupby("category", observed=True)["amount"].agg(["sum", "mean", "min", "max"]).compute(),
    "Multi-key group-by": lambda ddf: ddf.groupby(["category", "subcategory"], observed=True)["amount"].sum().compute(),
    "High-card group-by": lambda ddf: ddf.groupby("subcategory", observed=True)["amount"].sum().compute(),
    "Column multiply page": lambda ddf: _sample((ddf["amount"] * ddf["quantity"]).to_frame()),
    "Filter rows page":     lambda ddf: _sample(ddf[ddf["amount"] > FILTER_THRESHOLD]),
    "Sort page":            lambda ddf: _sample(ddf.sort_values("amount", ascending=False)),
    "Column multiply execute": lambda ddf: _discard((ddf["amount"] * ddf["quantity"]).to_frame()),
    "Filter rows execute":     lambda ddf: _discard(ddf[ddf["amount"] > FILTER_THRESHOLD]),
    "Sort execute":            lambda ddf: _discard(ddf.sort_values("amount", ascending=False)),
}
