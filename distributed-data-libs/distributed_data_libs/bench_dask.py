"""Dask adapter — LocalCluster inside the container. Lazy ops materialize
via .compute() (small results) or len(.compute()) (large results)."""

from contextlib import contextmanager

import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

from .config import FILTER_THRESHOLD

VERSION = dask.__version__
IS_ASYNC = False


@contextmanager
def context():
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=1,
        memory_limit="2GB",
        dashboard_address=None,
    )
    client = Client(cluster)
    try:
        yield client
    finally:
        client.close()
        cluster.close()


def convert(path):
    ddf = dd.read_parquet(path, split_row_groups=False).repartition(npartitions=8)
    ddf = ddf.categorize(columns=["category", "subcategory"])
    return ddf.persist()


def _materialize(ddf):
    """Trigger compute without collecting the full result to the driver."""
    return ddf.map_partitions(len).compute().sum()


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
}
