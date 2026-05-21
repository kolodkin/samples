"""aaiclick adapter — Ingest uses create_object_from_url so CH pulls the
parquet via url() (HTTP-only, hence the nginx fileserver sidecar)."""

import os

import aaiclick
from aaiclick import ColumnInfo
from aaiclick.data.data_context import data_context
from aaiclick.data.object import create_object_from_url
from aaiclick.data import Computed
from aaiclick.data.object.operators import Agg

from .config import FILTER_THRESHOLD, SAMPLE_LIMIT, SAMPLE_OFFSET

# Pin types so DESCRIBE-based inference doesn't widen ints.
COLUMNS = ["id", "category", "subcategory", "amount", "quantity"]
COLUMN_TYPES = {
    "id":          ColumnInfo("Int64"),
    "category":    ColumnInfo("String", low_cardinality=True),
    "subcategory": ColumnInfo("String", low_cardinality=True),
    "amount":      ColumnInfo("Float64"),
    "quantity":    ColumnInfo("Int64"),
}

VERSION = aaiclick.__version__
IS_ASYNC = True
ASYNC_CONTEXT = True

_FILESERVER = os.environ.get("FILESERVER_URL", "http://fileserver")


def context():
    return data_context()


async def convert(path):
    url = f"{_FILESERVER}/{os.path.basename(path)}"
    return await create_object_from_url(
        url, columns=COLUMNS, column_types=COLUMN_TYPES,
    )


async def _col_sum(obj):
    return await obj["amount"].sum()


async def _force(obj, *, select="*", where=None, order_by=None):
    """Force full server-side execution, discard the output — ClickHouse's
    `FORMAT Null`, the analog of Spark's noop sink and Dask's map_partitions(len).
    The operator/`.copy()` paths would instead CREATE a new MergeTree table
    holding the full result, charging these ops an extra ~10M-row write that
    the Spark/Dask materialize paths never pay."""
    sql = f"SELECT {select} FROM {obj.table}"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    await obj.ch_client.command(sql + " FORMAT Null")


async def _col_mul(obj):
    await _force(obj, select="amount * quantity")


async def _filter(obj):
    await _force(obj, where=f"amount > {FILTER_THRESHOLD}")


async def _sort(obj):
    await _force(obj, order_by="amount DESC")


async def _col_mul_page(obj):
    view = obj.with_columns(
        {"product": Computed("Float64", "amount * quantity")}
    ).view(limit=SAMPLE_LIMIT, offset=SAMPLE_OFFSET)
    return await view.data()


async def _filter_page(obj):
    view = obj.view(
        where=f"amount > {FILTER_THRESHOLD}",
        limit=SAMPLE_LIMIT,
        offset=SAMPLE_OFFSET,
    )
    return await view.data()


async def _sort_page(obj):
    view = obj.view(
        order_by="amount DESC",
        limit=SAMPLE_LIMIT,
        offset=SAMPLE_OFFSET,
    )
    return await view.data()


async def _count_distinct(obj):
    return await obj["category"].nunique()


async def _groupby_sum(obj):
    return await obj.group_by("category").sum("amount")


async def _groupby_count(obj):
    return await obj.group_by("category").count()


async def _groupby_multi(obj):
    return await obj.group_by("category").agg({
        "amount": [
            Agg("sum", "total"),
            Agg("mean", "average"),
            Agg("min", "minimum"),
            Agg("max", "maximum"),
        ],
    })


async def _groupby_multikey(obj):
    return await obj.group_by("category", "subcategory").sum("amount")


async def _groupby_highcard(obj):
    return await obj.group_by("subcategory").sum("amount")


BENCHMARKS = {
    "Column sum": _col_sum,
    "Column multiply": _col_mul,
    "Filter rows": _filter,
    "Sort": _sort,
    "Count distinct": _count_distinct,
    "Group-by sum": _groupby_sum,
    "Group-by count": _groupby_count,
    "Group-by multi-agg": _groupby_multi,
    "Multi-key group-by": _groupby_multikey,
    "High-card group-by": _groupby_highcard,
    "Column multiply page": _col_mul_page,
    "Filter rows page": _filter_page,
    "Sort page": _sort_page,
}
