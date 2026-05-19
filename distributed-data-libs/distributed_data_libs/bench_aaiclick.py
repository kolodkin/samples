"""aaiclick adapter — distributed mode against ClickHouse + Postgres sidecars.

Ingest uses create_object_from_url, which makes ClickHouse pull the parquet
itself via its url() table function and parse it natively (no Python dict
round-trip — that was the 60s tax dominating embedded-chdb Ingest). url()
requires HTTP, so a tiny nginx sidecar (`fileserver`) serves /data/.
"""

import os

import aaiclick
from aaiclick import ColumnInfo
from aaiclick.data.data_context import data_context
from aaiclick.data.object import create_object_from_url
from aaiclick.data.object.operators import Agg

from .config import FILTER_THRESHOLD

# Columns to read from the parquet via CH's url() table function. We also
# pin types so CH's DESCRIBE-based inference doesn't pick wider integers
# (Int64 from parquet metadata is fine; explicit LowCardinality on the
# string columns mirrors the python-data-libs adapter).
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

# Fileserver sidecar that mounts /data on port 80 (see docker-compose.yml).
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


async def _col_mul(obj):
    return await (obj["amount"] * obj["quantity"])


async def _filter(obj):
    return await obj.where(f"amount > {FILTER_THRESHOLD}").copy()


async def _sort(obj):
    return await obj.view(order_by="amount DESC").copy()


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
}
