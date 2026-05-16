"""aaiclick adapter — connects to a ClickHouse server sidecar (CLICKHOUSE_HOST
env var) instead of running chdb embedded. Ingest uses create_from_url so the
CH server reads parquet directly via its native vectorized reader, skipping
the Python dict round-trip that dominated embedded-mode Ingest time."""

import aaiclick
from aaiclick import ColumnInfo, Schema, create_from_url
from aaiclick.data.data_context import data_context
from aaiclick.data.object.operators import Agg

from .config import FILTER_THRESHOLD

_SCHEMA = Schema(
    fieldtype="d",
    columns={
        "aai_id": ColumnInfo("UInt64"),
        "id": ColumnInfo("Int64"),
        "category": ColumnInfo("String", low_cardinality=True),
        "subcategory": ColumnInfo("String", low_cardinality=True),
        "amount": ColumnInfo("Float64"),
        "quantity": ColumnInfo("Int64"),
    },
)

VERSION = aaiclick.__version__
IS_ASYNC = True
ASYNC_CONTEXT = True


def context():
    return data_context()


async def convert(path):
    # CH server reads the parquet file directly from the shared /data volume.
    # The CH container mounts /data so file:// URLs resolve server-side.
    return await create_from_url(f"file://{path}", schema=_SCHEMA)


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
