"""aaiclick benchmark — Python-to-ClickHouse compiler. Compute only, no .data()."""

import aaiclick
from aaiclick import create_object_from_value

from .config import FILTER_THRESHOLD

from aaiclick.data.data_context import data_context

NAME = "aaiclick"
VERSION = aaiclick.__version__
IS_ASYNC = True


def context():
    return data_context()


async def convert(data):
    return await create_object_from_value(data)


async def _col_sum(obj):
    return await obj["amount"].sum()


async def _col_mul(obj):
    return await (obj["amount"] * obj["quantity"])


async def _filter(obj):
    view = obj.where(f"amount > {FILTER_THRESHOLD}")
    return await view.copy()


async def _sort(obj):
    view = obj.view(order_by="amount DESC")
    return await view.copy()


async def _count_distinct(obj):
    uniq = await obj["category"].unique()
    return await uniq.count()


async def _groupby_sum(obj):
    return await obj.group_by("category").sum("amount")


async def _groupby_count(obj):
    return await obj.group_by("category").count()


async def _groupby_multi(obj):
    return await obj.group_by("category").agg({
        "amount": ["sum", "mean", "min", "max"],
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
