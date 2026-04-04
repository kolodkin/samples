"""PyArrow benchmark — Arrow Table."""

import pyarrow as pa
import pyarrow.compute as pc

from config import FILTER_THRESHOLD

NAME = "pyarrow"
VERSION = pa.__version__


def convert(data):
    return pa.table(data)


BENCHMARKS = {
    "Column sum": lambda t: pc.sum(t.column("amount")).as_py(),
    "Column multiply": lambda t: pc.multiply(t.column("amount"), t.column("quantity")),
    "Filter rows": lambda t: t.filter(pc.greater(t.column("amount"), FILTER_THRESHOLD)),
    "Sort": lambda t: t.sort_by([("amount", "descending")]),
    "Count distinct": lambda t: pc.count_distinct(t.column("category")).as_py(),
    "Group-by sum": lambda t: t.group_by("category").aggregate([("amount", "sum")]),
    "Group-by count": lambda t: t.group_by("category").aggregate([("category", "count")]),
    "Group-by multi-agg": lambda t: t.group_by("category").aggregate([
        ("amount", "sum"), ("amount", "mean"), ("amount", "min"), ("amount", "max"),
    ]),
    "Multi-key group-by": lambda t: t.group_by(["category", "subcategory"]).aggregate([("amount", "sum")]),
    "High-card group-by": lambda t: t.group_by("subcategory").aggregate([("amount", "sum")]),
}
