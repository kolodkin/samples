"""Pandas benchmark — DataFrame."""

import pandas as pd

from config import FILTER_THRESHOLD

NAME = "pandas"
VERSION = pd.__version__


def convert(data):
    return pd.DataFrame(data)


BENCHMARKS = {
    "Column sum": lambda df: df["amount"].sum(),
    "Column multiply": lambda df: df["amount"] * df["quantity"],
    "Filter rows": lambda df: df[df["amount"] > FILTER_THRESHOLD],
    "Sort": lambda df: df.sort_values("amount", ascending=False),
    "Count distinct": lambda df: df["category"].nunique(),
    "Group-by sum": lambda df: df.groupby("category")["amount"].sum(),
    "Group-by count": lambda df: df.groupby("category").size(),
    "Group-by multi-agg": lambda df: df.groupby("category")["amount"].agg(["sum", "mean", "min", "max"]),
    "Multi-key group-by": lambda df: df.groupby(["category", "subcategory"])["amount"].sum(),
    "High-card group-by": lambda df: df.groupby("subcategory")["amount"].sum(),
}
