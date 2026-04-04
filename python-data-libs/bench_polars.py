"""Polars benchmark — Polars DataFrame."""

import polars as pl

from config import FILTER_THRESHOLD

NAME = "polars"
VERSION = pl.__version__


def convert(data):
    return pl.DataFrame(data)


BENCHMARKS = {
    "Column sum": lambda df: df["amount"].sum(),
    "Column multiply": lambda df: df["amount"] * df["quantity"],
    "Filter rows": lambda df: df.filter(pl.col("amount") > FILTER_THRESHOLD),
    "Sort": lambda df: df.sort("amount", descending=True),
    "Count distinct": lambda df: df["category"].n_unique(),
    "Group-by sum": lambda df: df.group_by("category").agg(pl.col("amount").sum()),
    "Group-by count": lambda df: df.group_by("category").agg(pl.len().alias("count")),
    "Group-by multi-agg": lambda df: df.group_by("category").agg(
        pl.col("amount").sum().alias("sum"),
        pl.col("amount").mean().alias("mean"),
        pl.col("amount").min().alias("min"),
        pl.col("amount").max().alias("max"),
    ),
    "Multi-key group-by": lambda df: df.group_by(["category", "subcategory"]).agg(pl.col("amount").sum()),
    "High-card group-by": lambda df: df.group_by("subcategory").agg(pl.col("amount").sum()),
}
