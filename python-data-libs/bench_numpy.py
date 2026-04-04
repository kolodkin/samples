"""NumPy benchmark — dict of ndarrays. No for-loops."""

import numpy as np

from config import FILTER_THRESHOLD

NAME = "numpy"
VERSION = np.__version__


def convert(data):
    return {k: np.array(v) for k, v in data.items()}


def _groupby_sum(d, key_col="category"):
    uniq, inv = np.unique(d[key_col], return_inverse=True)
    sums = np.zeros(len(uniq))
    np.add.at(sums, inv, d["amount"])
    return dict(zip(uniq, sums))


def _groupby_multi(d):
    uniq, inv = np.unique(d["category"], return_inverse=True)
    n = len(uniq)
    sums = np.zeros(n)
    np.add.at(sums, inv, d["amount"])
    counts = np.bincount(inv, minlength=n).astype(float)
    mins = np.full(n, np.inf)
    np.minimum.at(mins, inv, d["amount"])
    maxs = np.full(n, -np.inf)
    np.maximum.at(maxs, inv, d["amount"])
    return dict(zip(uniq, zip(sums, sums / counts, mins, maxs)))


def _groupby_multikey(d):
    keys = np.char.add(np.char.add(d["category"], "|"), d["subcategory"])
    uniq, inv = np.unique(keys, return_inverse=True)
    sums = np.zeros(len(uniq))
    np.add.at(sums, inv, d["amount"])
    return dict(zip(uniq, sums))


BENCHMARKS = {
    "Column sum": lambda d: d["amount"].sum(),
    "Column multiply": lambda d: d["amount"] * d["quantity"],
    "Filter rows": lambda d: {k: v[d["amount"] > FILTER_THRESHOLD] for k, v in d.items()},
    "Sort": lambda d: {k: v[np.argsort(d["amount"])[::-1]] for k, v in d.items()},
    "Count distinct": lambda d: len(np.unique(d["category"])),
    "Group-by sum": lambda d: _groupby_sum(d),
    "Group-by count": lambda d: dict(zip(*np.unique(d["category"], return_counts=True))),
    "Group-by multi-agg": lambda d: _groupby_multi(d),
    "Multi-key group-by": lambda d: _groupby_multikey(d),
    "High-card group-by": lambda d: _groupby_sum(d, "subcategory"),
}
