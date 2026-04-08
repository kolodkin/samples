"""Native Python benchmark — columnar dict[str, list]."""

import sys
from collections import Counter, defaultdict
from statistics import mean

from .config import FILTER_THRESHOLD

NAME = "python"
VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"


def convert(data):
    return data


BENCHMARKS = {
    "Column sum": lambda d: sum(d["amount"]),

    "Column multiply": lambda d: [a * q for a, q in zip(d["amount"], d["quantity"])],

    "Filter rows": lambda d: {
        k: [v for v, a in zip(d[k], d["amount"]) if a > FILTER_THRESHOLD]
        for k in d
    },

    "Sort": lambda d: (
        lambda idx: {k: [d[k][i] for i in idx] for k in d}
    )(sorted(range(len(d["amount"])), key=lambda i: d["amount"][i], reverse=True)),

    "Count distinct": lambda d: len(set(d["category"])),

    "Group-by sum": lambda d: dict(
        (r := defaultdict(float),
         [r.__setitem__(c, r[c] + a) for c, a in zip(d["category"], d["amount"])],
         r)[-1]
    ) if False else _groupby_sum(d),

    "Group-by count": lambda d: dict(Counter(d["category"])),

    "Group-by multi-agg": lambda d: _groupby_multi(d),

    "Multi-key group-by": lambda d: _groupby_multikey(d),

    "High-card group-by": lambda d: _groupby_highcard(d),
}


def _groupby_sum(data):
    result = defaultdict(float)
    for cat, amt in zip(data["category"], data["amount"]):
        result[cat] += amt
    return dict(result)


def _groupby_multi(data):
    acc = defaultdict(list)
    for cat, amt in zip(data["category"], data["amount"]):
        acc[cat].append(amt)
    return {
        cat: (sum(v), mean(v), min(v), max(v))
        for cat, v in acc.items()
    }


def _groupby_multikey(data):
    result = defaultdict(float)
    for cat, sub, amt in zip(data["category"], data["subcategory"], data["amount"]):
        result[(cat, sub)] += amt
    return dict(result)


def _groupby_highcard(data):
    result = defaultdict(float)
    for sub, amt in zip(data["subcategory"], data["amount"]):
        result[sub] += amt
    return dict(result)
