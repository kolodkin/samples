"""chdb benchmark — embedded ClickHouse, SQL queries, in-memory.

Data is loaded from the same Python dict as all other libraries,
serialized via CSV (chdb has no direct Python insert API).
"""

from contextlib import contextmanager

import chdb
from chdb.session import Session

from .config import FILTER_THRESHOLD

NAME = "chdb"
VERSION = chdb.__version__

_session = None


@contextmanager
def context():
    global _session
    _session = Session()
    try:
        yield
    finally:
        _session.cleanup()
        _session.close()
        _session = None


def convert(data):
    _session.query("CREATE DATABASE IF NOT EXISTS bench ENGINE = Atomic")
    _session.query("DROP TABLE IF EXISTS bench.data")
    _session.query("""
        CREATE TABLE bench.data (
            id Int64,
            category LowCardinality(String),
            subcategory LowCardinality(String),
            amount Float64,
            quantity Int64
        ) ENGINE = MergeTree() ORDER BY id
    """)
    csv = "\n".join(
        f'{i},"{c}","{s}",{a},{q}'
        for i, c, s, a, q in zip(
            data["id"], data["category"], data["subcategory"],
            data["amount"], data["quantity"],
        )
    )
    _session.query(f"INSERT INTO bench.data FORMAT CSV\n{csv}")
    return _session


BENCHMARKS = {
    "Column sum": lambda s: s.query("SELECT sum(amount) FROM bench.data"),
    "Column multiply": lambda s: s.query("SELECT amount * quantity FROM bench.data FORMAT Null"),
    "Filter rows": lambda s: s.query(f"SELECT * FROM bench.data WHERE amount > {FILTER_THRESHOLD} FORMAT Null"),
    "Sort": lambda s: s.query("SELECT * FROM bench.data ORDER BY amount DESC FORMAT Null"),
    "Count distinct": lambda s: s.query("SELECT count(DISTINCT category) FROM bench.data"),
    "Group-by sum": lambda s: s.query("SELECT category, sum(amount) FROM bench.data GROUP BY category"),
    "Group-by count": lambda s: s.query("SELECT category, count() FROM bench.data GROUP BY category"),
    "Group-by multi-agg": lambda s: s.query(
        "SELECT category, sum(amount), avg(amount), min(amount), max(amount) FROM bench.data GROUP BY category"
    ),
    "Multi-key group-by": lambda s: s.query(
        "SELECT category, subcategory, sum(amount) FROM bench.data GROUP BY category, subcategory"
    ),
    "High-card group-by": lambda s: s.query("SELECT subcategory, sum(amount) FROM bench.data GROUP BY subcategory"),
}
