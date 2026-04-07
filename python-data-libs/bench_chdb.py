"""chdb benchmark — embedded ClickHouse, SQL queries, in-memory.

Data is loaded from the same Python dict as all other libraries via
PyArrow zero-copy: Python(arrow_table) table function.
"""

from contextlib import contextmanager

import chdb
import pyarrow as pa
from chdb.session import Session

from config import FILTER_THRESHOLD

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
        ) ENGINE = Memory
    """)
    _session.query("DROP TABLE IF EXISTS bench.sink")
    _session.query("""
        CREATE TABLE bench.sink (
            id Int64,
            category LowCardinality(String),
            subcategory LowCardinality(String),
            amount Float64,
            quantity Int64
        ) ENGINE = Memory
    """)
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    _session.query("INSERT INTO bench.data SELECT * FROM Python(arrow_table)")
    return _session


def _materialize(s, sql):
    s.query("TRUNCATE TABLE bench.sink")
    s.query(f"INSERT INTO bench.sink {sql}")


BENCHMARKS = {
    "Column sum": lambda s: s.query("SELECT sum(amount) FROM bench.data"),
    "Column multiply": lambda s: s.query("SELECT id, category, subcategory, amount * quantity AS amount, quantity FROM bench.data FORMAT Null"),
    "Filter rows": lambda s: _materialize(s, f"SELECT * FROM bench.data WHERE amount > {FILTER_THRESHOLD}"),
    "Sort": lambda s: _materialize(s, "SELECT * FROM bench.data ORDER BY amount DESC"),
    "Count distinct": lambda s: s.query("SELECT count() FROM (SELECT category FROM bench.data GROUP BY category)"),
    "Group-by sum": lambda s: s.query(
        "SELECT category, sum(amount) FROM bench.data GROUP BY category"
    ),
    "Group-by count": lambda s: s.query(
        "SELECT category, count() FROM bench.data GROUP BY category"
    ),
    "Group-by multi-agg": lambda s: s.query(
        "SELECT category, sum(amount), avg(amount), min(amount), max(amount) FROM bench.data GROUP BY category"
    ),
    "Multi-key group-by": lambda s: s.query(
        "SELECT category, subcategory, sum(amount) FROM bench.data GROUP BY category, subcategory"
    ),
    "High-card group-by": lambda s: s.query(
        "SELECT subcategory, sum(amount) FROM bench.data GROUP BY subcategory"
    ),
}
