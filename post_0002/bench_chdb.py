"""chdb benchmark — embedded ClickHouse, SQL queries, in-memory."""

import chdb
from chdb.session import Session

from .config import FILTER_THRESHOLD

NAME = "chdb"
VERSION = chdb.__version__


def convert(data):
    session = Session()
    session.query("CREATE DATABASE IF NOT EXISTS bench ENGINE = Atomic")
    session.query("DROP TABLE IF EXISTS bench.data")
    session.query("""
        CREATE TABLE bench.data (
            id Int64,
            category LowCardinality(String),
            subcategory LowCardinality(String),
            amount Float64,
            quantity Int64
        ) ENGINE = MergeTree() ORDER BY id
    """)
    num_rows = len(data["id"])
    session.query(f"""
        INSERT INTO bench.data
        SELECT
            number AS id,
            concat('cat_', toString(number % 10)) AS category,
            concat('sub_', toString(number % 1000)) AS subcategory,
            rand64() % 1000000 / 1000.0 AS amount,
            rand64() % 100 + 1 AS quantity
        FROM numbers({num_rows})
    """)
    return session


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
