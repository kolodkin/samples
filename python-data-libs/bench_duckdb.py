"""DuckDB benchmark — embedded analytical database, SQL queries, in-memory."""

import duckdb

from config import FILTER_THRESHOLD

NAME = "duckdb"
VERSION = duckdb.__version__


def convert(data):
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE data (
            id BIGINT,
            category VARCHAR,
            subcategory VARCHAR,
            amount DOUBLE,
            quantity BIGINT
        )
    """)
    conn.execute(
        "INSERT INTO data SELECT * FROM ("
        "SELECT UNNEST($1) AS id, UNNEST($2) AS category, "
        "UNNEST($3) AS subcategory, UNNEST($4) AS amount, UNNEST($5) AS quantity"
        ")",
        [data["id"], data["category"], data["subcategory"], data["amount"], data["quantity"]],
    )
    return conn


BENCHMARKS = {
    "Column sum": lambda c: c.execute("SELECT sum(amount) FROM data").fetchone(),
    "Column multiply": lambda c: c.execute("SELECT amount * quantity FROM data").fetchall(),
    "Filter rows": lambda c: c.execute(f"SELECT * FROM data WHERE amount > {FILTER_THRESHOLD}").fetchall(),
    "Sort": lambda c: c.execute("SELECT * FROM data ORDER BY amount DESC").fetchall(),
    "Count distinct": lambda c: c.execute("SELECT count(DISTINCT category) FROM data").fetchone(),
    "Group-by sum": lambda c: c.execute("SELECT category, sum(amount) FROM data GROUP BY category").fetchall(),
    "Group-by count": lambda c: c.execute("SELECT category, count(*) FROM data GROUP BY category").fetchall(),
    "Group-by multi-agg": lambda c: c.execute(
        "SELECT category, sum(amount), avg(amount), min(amount), max(amount) FROM data GROUP BY category"
    ).fetchall(),
    "Multi-key group-by": lambda c: c.execute(
        "SELECT category, subcategory, sum(amount) FROM data GROUP BY category, subcategory"
    ).fetchall(),
    "High-card group-by": lambda c: c.execute(
        "SELECT subcategory, sum(amount) FROM data GROUP BY subcategory"
    ).fetchall(),
}
