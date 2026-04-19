"""SQLite benchmark with covering indexes — stdlib sqlite3, in-memory.

Shares BENCHMARKS and VERSION with bench_sqlite; only convert() differs:
after bulk insert we build two covering indexes and ANALYZE so the planner
uses index-only scans for count-distinct and all group-by queries.
"""

from . import bench_sqlite

NAME = "sqlite+idx"
VERSION = bench_sqlite.VERSION


def convert(data):
    conn = bench_sqlite.convert(data)
    conn.execute("CREATE INDEX idx_cat_subcat_amount ON data(category, subcategory, amount)")
    conn.execute("CREATE INDEX idx_subcat_amount ON data(subcategory, amount)")
    conn.execute("ANALYZE")
    return conn


BENCHMARKS = bench_sqlite.BENCHMARKS
