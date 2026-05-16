"""Postgres adapter — raw psycopg client against a postgres sidecar
(PGHOST env var). Ingest streams parquet to the server via COPY FROM STDIN
in CSV format — the standard Postgres bulk-load path."""

from contextlib import contextmanager

import psycopg

from .config import FILTER_THRESHOLD

VERSION = psycopg.__version__
IS_ASYNC = False

_TABLE = "bench"
_SINK = "bench_sink"

_CREATE_SQL = f"""
DROP TABLE IF EXISTS {_TABLE};
CREATE UNLOGGED TABLE {_TABLE} (
    id          BIGINT,
    category    TEXT,
    subcategory TEXT,
    amount      DOUBLE PRECISION,
    quantity    BIGINT
);
"""

# Stashed by context() so convert() and the BENCHMARKS lambdas can reach the
# live connection without changing the runner contract (convert(path) -> dataset).
_CONN = {"c": None}


@contextmanager
def context():
    # psycopg reads PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE from env.
    # autocommit=True so DDL doesn't get held open while we time queries.
    with psycopg.connect(autocommit=True) as conn:
        _CONN["c"] = conn
        try:
            yield conn
        finally:
            _CONN["c"] = None


def convert(path):
    import io
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    conn = _CONN["c"]
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)

    # PyArrow's CSV writer is ~10x faster than pandas.to_csv at 10M rows
    # and uses constant memory.
    table = pq.read_table(path)
    buf = io.BytesIO()
    pcsv.write_csv(
        table, buf,
        write_options=pcsv.WriteOptions(include_header=False),
    )
    buf.seek(0)

    with conn.cursor() as cur, cur.copy(
        f"COPY {_TABLE} FROM STDIN WITH (FORMAT CSV)"
    ) as cp:
        cp.write(buf.read())

    with conn.cursor() as cur:
        cur.execute(f"ANALYZE {_TABLE}")
    return conn


def _exec_one(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None


def _materialize(conn, sql):
    """Wrap a large-result query in a temp sink table so we measure compute
    not fetch (matches the chdb/duckdb pattern in python-data-libs)."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {_SINK} AS {sql}")
        cur.execute(f"DROP TABLE {_SINK}")


BENCHMARKS = {
    "Column sum":         lambda c: _exec_one(c, f"SELECT sum(amount) FROM {_TABLE}"),
    "Column multiply":    lambda c: _materialize(c, f"SELECT amount * quantity AS p FROM {_TABLE}"),
    "Filter rows":        lambda c: _materialize(c, f"SELECT * FROM {_TABLE} WHERE amount > {FILTER_THRESHOLD}"),
    "Sort":               lambda c: _materialize(c, f"SELECT * FROM {_TABLE} ORDER BY amount DESC"),
    "Count distinct":     lambda c: _exec_one(c, f"SELECT count(DISTINCT category) FROM {_TABLE}"),
    "Group-by sum":       lambda c: _exec_one(c, f"SELECT category, sum(amount) FROM {_TABLE} GROUP BY category"),
    "Group-by count":     lambda c: _exec_one(c, f"SELECT category, count(*) FROM {_TABLE} GROUP BY category"),
    "Group-by multi-agg": lambda c: _exec_one(
        c, f"SELECT category, sum(amount), avg(amount), min(amount), max(amount) FROM {_TABLE} GROUP BY category"
    ),
    "Multi-key group-by": lambda c: _exec_one(
        c, f"SELECT category, subcategory, sum(amount) FROM {_TABLE} GROUP BY category, subcategory"
    ),
    "High-card group-by": lambda c: _exec_one(c, f"SELECT subcategory, sum(amount) FROM {_TABLE} GROUP BY subcategory"),
}
