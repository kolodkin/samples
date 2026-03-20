#!/usr/bin/env python3
"""Compare ClickHouse String vs LowCardinality(String) storage and query performance."""

import os
import clickhouse_connect
from rich.console import Console
from rich.table import Table


NUM_ROWS = 10_000_000

console = Console()


def elapsed_s(result):
    """Extract server-side elapsed seconds from a query result."""
    return int(result.summary.get("elapsed_ns", 0)) / 1e9


def main():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    # Cleanup
    client.command("DROP TABLE IF EXISTS events_string")
    client.command("DROP TABLE IF EXISTS events_lc")

    # --- Create tables ---
    console.print("Creating tables...")

    client.command("""
        CREATE TABLE events_string (
            id UInt64,
            status String,
            created_at DateTime
        ) ENGINE = MergeTree() ORDER BY id
    """)

    client.command("""
        CREATE TABLE events_lc (
            id UInt64,
            status LowCardinality(String),
            created_at DateTime
        ) ENGINE = MergeTree() ORDER BY id
    """)

    # --- Insert rows ---
    console.print(f"Inserting {NUM_ROWS:,} rows...")

    r = client.query(f"""
        INSERT INTO events_string
        SELECT number,
            concat('status_', toString(number % 30)),
            now()
        FROM numbers({NUM_ROWS})
    """)
    t_insert_string = elapsed_s(r)

    r = client.query(f"""
        INSERT INTO events_lc
        SELECT number,
            concat('status_', toString(number % 30)),
            now()
        FROM numbers({NUM_ROWS})
    """)
    t_insert_lc = elapsed_s(r)

    # --- Storage ---
    console.print("Measuring storage...")
    storage = client.query("""
        SELECT
            table,
            formatReadableSize(sum(data_compressed_bytes)) AS compressed,
            formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
        FROM system.parts
        WHERE table IN ('events_string', 'events_lc') AND active
        GROUP BY table
        ORDER BY table
    """)
    storage_by_table = {row[0]: (row[1], row[2]) for row in storage.result_rows}

    # --- Query performance ---
    console.print("Running queries...")
    bench_queries = [
        ("COUNT with filter", "SELECT count() FROM {table} WHERE status = 'status_0'"),
        ("GROUP BY",          "SELECT status, count() FROM {table} GROUP BY status"),
        ("DISTINCT",          "SELECT DISTINCT status FROM {table}"),
    ]

    # Build results table
    table = Table(title=f"String vs LowCardinality(String) — {NUM_ROWS:,} rows")
    table.add_column("Metric", style="bold")
    table.add_column("String", justify="right")
    table.add_column("LowCardinality", justify="right")
    table.add_column("Speedup", justify="right", style="green")

    # Insert row
    speedup = t_insert_string / t_insert_lc if t_insert_lc > 0 else float("inf")
    table.add_row("INSERT", f"{t_insert_string:.4f}s", f"{t_insert_lc:.4f}s", f"{speedup:.2f}x")

    # Query rows
    for label, query_tpl in bench_queries:
        times = {"events_string": [], "events_lc": []}
        for tbl in ("events_string", "events_lc"):
            q = query_tpl.format(table=tbl)
            for _ in range(10):
                r = client.query(q)
                times[tbl].append(elapsed_s(r))

        avg_s = sum(times["events_string"]) / 10
        avg_lc = sum(times["events_lc"]) / 10
        speedup = avg_s / avg_lc if avg_lc > 0 else float("inf")
        table.add_row(label, f"{avg_s:.4f}s", f"{avg_lc:.4f}s", f"{speedup:.2f}x")

    # Storage rows
    table.add_section()
    for tbl_key, tbl_label in [("events_lc", "LowCardinality"), ("events_string", "String")]:
        compressed, uncompressed = storage_by_table.get(tbl_key, ("?", "?"))
        table.add_row(f"Storage ({tbl_label})", compressed, uncompressed, "")

    console.print()
    console.print(table)

    # Cleanup
    client.command("DROP TABLE IF EXISTS events_string")
    client.command("DROP TABLE IF EXISTS events_lc")


if __name__ == "__main__":
    main()
