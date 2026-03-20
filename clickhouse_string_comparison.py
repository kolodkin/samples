#!/usr/bin/env python3
"""Compare ClickHouse String vs LowCardinality(String) storage and query performance."""

import time
import clickhouse_connect


def timed(client, query, label):
    """Execute a query and return (result, elapsed_seconds)."""
    start = time.perf_counter()
    result = client.query(query)
    elapsed = time.perf_counter() - start
    print(f"  {label}: {elapsed:.4f}s")
    return result, elapsed


def main():
    client = clickhouse_connect.get_client(host="localhost")

    # Cleanup
    client.command("DROP TABLE IF EXISTS events_string")
    client.command("DROP TABLE IF EXISTS events_lc")

    # --- Create tables ---
    print("Creating tables...")

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

    # --- Insert 1M rows ---
    print("\nInserting 1M rows...")

    _, t_insert_string = timed(client, """
        INSERT INTO events_string
        SELECT
            number,
            ['active', 'inactive', 'pending', 'banned', 'deleted'][number % 5 + 1],
            now()
        FROM numbers(1000000)
    """, "events_string (String)")

    _, t_insert_lc = timed(client, """
        INSERT INTO events_lc
        SELECT
            number,
            ['active', 'inactive', 'pending', 'banned', 'deleted'][number % 5 + 1],
            now()
        FROM numbers(1000000)
    """, "events_lc (LowCardinality)")

    # --- Storage comparison ---
    print("\nStorage comparison:")
    result, _ = timed(client, """
        SELECT
            table,
            formatReadableSize(sum(data_compressed_bytes)) AS compressed,
            formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
            count() AS parts
        FROM system.parts
        WHERE table IN ('events_string', 'events_lc') AND active
        GROUP BY table
    """, "storage query")

    print(f"\n  {'Table':<20} {'Compressed':<15} {'Uncompressed':<15} {'Parts':<6}")
    print(f"  {'-'*56}")
    for row in result.result_rows:
        print(f"  {row[0]:<20} {row[1]:<15} {row[2]:<15} {row[3]:<6}")

    # --- Query performance comparison ---
    queries = [
        ("COUNT with filter", "SELECT count() FROM {table} WHERE status = 'active'"),
        ("GROUP BY",          "SELECT status, count() FROM {table} GROUP BY status"),
        ("DISTINCT",          "SELECT DISTINCT status FROM {table}"),
    ]

    print("\n\nQuery performance (each run 3 times, showing average):")
    print(f"  {'Query':<25} {'String':<12} {'LowCard':<12} {'Speedup':<10}")
    print(f"  {'-'*59}")

    for label, query_tpl in queries:
        times = {"events_string": [], "events_lc": []}
        for table in ("events_string", "events_lc"):
            q = query_tpl.format(table=table)
            for _ in range(3):
                start = time.perf_counter()
                client.query(q)
                times[table].append(time.perf_counter() - start)

        avg_s = sum(times["events_string"]) / 3
        avg_lc = sum(times["events_lc"]) / 3
        speedup = avg_s / avg_lc if avg_lc > 0 else float("inf")
        print(f"  {label:<25} {avg_s:.4f}s      {avg_lc:.4f}s      {speedup:.2f}x")

    # --- Summary ---
    print(f"\n\nInsert timing: String={t_insert_string:.4f}s, LowCardinality={t_insert_lc:.4f}s")
    print("Done.")

    # Cleanup
    client.command("DROP TABLE IF EXISTS events_string")
    client.command("DROP TABLE IF EXISTS events_lc")


if __name__ == "__main__":
    main()
