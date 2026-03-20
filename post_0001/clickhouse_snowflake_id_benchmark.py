#!/usr/bin/env python3
"""Compare ClickHouse UInt64 (Snowflake) vs UUID vs String for ID storage and query performance."""

import os
import clickhouse_connect
from rich.console import Console
from rich.table import Table

NUM_ROWS = 10_000_000
NUM_RUNS = 10

BENCH_QUERIES = [
    ("Point lookup", "SELECT * FROM {table} WHERE id = {id_expr}"),
    ("Range scan",   "SELECT count() FROM {table} WHERE id > {range_start} AND id < {range_end}"),
    ("ORDER BY",     "SELECT id FROM {table} ORDER BY id DESC LIMIT 100"),
    ("IN (5 vals)",  "SELECT * FROM {table} WHERE id IN ({in_list})"),
    ("GROUP BY val", "SELECT value, count() FROM {table} GROUP BY value"),
]

COL_DEFS = {
    "uint64": {"id_type": "UInt64", "id_default": "generateSnowflakeID()"},
    "uuid":   {"id_type": "UUID",   "id_default": "generateUUIDv7()"},
    "string": {"id_type": "String", "id_default": "toString(generateUUIDv7())"},
}

console = Console()


def elapsed_s(result):
    """Extract server-side elapsed seconds from a query result."""
    return int(result.summary.get("elapsed_ns", 0)) / 1e9


def table_name(col_type):
    return f"snowflake_bench_{col_type}"


def run_bench(client):
    """Run the full benchmark across all column types."""
    tables = {key: table_name(key) for key in COL_DEFS}

    # Create tables
    for key, tbl in tables.items():
        defs = COL_DEFS[key]
        client.command(f"DROP TABLE IF EXISTS {tbl}")
        client.command(f"""
            CREATE TABLE {tbl} (
                id {defs['id_type']} DEFAULT {defs['id_default']},
                value UInt64
            ) ENGINE = MergeTree() ORDER BY id
        """)

    # Insert rows
    console.print(f"  Inserting {NUM_ROWS:,} rows...")
    insert_times = {}
    for key, tbl in tables.items():
        r = client.query(f"""
            INSERT INTO {tbl} (value)
            SELECT rand64()
            FROM numbers({NUM_ROWS})
        """)
        insert_times[key] = elapsed_s(r)

    # Grab sample IDs per table for parameterised queries
    samples = {}
    for key, tbl in tables.items():
        s = client.query(f"SELECT id FROM {tbl} ORDER BY id LIMIT 5")
        samples[key] = [row[0] for row in s.result_rows]

    # Storage
    storage = client.query(f"""
        SELECT
            table,
            formatReadableSize(sum(data_compressed_bytes)) AS compressed,
            formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
        FROM system.parts
        WHERE table IN ({','.join(f"'{t}'" for t in tables.values())}) AND active
        GROUP BY table
        ORDER BY table
    """)
    storage_by_table = {row[0]: (row[1], row[2]) for row in storage.result_rows}

    # Query performance
    console.print("  Running queries...")
    results = {}
    results["INSERT"] = {key: insert_times[key] for key in COL_DEFS}

    for label, query_tpl in BENCH_QUERIES:
        results[label] = {}
        for key, tbl in tables.items():
            ids = samples[key]

            def q(v):
                return str(v) if key == "uint64" else f"'{v}'"

            query = query_tpl.format(
                table=tbl,
                id_expr=q(ids[0]),
                range_start=q(ids[0]),
                range_end=q(ids[-1]),
                in_list=",".join(q(i) for i in ids),
            )
            times = []
            for i in range(NUM_RUNS + 1):
                r = client.query(query)
                if i > 0:
                    times.append(elapsed_s(r))
            results[label][key] = sum(times) / NUM_RUNS

    # Cleanup
    for tbl in tables.values():
        client.command(f"DROP TABLE IF EXISTS {tbl}")

    return results, storage_by_table


def print_summary(results, storage_by_table):
    """Print performance and storage tables."""
    labels = ["INSERT"] + [label for label, _ in BENCH_QUERIES]
    keys = list(COL_DEFS.keys())

    table = Table(title=f"Snowflake (UInt64) vs UUID vs String — {NUM_ROWS:,} rows")
    table.add_column("Query", style="bold", no_wrap=True)
    for key in keys:
        table.add_column(key, justify="right")
    table.add_column("UUID / Snowflake", justify="right", style="green")
    table.add_column("String / Snowflake", justify="right", style="green")

    for label in labels:
        times = results[label]
        row = [label]
        for key in keys:
            row.append(f"{times[key]:.4f}s")
        uuid_ratio = times["uuid"] / times["uint64"] if times["uint64"] > 0 else float("inf")
        str_ratio = times["string"] / times["uint64"] if times["uint64"] > 0 else float("inf")
        row += [f"{uuid_ratio:.2f}x", f"{str_ratio:.2f}x"]
        table.add_row(*row)

    console.print()
    console.print(table)

    st = Table(title="Storage")
    st.add_column("Type", style="bold")
    st.add_column("Compressed", justify="right")
    st.add_column("Uncompressed", justify="right")

    for key in keys:
        tbl = table_name(key)
        comp, uncomp = storage_by_table.get(tbl, ("?", "?"))
        st.add_row(key, comp, uncomp)

    console.print()
    console.print(st)


def main():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    version = client.command("SELECT version()")
    console.print(f"\n[bold]ID Benchmark: Snowflake (UInt64) vs UUID vs String[/bold]")
    console.print(f"  ClickHouse {version}")
    results, storage = run_bench(client)
    print_summary(results, storage)


if __name__ == "__main__":
    main()
