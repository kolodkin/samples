#!/usr/bin/env python3
"""Compare ClickHouse UInt64 vs String vs UUID for Snowflake ID storage and query performance."""

import os
import clickhouse_connect
from rich.console import Console
from rich.table import Table

NUM_ROWS = 10_000_000
NUM_RUNS = 10

BENCH_QUERIES = [
    ("Point lookup",    "SELECT * FROM {table} WHERE id = {id_expr}"),
    ("Range scan",      "SELECT count() FROM {table} WHERE id > {range_start} AND id < {range_end}"),
    ("JOIN",            "SELECT count() FROM {table} a JOIN {table} b ON a.user_id = b.user_id WHERE a.id != b.id LIMIT 1000"),
    ("GROUP BY id",     "SELECT user_id, count() FROM {table} GROUP BY user_id"),
    ("ORDER BY LIMIT",  "SELECT id FROM {table} ORDER BY id DESC LIMIT 100"),
    ("IN (5 values)",   "SELECT * FROM {table} WHERE id IN ({in_list})"),
    ("COUNT with filter", "SELECT count() FROM {table} WHERE user_id = {user_id_expr}"),
]

COL_DEFS = {
    "uint64": {"id": "UInt64", "user_id": "UInt64"},
    "string": {"id": "String", "user_id": "String"},
    "uuid":   {"id": "UUID",   "user_id": "UUID"},
}

console = Console()


def elapsed_s(result):
    """Extract server-side elapsed seconds from a query result."""
    return int(result.summary.get("elapsed_ns", 0)) / 1e9


def table_name(col_type):
    return f"snowflake_bench_{col_type}"


def run_bench(client):
    """Run the full benchmark across all three column types."""
    tables = {key: table_name(key) for key in COL_DEFS}

    # Create tables
    for key, tbl in tables.items():
        defs = COL_DEFS[key]
        client.command(f"DROP TABLE IF EXISTS {tbl}")
        client.command(f"""
            CREATE TABLE {tbl} (
                id {defs['id']},
                user_id {defs['id']},
                payload String,
                created_at DateTime
            ) ENGINE = MergeTree() ORDER BY id
        """)

    # Insert rows — generate snowflake-like IDs as UInt64, cast for other types
    console.print(f"  Inserting {NUM_ROWS:,} rows...")
    insert_times = {}
    for key, tbl in tables.items():
        if key == "uint64":
            id_expr = "generateSnowflakeID()"
            user_id_expr = "generateSnowflakeID()"
        elif key == "string":
            id_expr = "toString(generateSnowflakeID())"
            user_id_expr = "toString(generateSnowflakeID())"
        else:  # uuid
            id_expr = "generateUUIDv7()"
            user_id_expr = "generateUUIDv7()"

        r = client.query(f"""
            INSERT INTO {tbl}
            SELECT
                {id_expr},
                {user_id_expr},
                randomString(32),
                now() - INTERVAL number SECOND
            FROM numbers({NUM_ROWS})
        """)
        insert_times[key] = elapsed_s(r)

    # Grab sample IDs per table for parameterised queries
    samples = {}
    for key, tbl in tables.items():
        s = client.query(f"SELECT id, user_id FROM {tbl} ORDER BY id LIMIT 5")
        samples[key] = {
            "ids": [row[0] for row in s.result_rows],
            "user_id": s.result_rows[0][1],
        }

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
            ids = samples[key]["ids"]
            user_id = samples[key]["user_id"]

            def q(v):
                return f"'{v}'" if isinstance(v, (str, type(None))) or key != "uint64" else str(v)

            id_val = q(ids[0])
            range_s = q(ids[0])
            range_e = q(ids[-1])
            in_list = ",".join(q(i) for i in ids)
            user_val = q(user_id)

            query = query_tpl.format(
                table=tbl, id_expr=id_val, range_start=range_s,
                range_end=range_e, in_list=in_list, user_id_expr=user_val,
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

    table = Table(title=f"Snowflake ID types — {NUM_ROWS:,} rows")
    table.add_column("Query", style="bold")
    for key in keys:
        table.add_column(key, justify="right")
    table.add_column("UInt64 vs String", justify="right", style="green")
    table.add_column("UInt64 vs UUID", justify="right", style="green")

    for label in labels:
        times = results[label]
        row = [label]
        for key in keys:
            row.append(f"{times[key]:.4f}s")
        str_speedup = times["string"] / times["uint64"] if times["uint64"] > 0 else float("inf")
        uuid_speedup = times["uuid"] / times["uint64"] if times["uint64"] > 0 else float("inf")
        row += [f"{str_speedup:.2f}x", f"{uuid_speedup:.2f}x"]
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

    console.print("\n[bold]Snowflake ID Benchmark: UInt64 vs String vs UUID[/bold]")
    results, storage = run_bench(client)
    print_summary(results, storage)


if __name__ == "__main__":
    main()
