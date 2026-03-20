#!/usr/bin/env python3
"""Compare ClickHouse String vs LowCardinality(String) storage and query performance."""

import os
import clickhouse_connect
from rich.console import Console
from rich.table import Table


NUM_ROWS = 10_000_000

_COMPONENTS = [
    "button", "link", "icon", "card", "row", "cell", "chip", "tag", "avatar", "logo",
    "modal", "dialog", "tooltip", "popover", "toast", "sidebar", "navbar", "toolbar",
    "breadcrumb", "tab", "accordion", "carousel", "slider", "switch", "checkbox",
    "radio", "dropdown", "datepicker", "timepicker", "colorpicker", "textarea",
    "input", "form", "table", "list", "grid", "tree", "menu", "panel", "drawer",
    "badge", "banner", "alert", "progress", "spinner", "skeleton", "divider",
    "pagination", "stepper", "rating",
]

_ACTIONS = [
    "click", "dblclick", "hover", "focus", "blur", "open", "close", "toggle",
    "submit", "reset", "select", "deselect", "expand", "collapse", "scroll",
    "drag", "drop", "resize", "load", "error",
]

UI_EVENTS = [f"{comp}_{action}" for comp in _COMPONENTS for action in _ACTIONS]

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
            event_type String,
            created_at DateTime
        ) ENGINE = MergeTree() ORDER BY id
    """)

    client.command("""
        CREATE TABLE events_lc (
            id UInt64,
            event_type LowCardinality(String),
            created_at DateTime
        ) ENGINE = MergeTree() ORDER BY id
    """)

    # --- Insert rows ---
    console.print(f"Inserting {NUM_ROWS:,} rows...")
    status_array = "[" + ",".join(f"'{s}'" for s in UI_EVENTS) + "]"
    n = len(UI_EVENTS)

    r = client.query(f"""
        INSERT INTO events_string
        SELECT number,
            {status_array}[number % {n} + 1],
            now()
        FROM numbers({NUM_ROWS})
    """)
    t_insert_string = elapsed_s(r)

    r = client.query(f"""
        INSERT INTO events_lc
        SELECT number,
            {status_array}[number % {n} + 1],
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
        ("COUNT with filter", "SELECT count() FROM {table} WHERE event_type = 'button_click'"),
        ("GROUP BY",          "SELECT event_type, count() FROM {table} GROUP BY event_type"),
        ("DISTINCT",          "SELECT DISTINCT event_type FROM {table}"),
        ("COUNT DISTINCT",    "SELECT count(DISTINCT event_type) FROM {table}"),
        ("IN (5 values)",     "SELECT count() FROM {table} WHERE event_type IN ('button_click','page_view','form_submit','modal_open','file_download')"),
        ("ORDER BY LIMIT",    "SELECT event_type FROM {table} ORDER BY event_type LIMIT 100"),
        ("LIKE pattern",      "SELECT count() FROM {table} WHERE event_type LIKE '%_click'"),
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

    console.print()
    console.print(table)

    # Storage table
    st = Table(title="Storage")
    st.add_column("Table", style="bold")
    st.add_column("Compressed", justify="right")
    st.add_column("Uncompressed", justify="right")
    for tbl_key in ("events_string", "events_lc"):
        compressed, uncompressed = storage_by_table.get(tbl_key, ("?", "?"))
        st.add_row(tbl_key, compressed, uncompressed)

    console.print()
    console.print(st)

    # Cleanup
    client.command("DROP TABLE IF EXISTS events_string")
    client.command("DROP TABLE IF EXISTS events_lc")


if __name__ == "__main__":
    main()
