#!/usr/bin/env python3
"""Compare ClickHouse String vs LowCardinality(String) storage and query performance."""

import os
import clickhouse_connect
from rich.console import Console
from rich.table import Table

NUM_ROWS = 10_000_000
NUM_RUNS = 10
CARDINALITIES = [10, 1000]

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

ALL_EVENTS = [f"{comp}_{action}" for comp in _COMPONENTS for action in _ACTIONS]

BENCH_QUERIES = [
    ("COUNT with filter", "SELECT count() FROM {table} WHERE event_type = 'button_click'"),
    ("GROUP BY",          "SELECT event_type, count() FROM {table} GROUP BY event_type"),
    ("DISTINCT",          "SELECT DISTINCT event_type FROM {table}"),
    ("COUNT DISTINCT",    "SELECT count(DISTINCT event_type) FROM {table}"),
    ("IN (5 values)",     "SELECT count() FROM {table} WHERE event_type IN ('button_click','link_hover','form_submit','modal_open','grid_resize')"),
    ("ORDER BY LIMIT",    "SELECT event_type FROM {table} ORDER BY event_type LIMIT 100"),
    ("LIKE pattern",      "SELECT count() FROM {table} WHERE event_type LIKE '%_click'"),
]

COL_TYPES = {"string": "String", "lc": "LowCardinality(String)"}

console = Console()


def elapsed_s(result):
    """Extract server-side elapsed seconds from a query result."""
    return int(result.summary.get("elapsed_ns", 0)) / 1e9


def table_names(cardinality):
    """Return (string_table, lc_table) names for a given cardinality."""
    return f"events_string_{cardinality}", f"events_lc_{cardinality}"


def run_bench(client, cardinality):
    """Run a full benchmark for the given number of distinct event types."""
    events = ALL_EVENTS[:cardinality]
    tbl_str, tbl_lc = table_names(cardinality)
    tables = {"string": tbl_str, "lc": tbl_lc}

    # Create tables
    for key, tbl in tables.items():
        client.command(f"DROP TABLE IF EXISTS {tbl}")
        client.command(f"""
            CREATE TABLE {tbl} (
                id UInt64,
                event_type {COL_TYPES[key]},
                created_at DateTime
            ) ENGINE = MergeTree() ORDER BY id
        """)

    # Insert rows
    console.print(f"  Inserting {NUM_ROWS:,} rows...")
    status_array = "[" + ",".join(f"'{s}'" for s in events) + "]"
    num_events = len(events)

    insert_times = {}
    for key, tbl in tables.items():
        r = client.query(f"""
            INSERT INTO {tbl}
            SELECT number, {status_array}[number % {num_events} + 1], now()
            FROM numbers({NUM_ROWS})
        """)
        insert_times[key] = elapsed_s(r)

    # Storage
    storage = client.query(f"""
        SELECT
            table,
            formatReadableSize(sum(data_compressed_bytes)) AS compressed,
            formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
        FROM system.parts
        WHERE table IN ('{tbl_str}', '{tbl_lc}') AND active
        GROUP BY table
        ORDER BY table
    """)
    storage_by_table = {row[0]: (row[1], row[2]) for row in storage.result_rows}

    # Query performance (first run is warmup, discard it)
    console.print("  Running queries...")
    results = {}
    results["INSERT"] = (insert_times["string"], insert_times["lc"])

    for label, query_tpl in BENCH_QUERIES:
        times = {tbl_str: [], tbl_lc: []}
        for tbl in (tbl_str, tbl_lc):
            q = query_tpl.format(table=tbl)
            for i in range(NUM_RUNS + 1):
                r = client.query(q)
                if i > 0:  # skip warmup
                    times[tbl].append(elapsed_s(r))
        avg_s = sum(times[tbl_str]) / NUM_RUNS
        avg_lc = sum(times[tbl_lc]) / NUM_RUNS
        results[label] = (avg_s, avg_lc)

    # Cleanup
    for tbl in tables.values():
        client.command(f"DROP TABLE IF EXISTS {tbl}")

    return results, storage_by_table


def main():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    all_results = {}
    all_storage = {}
    for card in CARDINALITIES:
        console.print(f"\n[bold]Cardinality {card}[/bold]")
        all_results[card], all_storage[card] = run_bench(client, card)

    # --- Combined performance table ---
    labels = ["INSERT"] + [label for label, _ in BENCH_QUERIES]

    table = Table(title=f"String vs LowCardinality(String) — {NUM_ROWS:,} rows")
    table.add_column("Query", style="bold")
    for card in CARDINALITIES:
        table.add_column(f"String ({card})", justify="right")
        table.add_column(f"LC ({card})", justify="right")
        table.add_column(f"Speedup ({card})", justify="right", style="green")

    for label in labels:
        row = [label]
        for card in CARDINALITIES:
            t_str, t_lc = all_results[card][label]
            speedup = t_str / t_lc if t_lc > 0 else float("inf")
            row += [f"{t_str:.4f}s", f"{t_lc:.4f}s", f"{speedup:.2f}x"]
        table.add_row(*row)

    console.print()
    console.print(table)

    # --- Storage table ---
    st = Table(title="Storage")
    st.add_column("Cardinality", style="bold")
    st.add_column("String compressed", justify="right")
    st.add_column("LC compressed", justify="right")
    st.add_column("String uncompressed", justify="right")
    st.add_column("LC uncompressed", justify="right")

    for card in CARDINALITIES:
        storage = all_storage[card]
        tbl_str, tbl_lc = table_names(card)
        s_comp, s_uncomp = storage.get(tbl_str, ("?", "?"))
        lc_comp, lc_uncomp = storage.get(tbl_lc, ("?", "?"))
        st.add_row(str(card), s_comp, lc_comp, s_uncomp, lc_uncomp)

    console.print()
    console.print(st)


if __name__ == "__main__":
    main()
