NUM_ROWS = 1_000_000
NUM_RUNS = 10
FILTER_THRESHOLD = 500.0
CATEGORIES = [f"cat_{i}" for i in range(10)]
SUBCATEGORIES = [f"sub_{i}" for i in range(1000)]

INGEST = "Ingest"

BENCH_NAMES = [
    INGEST,
    "Column sum",
    "Column multiply",
    "Filter rows",
    "Sort",
    "Count distinct",
    "Group-by sum",
    "Group-by count",
    "Group-by multi-agg",
    "Multi-key group-by",
    "High-card group-by",
]
