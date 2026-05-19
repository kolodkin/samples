NUM_ROWS = 10_000_000
NUM_RUNS = 3
FILTER_THRESHOLD = 500.0

# Mid-range slice for Column multiply / Filter rows / Sort. Mirrors a
# realistic distributed-inspection pattern: force computation through the
# offset, then pull a small bounded window back to the driver. Replaces
# the previous "materialize full result into a noop sink" hack.
SAMPLE_OFFSET = NUM_ROWS // 2
SAMPLE_LIMIT = 10
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

FRAMEWORKS = ["aaiclick", "spark", "dask", "ray"]

RAW_DATA_PATH = "/data/raw.parquet"
RESULTS_DIR = "/data"
