NUM_ROWS = 10_000_000
NUM_RUNS = 3
FILTER_THRESHOLD = 500.0

# Mid-range slice for the paginated companions (Column multiply page,
# Filter rows page, Sort page). Realistic deep-pagination pattern:
# force computation through the offset, then pull a small bounded
# window back to the driver. The non-paginated ops still measure full
# materialization (top of the table); the paginated ones measure how
# each framework handles OFFSET as a primitive.
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
    "Column multiply page",
    "Filter rows page",
    "Sort page",
]

FRAMEWORKS = ["aaiclick", "spark", "dask", "ray"]

RAW_DATA_PATH = "/data/raw.parquet"
RESULTS_DIR = "/data"
