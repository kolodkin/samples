"""Generate raw.parquet once, shared across all framework runners via the
bind-mounted /data volume."""

import random

import pyarrow as pa
import pyarrow.parquet as pq

from .config import CATEGORIES, NUM_ROWS, RAW_DATA_PATH, SUBCATEGORIES


def main():
    random.seed(42)
    table = pa.table({
        "id": list(range(NUM_ROWS)),
        "category": [random.choice(CATEGORIES) for _ in range(NUM_ROWS)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(NUM_ROWS)],
        "amount": [random.uniform(0, 1000) for _ in range(NUM_ROWS)],
        "quantity": [random.randint(1, 100) for _ in range(NUM_ROWS)],
    })
    pq.write_table(table, RAW_DATA_PATH, compression="snappy")
    print(f"wrote {NUM_ROWS:,} rows to {RAW_DATA_PATH}")


if __name__ == "__main__":
    main()
