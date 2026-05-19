"""Generate raw.parquet once, shared across all framework runners via the
bind-mounted /data volume."""

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .config import CATEGORIES, NUM_ROWS, RAW_DATA_PATH, SUBCATEGORIES


def main():
    rng = np.random.default_rng(42)
    table = pa.table({
        "id":          np.arange(NUM_ROWS, dtype=np.int64),
        "category":    np.array(CATEGORIES)[rng.integers(0, len(CATEGORIES), NUM_ROWS)],
        "subcategory": np.array(SUBCATEGORIES)[rng.integers(0, len(SUBCATEGORIES), NUM_ROWS)],
        "amount":      rng.uniform(0, 1000, NUM_ROWS),
        "quantity":    rng.integers(1, 101, NUM_ROWS, dtype=np.int64),
    })
    pq.write_table(table, RAW_DATA_PATH, compression="snappy")
    print(f"wrote {NUM_ROWS:,} rows to {RAW_DATA_PATH}")


if __name__ == "__main__":
    main()
