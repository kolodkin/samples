# post_0002: Python Data Library Benchmark

**NumPy vs Pandas vs PyArrow vs Polars vs Native Python**

Group-by and common column operations — how much do data libraries buy you
over plain Python?

## Dataset

- 1000 rows, generated in-memory
- Columns:
  - `id` — int
  - `category` — string, ~10 distinct values (low cardinality)
  - `subcategory` — string, ~1000 distinct values (high cardinality)
  - `amount` — float
  - `quantity` — int

## Data representation

| Library        | Data structure                |
| -------------- | ----------------------------- |
| Native Python  | `dict[str, list]` (columnar)  |
| NumPy          | dict of ndarrays              |
| Pandas         | DataFrame                     |
| PyArrow        | Table                         |
| Polars         | DataFrame                     |

## Benchmarks

### Column operations (all 5 libraries)

| Operation          | What it tests                            |
| ------------------ | ---------------------------------------- |
| Column sum         | Reduce `amount` to a single value        |
| Column multiply    | `amount * quantity` -> new column        |
| Filter rows        | `amount > threshold` -> subset           |
| Sort               | Order by `amount` descending             |
| Count distinct     | Unique values in `category`              |

### Group-by (all 5 libraries)

| Operation                    | Native Python / NumPy approach                          |
| ---------------------------- | ------------------------------------------------------- |
| Group-by + sum               | `defaultdict` / `np.unique` + manual loop               |
| Group-by + count             | `Counter` / manual                                      |
| Group-by + multi-agg         | sum, mean, min, max — manual dict accumulation           |
| Multi-key group-by           | tuple keys in dict / manual                              |
| High cardinality group-by    | 1000 groups — same approach, more keys                   |

NumPy and native Python group-by code will be ugly — that's the point.

## Metrics

- Wall-clock time (average of N runs, first run discarded as warmup)
- Peak memory (`tracemalloc`)
- Speedup ratio vs native Python baseline
- Output as rich tables (same style as post_0001)

## Decisions

- [x] Native Python: columnar `dict[str, list]`
- [x] 1000 rows for all libraries
- [x] NumPy group-by: include with manual `np.unique` + loop approach
- [x] Scope: column ops + group-by only (no joins, no string ops)

## File structure

```
post_0002/
  PLAN.md                        # this file
  reddit.md                      # blog post
  data_library_benchmark.py      # single script, all benchmarks
```

## CI

- `.github/workflows/post_0002.yml`
- No ClickHouse needed — pure Python
- Triggers on `post_0002/**` changes
