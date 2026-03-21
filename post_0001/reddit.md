**ClickHouse performance: two easy schema wins**
**1. `LowCardinality(String)` for low-variance string columns**
Dictionary-encodes repeated strings (status, country, event_type).
In the benchmark, filters, DISTINCT, and LIKE were several times faster; GROUP BY improved at higher cardinality.
Storage shrank significantly — especially as the number of unique values grows, where plain String bloats but LC stays compact.
([docs](https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality))
```sql
CREATE TABLE events (
    status LowCardinality(String),
    created_at DateTime
) ENGINE = MergeTree() ORDER BY created_at;
```
[Benchmark](https://gist.github.com/kolodkin/b7450a9c497e8ca6f7c8a66f9f91ee90)
---
**2. Store Snowflake IDs as `UInt64`, not `String` or `UUID`**
[Snowflake IDs](https://en.wikipedia.org/wiki/Snowflake_ID) are 64-bit integers — no need for 128-bit UUIDs. The benchmark compared all three:

UInt64 wins on **INSERT** and **storage** — writes are noticeably faster and disk footprint much smaller.
For read queries (point lookup, range scan, ORDER BY, OFFSET LIMIT, IN, GROUP BY) UInt64 and UUID perform about the same.
**JOIN** is where String hurts most. String is the worst choice overall: slowest writes, largest storage, slowest JOINs.
If your IDs are already 64-bit integers, UInt64 is still the best overall choice. ([UInt64 docs](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint), [UUID docs](https://clickhouse.com/docs/en/sql-reference/data-types/uuid))
```sql
CREATE TABLE events (
    id UInt64,
    user_id UInt64,
    ...
) ENGINE = MergeTree() ORDER BY id;
```
[Benchmark](https://gist.github.com/kolodkin/9ca3c8991f327d8c3e8e68606dc9afc0)
Small schema decisions, real impact. Happy to discuss in the comments!
