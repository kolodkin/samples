ClickHouse LowCardinality & Snowflake ID Benchmark
---

Benchmarks ClickHouse schema optimizations on 10M rows: compares String vs LowCardinality(String) for dictionary-encoded columns at varying cardinalities, and UInt64 (Snowflake) vs UUID vs String for ID storage across point lookups, range scans, JOINs, and aggregations.

```bash
./clickhouse-low-card-snow-id.sh
```
