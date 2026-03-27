**Two schema changes that sped up our ClickHouse queries — what's worked for you?**

We've been running ClickHouse for our analytics pipeline and recently made two small schema changes that had a surprisingly big impact. Wanted to share in case it helps anyone else.

**1. Switching to `LowCardinality(String)` for repetitive columns**

We had columns like `status`, `country`, and `event_type` stored as plain `String`. After switching to `LowCardinality(String)`, our filters, DISTINCT, and LIKE queries got several times faster, and storage shrank noticeably. It basically dictionary-encodes repeated values under the hood.

```sql
CREATE TABLE events (
    status LowCardinality(String),
    created_at DateTime
) ENGINE = MergeTree() ORDER BY created_at;
```

**2. Storing Snowflake IDs as `UInt64` instead of `String`**

We were storing IDs as Strings — turns out that's the worst option. Switching to `UInt64` made inserts faster, cut storage, and JOINs improved the most. UUID performs similarly for reads, but UInt64 still wins on writes and disk. If your IDs are already 64-bit integers, just use `UInt64`.

```sql
CREATE TABLE events (
    id UInt64 DEFAULT generateSnowflakeID(),
    user_id UInt64,
    ...
) ENGINE = MergeTree() ORDER BY id;
```

I put together a benchmark if anyone wants to dig into the numbers: https://gist.github.com/kolodkin/b7450a9c497e8ca6f7c8a66f9f91ee90

Has anyone else found small schema tweaks that made a big difference in ClickHouse? Curious what others have run into.
