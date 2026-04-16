# Data Lineage

aaiclick automatically records every data operation into a ClickHouse
**operation log** (oplog). After a pipeline finishes you can walk the log
backward or forward to answer "where did this table come from?" or "what
downstream tables depend on this one?"

## How the oplog works

Every `create_object`, `insert`, `copy`, `group_by`, `where`, etc. that runs
inside a `task_scope` is recorded as an `OperationEvent`:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `UInt64` | Snowflake ID |
| `result_table` | `String` | Table produced by this operation |
| `operation` | `String` | Operation name (`insert`, `copy`, `group_by`, ...) |
| `args` | `Array(String)` | Input tables consumed by this operation |
| `kwargs` | `Map(String, String)` | Named arguments (column names, predicates, ...) |
| `sql_template` | `Nullable(String)` | SQL that was executed, if available |
| `task_id` | `Nullable(UInt64)` | Orchestration task that produced this operation |
| `job_id` | `Nullable(UInt64)` | Job the task belongs to |
| `created_at` | `DateTime64(3)` | When the operation ran |

A companion `table_registry` tracks every table created during the session.

Recording is **zero-overhead when disabled** — `oplog_record()` checks a
`ContextVar` and returns immediately when no collector is active.

## When is the oplog active?

| Context | Oplog |
|---------|-------|
| `task_scope(task_id, job_id)` inside `orch_context()` | Always active — one `OplogCollector` per task, flushed on clean exit |
| `data_context()` (standalone) | Not active by default |

During orchestration every task gets its own `OplogCollector`. Events are
buffered in memory and batch-inserted into ClickHouse on successful exit. If
the task raises, the buffer is discarded so the log never contains partial
runs.

## Querying lineage

After the pipeline completes, open a `lineage_context()` and query the graph:

```python
from aaiclick.oplog.lineage import (
    backward_oplog,
    forward_oplog,
    lineage_context,
    oplog_subgraph,
)

async with lineage_context():
    # All upstream operations that produced "report"
    nodes = await backward_oplog("report")

    # All downstream tables that consumed "raw_events"
    nodes = await forward_oplog("raw_events")

    # Structured graph for visualization or AI context
    graph = await oplog_subgraph("report", direction="backward")
```

### backward_oplog

```python
async def backward_oplog(table: str, max_depth: int = 10) -> list[OplogNode]
```

Traces upstream. Uses a `WITH RECURSIVE` CTE so the entire traversal runs in
a single SQL round-trip. A `visited` array prevents revisiting nodes in
diamond-shaped graphs.

### forward_oplog

```python
async def forward_oplog(table: str, max_depth: int = 10) -> list[OplogNode]
```

Traces downstream. Returns nodes in BFS order starting from operations that
consumed `table`.

### oplog_subgraph

```python
async def oplog_subgraph(
    table: str,
    direction: str = "backward",
    max_depth: int = 10,
) -> OplogGraph
```

Returns an `OplogGraph` with nodes and edges suitable for visualization or as
context for AI explanation. Wraps `backward_oplog` or `forward_oplog`
depending on `direction`.

## Data structures

```python
@dataclass
class OplogNode:
    table: str
    operation: str
    args: list[str]          # input tables
    kwargs: dict[str, str]   # named arguments
    sql_template: str | None
    task_id: int | None
    job_id: int | None

@dataclass
class OplogEdge:
    source: str   # input table
    target: str   # output table
    operation: str

@dataclass
class OplogGraph:
    nodes: list[OplogNode]
    edges: list[OplogEdge]
```

## AI-powered explanation

If `aaiclick[ai]` is installed, `explain()` feeds the oplog subgraph to an
LLM and returns a human-readable explanation:

```python
import aaiclick

async with lineage_context():
    print(await aaiclick.explain("report"))
    # "report was produced by a group_by on filtered_events, which was
    #  filtered from raw_events (amount > 500) ingested from ..."

    print(await aaiclick.explain("report", question="why are there nulls in amount?"))
```

Requires `pip install "aaiclick[ai]"`.

## End-to-end example

A typical orchestration pipeline with lineage:

```python
from aaiclick import Schema, ColumnInfo, create_object
from aaiclick.orchestration import job, task, tasks_list

@task
async def ingest(raw_data: dict) -> str:
    schema = Schema(fieldtype="d", columns={
        "id": ColumnInfo("Int64"),
        "category": ColumnInfo("String", low_cardinality=True),
        "amount": ColumnInfo("Float64"),
    })
    obj = await create_object(schema)
    await obj.insert(raw_data)
    return obj.table_name

@task
async def transform(source_table: str) -> str:
    obj = await open_object(source_table)
    filtered = obj.where("amount > 100")
    result = await filtered.copy()
    return result.table_name

@task
async def report(source_table: str) -> str:
    obj = await open_object(source_table)
    summary = await obj.group_by("category").agg({
        "amount": [Agg("sum", "total"), Agg("count", "rows")],
    })
    print(summary.markdown())
    return summary.table_name

@job("etl_pipeline")
def etl_pipeline(raw_data: dict):
    raw = ingest(raw_data=raw_data)
    clean = transform(source_table=raw)
    final = report(source_table=clean)
    return tasks_list(final)
```

After execution, querying `backward_oplog("report_table")` returns the full
chain: `ingest -> transform -> report`, with the SQL and arguments at each
step.
