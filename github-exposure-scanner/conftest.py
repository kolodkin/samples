"""Expose aaiclick's orchestration test fixtures (SQLite + embedded chdb).

Importing these names registers them as pytest fixtures so the dynamic-DAG
test can run a full job in-process via ``ajob_test`` — no PostgreSQL or
ClickHouse server needed.
"""

from aaiclick.testing import (  # noqa: F401 - re-exported as pytest fixtures
    ch_worker_setup,
    gc_leak_check,
    orch_ctx,
    orch_ctx_no_ch,
    orch_module_ctx,
    orch_module_ctx_no_ch,
    sql_worker_setup,
)
