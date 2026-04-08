#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"
$PYTHON clickhouse_lowcardinality_benchmark.py
$PYTHON clickhouse_snowflake_id_benchmark.py
