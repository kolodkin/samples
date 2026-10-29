#!/usr/bin/env bash
# IMDb Dataset Builder — distributed mode: ClickHouse + PostgreSQL servers at
# AAICLICK_CH_URL / AAICLICK_SQL_URL (provisioned locally by
# scripts/setup_aaiclick when nothing answers there), with a worker executing
# the tasks.
#
# Usage: ./imdb-dataset-builder.sh [--set KEY=VALUE ...]
#   e.g. ./imdb-dataset-builder.sh --set limit=500000
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-uv run python}"

export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_REPORT_FILE="tmp/imdb_report.md"
../scripts/setup_aaiclick

$PYTHON -m aaiclick background start & BG_PID=$!
$PYTHON -m aaiclick execution-worker start & WORKER_PID=$!
trap 'kill $WORKER_PID $BG_PID 2>/dev/null || true; wait 2>/dev/null || true' EXIT

$PYTHON -m aaiclick run-job imdb_dataset_builder.imdb_dataset_pipeline --progress "$@"
printf '\n### Dataset Report\n\n'
cat "$AAICLICK_REPORT_FILE"
