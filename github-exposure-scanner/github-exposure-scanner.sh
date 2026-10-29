#!/usr/bin/env bash
# GitHub Exposure Scanner — distributed mode: ClickHouse + PostgreSQL servers at
# AAICLICK_CH_URL / AAICLICK_SQL_URL (provisioned locally by
# scripts/setup_aaiclick when nothing answers there), with a worker executing
# the tasks.
#
# Usage: ./github-exposure-scanner.sh [--set KEY=VALUE ...]
#   e.g. ./github-exposure-scanner.sh --set 'targets=["octocat/Hello-World"]' --set head_only=true
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-uv run python}"

export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_REPORT_FILE="tmp/ghx_report.md"
../scripts/setup_aaiclick

$PYTHON -m aaiclick background start & BG_PID=$!
$PYTHON -m aaiclick execution-worker start & WORKER_PID=$!
trap 'kill $WORKER_PID $BG_PID 2>/dev/null || true; wait 2>/dev/null || true' EXIT

$PYTHON -m aaiclick run-job github_exposure_scanner.exposure_pipeline --progress "$@"
printf '\n### Exposure Report\n\n'
cat "$AAICLICK_REPORT_FILE"
