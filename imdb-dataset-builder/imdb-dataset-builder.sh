#!/usr/bin/env bash
# IMDb Dataset Builder: load, curate, and profile IMDb title.basics data, then
# optionally publish the clean dataset to Hugging Face.
#
# Usage: ./imdb-dataset-builder.sh [--sample] [--year-from YEAR] [--publish] [--airtable]
#
# Options:
#   --sample          Run on a 500k row sample (default: full ~12.6M row dataset)
#   --year-from YEAR  Earliest startYear to keep in the curated output (default: 1950)
#   --publish         Publish the curated dataset to Hugging Face (requires HF_TOKEN)
#   --airtable        Publish the showcase sample to Airtable (requires
#                     AIRTABLE_API_KEY + AIRTABLE_BASE_ID)

set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"

# Distributed backend: a real ClickHouse server + PostgreSQL orchestration rather
# than embedded chdb + SQLite, which is single-process and so a poor fit for the
# worker below. Exporting these is what tells setup_aaiclick to set up servers at
# all; point either at an existing cluster (CI passes service containers) and it
# probes that instead of provisioning.
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_REPORT_FILE="tmp/imdb_report.md"
# Task stdout/stderr streams to the ClickHouse task_logs table; this catches
# only worker-process-level messages, so it stays empty on a clean run.
WORKER_LOG="tmp/imdb_worker.log"
mkdir -p tmp

# Pipeline kwargs go straight through as `run-job --set KEY=VALUE` (JSON-typed),
# so there is no JSON string to assemble here.
KWARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --sample)    KWARGS+=(--set "limit=500000"); SAMPLE=1; shift ;;
        --year-from) KWARGS+=(--set "year_from=$2"); shift 2 ;;
        --publish)   KWARGS+=(--set "publish_hf=true"); shift ;;
        --airtable)  KWARGS+=(--set "publish_airtable=true"); shift ;;
        *) echo "Unknown flag: $1" >&2
           echo "Usage: $0 [--sample] [--year-from YEAR] [--publish] [--airtable]" >&2; exit 1 ;;
    esac
done

../scripts/setup_aaiclick

echo "## IMDb Dataset Builder Pipeline"
[ -n "${SAMPLE:-}" ] || echo "Running on the full IMDb dataset (~12.6M rows; pass --sample for a 500k-row demo)."

# Workers execute the DAG's tasks; the EXIT trap stops them, so a failed run
# never leaves them behind.
stop_workers() { kill ${WORKER_PID:-} ${BG_PID:-} 2>/dev/null || true; wait ${WORKER_PID:-} ${BG_PID:-} 2>/dev/null || true; }
trap stop_workers EXIT
$PYTHON -m aaiclick background start >/dev/null 2>&1 & BG_PID=$!
$PYTHON -m aaiclick execution-worker start > "$WORKER_LOG" 2>&1 & WORKER_PID=$!

# `run-job --progress` registers the job, streams per-task progress, blocks until
# it reaches a terminal status, and exits non-zero if it failed — so no job-id
# scraping, no poll loop, and no status branching is needed here.
STATUS=0
$PYTHON -m aaiclick run-job imdb_dataset_builder.imdb_dataset_pipeline "${KWARGS[@]}" --progress || STATUS=$?

if [ -s "$WORKER_LOG" ]; then
    printf '\n### Worker Log\n\n'
    cat "$WORKER_LOG"
fi
if [ $STATUS -eq 0 ]; then
    printf '\n### Dataset Report\n\n'
    cat "$AAICLICK_REPORT_FILE"
fi
exit $STATUS
