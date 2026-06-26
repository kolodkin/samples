#!/bin/bash
# IMDb Dataset Builder: load, curate, and profile IMDb title.basics data,
# then optionally publish the clean dataset to Hugging Face.
#
# Usage: ./imdb-dataset-builder.sh [--sample] [--year-from YEAR] [--publish] [--airtable] [--local-setup]
#
# Options:
#   --sample            Run on a 500k row sample (default: full ~12.6M row dataset)
#   --year-from YEAR    Earliest startYear to keep in the curated output (default: 1950)
#   --publish           Publish the curated dataset to Hugging Face (default: off;
#                       requires HF_TOKEN — registration fails if it is unset)
#   --airtable          Publish the showcase sample to Airtable (default: off; also
#                       requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID)
#   --local-setup       Auto-provision ClickHouse + PostgreSQL locally via apt
#                       (default: off). Without it, the databases are assumed to
#                       already exist at AAICLICK_CH_URL / AAICLICK_SQL_URL — the
#                       mode CI uses, where the DBs are service containers.
#
# Environment:
#   HF_TOKEN  — Hugging Face token, required when --publish is passed

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

# ---------------------------------------------------------------------------
# Distributed backend (default): real ClickHouse server + PostgreSQL
# orchestration, instead of embedded chdb + SQLite. This is the correct fit
# for the worker-process execution model below (chdb + SQLite is single-process
# and the orchestration schema lives in Postgres via migrations). Connection
# contracts match scripts/setup_clickhouse and scripts/setup_postgres; override
# either URL to point at an existing cluster.
# ---------------------------------------------------------------------------
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"

WORKER_LOG="tmp/imdb_worker.log"
export AAICLICK_REPORT_FILE="tmp/imdb_report.md"
mkdir -p tmp

# Parse flags
PARAMS_PARTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --sample)
            echo "Running on 500k row sample..."
            PARAMS_PARTS+=('"limit": 500000')
            SAMPLE_MODE=1
            shift
            ;;
        --year-from)
            PARAMS_PARTS+=("\"year_from\": $2")
            shift 2
            ;;
        --publish)
            echo "Hugging Face publishing enabled..."
            PARAMS_PARTS+=('"publish_hf": true')
            shift
            ;;
        --airtable)
            echo "Airtable showcase publishing enabled..."
            PARAMS_PARTS+=('"publish_airtable": true')
            shift
            ;;
        --local-setup)
            LOCAL_SETUP=1
            shift
            ;;
        *)
            echo "Unknown flag: $1" >&2
            echo "Usage: $0 [--sample] [--year-from YEAR] [--publish] [--airtable] [--local-setup]" >&2
            exit 1
            ;;
    esac
done

# Auto-provision the databases locally only when --local-setup is passed
# (idempotent — both scripts skip work already done). setup_postgres also runs
# `aaiclick migrate`, so the orchestration schema exists. Without --local-setup
# (e.g. CI, where ClickHouse + PostgreSQL are service containers) the databases
# are assumed to already exist at the AAICLICK_*_URL values above.
if [ -n "${LOCAL_SETUP:-}" ]; then
    REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    if [ -x "$REPO_ROOT/scripts/setup_clickhouse" ] && [ -x "$REPO_ROOT/scripts/setup_postgres" ]; then
        echo "Provisioning distributed backend (ClickHouse + PostgreSQL)..."
        "$REPO_ROOT/scripts/setup_clickhouse"
        "$REPO_ROOT/scripts/setup_postgres"
        echo
    else
        echo "WARNING: scripts/setup_{clickhouse,postgres} not found — assuming a" >&2
        echo "         distributed backend is already running at the URLs above." >&2
    fi
fi

if [ -z "${SAMPLE_MODE:-}" ]; then
    echo "Running on full IMDb dataset (~12.6M rows; pass --sample for a 500k-row demo)..."
fi

PARAMS_ARG=""
if [ ${#PARAMS_PARTS[@]} -gt 0 ]; then
    PARAMS_ARG="{$(IFS=, ; echo "${PARAMS_PARTS[*]}")}"
fi

echo "## IMDb Dataset Builder Pipeline"
echo

# Step 1: Register the job and capture its ID
echo "Registering job..."
if [ -n "$PARAMS_ARG" ]; then
    REGISTER_OUTPUT=$($PYTHON -m imdb_dataset_builder --params "$PARAMS_ARG")
else
    REGISTER_OUTPUT=$($PYTHON -m imdb_dataset_builder)
fi
echo "$REGISTER_OUTPUT"
JOB_ID=$(echo "$REGISTER_OUTPUT" | grep -oP 'ID: \K[0-9]+')
echo

# Step 2: Start background cleanup worker
echo "Starting background cleanup worker..."
$PYTHON -m aaiclick background start &
BACKGROUND_PID=$!
echo "Background worker started (PID: $BACKGROUND_PID)"
echo

# Step 3: Start worker in background, capturing output to log file
echo "Starting worker..."
$PYTHON -m aaiclick worker start > "$WORKER_LOG" 2>&1 &
WORKER_PID=$!
echo "Worker started (PID: $WORKER_PID)"
echo

# Step 4: Poll job status until completed or failed
echo "Waiting for pipeline execution..."
MAX_WAIT=600
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    JOB_STATUS=$($PYTHON -m aaiclick job get "$JOB_ID" 2>/dev/null | grep "Status:" | awk '{print $2}')
    if [ "$JOB_STATUS" = "COMPLETED" ] || [ "$JOB_STATUS" = "FAILED" ]; then
        break
    fi
done
echo

# Step 5: Show job stats
echo "Job stats:"
$PYTHON -m aaiclick job stats "$JOB_ID"
echo

# Step 6: Stop workers
echo "Stopping workers..."
kill $WORKER_PID 2>/dev/null || true
kill $BACKGROUND_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true
wait $BACKGROUND_PID 2>/dev/null || true

# Step 7: Display worker log, then report
echo
echo "### Worker Log"
echo
cat "$WORKER_LOG"
echo
echo "### Dataset Report"
echo
cat "$AAICLICK_REPORT_FILE"

echo
if [ "$JOB_STATUS" = "COMPLETED" ]; then
    echo "Pipeline completed successfully."
elif [ "$JOB_STATUS" = "FAILED" ]; then
    echo "Pipeline FAILED."
    exit 1
else
    echo "Pipeline timed out (status: ${JOB_STATUS:-unknown})."
    exit 1
fi
