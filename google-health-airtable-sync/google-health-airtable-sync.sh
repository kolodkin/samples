#!/bin/bash
# Google Health -> Airtable Sync: fetch the last N days of Google Health data
# (exercise sessions plus daily step/weight rollups) and upsert it into two
# Airtable tables. Designed to be run once a day (e.g. from cron); the window
# overlaps previous runs and the upsert is keyed, so missed days self-heal.
#
# Usage: ./google-health-airtable-sync.sh [--days N] [--no-airtable] [--auth]
#
# Options:
#   --days N        Lookback window in days, including today (default: 7)
#   --no-airtable   Fetch and report only; skip the Airtable upsert
#   --auth          One-time OAuth consent flow: prints the refresh token to
#                   export as GOOGLE_HEALTH_REFRESH_TOKEN, then exits
#
# Requires a distributed aaiclick backend (PostgreSQL + ClickHouse server)
# reachable at AAICLICK_SQL_URL / AAICLICK_CH_URL — provided by CI as service
# containers, or point those vars at an existing cluster.
#
# Environment:
#   GOOGLE_HEALTH_CLIENT_ID     — OAuth client id (Google Cloud console)
#   GOOGLE_HEALTH_CLIENT_SECRET — OAuth client secret
#   GOOGLE_HEALTH_REFRESH_TOKEN — long-lived token from ./…sh --auth
#   AIRTABLE_API_KEY            — Airtable PAT (else the upsert is skipped)
#   AIRTABLE_BASE_ID            — Airtable base id
#   AIRTABLE_ACTIVITIES_TABLE / AIRTABLE_DAILY_TABLE — table name overrides
#
# Daily schedule example (crontab, 06:30 every morning):
#   30 6 * * * /path/to/google-health-airtable-sync.sh >> /var/log/ghs.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

# Distributed backend (default): real ClickHouse + PostgreSQL orchestration,
# matching scripts/setup_clickhouse and scripts/setup_postgres. Override either
# URL to point at an existing cluster.
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_LOG_DIR="${AAICLICK_LOG_DIR:-tmp/logs}"

WORKER_LOG="tmp/ghs_worker.log"
export AAICLICK_REPORT_FILE="tmp/ghs_report.md"
mkdir -p tmp "$AAICLICK_LOG_DIR"

# Parse flags
PARAMS_PARTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --days)
            PARAMS_PARTS+=("\"days\": $2")
            shift 2
            ;;
        --no-airtable)
            PARAMS_PARTS+=('"publish_airtable": false')
            shift
            ;;
        --auth)
            exec $PYTHON -m google_health_airtable_sync --auth
            ;;
        *)
            echo "Unknown flag: $1" >&2
            echo "Usage: $0 [--days N] [--no-airtable] [--auth]" >&2
            exit 1
            ;;
    esac
done

PARAMS_ARG=""
if [ ${#PARAMS_PARTS[@]} -gt 0 ]; then
    PARAMS_ARG="{$(IFS=, ; echo "${PARAMS_PARTS[*]}")}"
fi

echo "## Google Health -> Airtable Sync Pipeline"
echo

# Step 1: Register the job and capture its ID
echo "Registering job..."
if [ -n "$PARAMS_ARG" ]; then
    REGISTER_OUTPUT=$($PYTHON -m google_health_airtable_sync --params "$PARAMS_ARG")
else
    REGISTER_OUTPUT=$($PYTHON -m google_health_airtable_sync)
fi
echo "$REGISTER_OUTPUT"
JOB_ID=$(echo "$REGISTER_OUTPUT" | grep -oP 'ID: \K[0-9]+')
echo

# Step 2: Start background cleanup worker
echo "Starting background cleanup worker..."
$PYTHON -m aaiclick background start &
BACKGROUND_PID=$!
echo

# Step 3: Start worker
echo "Starting worker..."
$PYTHON -m aaiclick execution-worker start > "$WORKER_LOG" 2>&1 &
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
echo "### Sync Report"
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
