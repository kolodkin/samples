#!/bin/bash
# GitHub Exposure Scanner: enumerate an org's public repos, scan current-HEAD
# files for leaked secrets, score exposure, and print a redacted report.
#
# Usage: ./github-exposure-scanner.sh [--targets "org,org/repo,..."] \
#          [--max-repos N] [--max-file-kb N] [--airtable] [--local-setup]
#
# Options:
#   --targets LIST     Comma-separated orgs and/or org/repo targets
#                      (default: octocat/Hello-World)
#   --max-repos N      Max repos per bare org, top by stars (default: 25)
#   --max-file-kb N    Skip files larger than this (default: 512)
#   --airtable         Publish findings + summary to Airtable (default: off;
#                      requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID)
#   --local-setup      Auto-provision ClickHouse + PostgreSQL locally via the
#                      repo's scripts (default: off; CI provides them as services)
#
# Environment:
#   GITHUB_TOKEN  — raises the GitHub API rate limit (public data still works
#                   unauthenticated, but at 60 req/hr)

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

WORKER_LOG="tmp/ghx_worker.log"
export AAICLICK_REPORT_FILE="tmp/ghx_report.md"
mkdir -p tmp "$AAICLICK_LOG_DIR"

# Parse flags
PARAMS_PARTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --targets)
            IFS=',' read -ra _TARGS <<< "$2"
            _JSON_TARGS=$(printf '"%s",' "${_TARGS[@]}")
            _JSON_TARGS="[${_JSON_TARGS%,}]"
            PARAMS_PARTS+=("\"targets\": $_JSON_TARGS")
            shift 2
            ;;
        --max-repos)
            PARAMS_PARTS+=("\"max_repos\": $2")
            shift 2
            ;;
        --max-file-kb)
            PARAMS_PARTS+=("\"max_file_kb\": $2")
            shift 2
            ;;
        --airtable)
            echo "Airtable publishing enabled..."
            PARAMS_PARTS+=('"publish_airtable": true')
            shift
            ;;
        --local-setup)
            LOCAL_SETUP=1
            shift
            ;;
        *)
            echo "Unknown flag: $1" >&2
            echo "Usage: $0 [--targets LIST] [--max-repos N] [--max-file-kb N] [--airtable] [--local-setup]" >&2
            exit 1
            ;;
    esac
done

# Auto-provision databases locally only when --local-setup is passed.
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

PARAMS_ARG=""
if [ ${#PARAMS_PARTS[@]} -gt 0 ]; then
    PARAMS_ARG="{$(IFS=, ; echo "${PARAMS_PARTS[*]}")}"
fi

echo "## GitHub Exposure Scanner Pipeline"
echo

# Step 1: Register the job and capture its ID
echo "Registering job..."
if [ -n "$PARAMS_ARG" ]; then
    REGISTER_OUTPUT=$($PYTHON -m github_exposure_scanner --params "$PARAMS_ARG")
else
    REGISTER_OUTPUT=$($PYTHON -m github_exposure_scanner)
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
echo "### Exposure Report"
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
