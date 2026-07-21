#!/bin/bash
# GitHub Exposure Scanner: enumerate an org's public repos, mirror-clone each,
# scan their full git history for leaked secrets, score exposure, and print a
# redacted report. Each finding is attributed to the commit that introduced it
# and flagged live-at-HEAD or historical-only.
#
# Usage: ./github-exposure-scanner.sh [--targets "org,org/repo,..."] \
#          [--max-repos N] [--max-file-kb N] [--head-only] [--max-repo-mb N] \
#          [--max-commits N] [--max-blobs N] [--clone-timeout N] [--airtable]
#
# Options:
#   --targets LIST     Comma-separated orgs and/or org/repo targets
#                      (default: octocat/Hello-World)
#   --max-repos N      Max repos per bare org, top by stars (default: 25)
#   --max-file-kb N    Skip files larger than this (default: 512)
#   --head-only        Scan only current HEAD (fast path); default scans full
#                      git history via a local mirror clone
#   --max-repo-mb N    Skip repos whose GitHub size exceeds N MB (default: 100)
#   --max-commits N    Cap history walk to N oldest commits (0 = all)
#   --max-blobs N      Cap history scan to N unique blobs (0 = all)
#   --clone-timeout N  Per-repo clone timeout in seconds (default: 300)
#   --airtable         Publish findings + summary to Airtable (default: off;
#                      requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID)
#
# Requires a distributed aaiclick backend (PostgreSQL + ClickHouse server)
# reachable at AAICLICK_SQL_URL / AAICLICK_CH_URL — provided by CI as service
# containers, or point those vars at an existing cluster.
#
# Environment:
#   GITHUB_TOKEN  — raises the GitHub API rate limit (public data still works
#                   unauthenticated, but at 60 req/hr)
#   GITHUB_REPOS  — default targets when --targets is omitted: comma-separated
#                   "org|repo" or bare "org" entries (e.g. "acme|widgets,octocat").
#                   An explicit --targets on the command line overrides it.

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
        --head-only)
            PARAMS_PARTS+=('"head_only": true')
            shift
            ;;
        --max-repo-mb)
            PARAMS_PARTS+=("\"max_repo_mb\": $2")
            shift 2
            ;;
        --max-commits)
            PARAMS_PARTS+=("\"max_commits\": $2")
            shift 2
            ;;
        --max-blobs)
            PARAMS_PARTS+=("\"max_blobs\": $2")
            shift 2
            ;;
        --clone-timeout)
            PARAMS_PARTS+=("\"clone_timeout\": $2")
            shift 2
            ;;
        --airtable)
            echo "Airtable publishing enabled..."
            PARAMS_PARTS+=('"publish_airtable": true')
            shift
            ;;
        *)
            echo "Unknown flag: $1" >&2
            echo "Usage: $0 [--targets LIST] [--max-repos N] [--max-file-kb N] [--head-only]" \
                 "[--max-repo-mb N] [--max-commits N] [--max-blobs N] [--clone-timeout N] [--airtable]" >&2
            exit 1
            ;;
    esac
done

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
