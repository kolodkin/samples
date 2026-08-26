#!/bin/bash
# Movie Plot RAG: embed movie plots with a local sentence-transformers model,
# store the vectors in ClickHouse, answer natural-language queries with
# cosine-similarity SQL, and optionally ground LLM answers in the results.
#
# Usage: ./movie-plot-rag.sh [--movies N] [--top-k N] [--generate] [--local-setup]
#
# Options:
#   --movies N     Corpus size — top-N movies by IMDb vote count (default: 1000)
#   --top-k N      Retrieved movies per query (default: 3)
#   --generate     Run the LLM answer step (default: off; requires
#                  ANTHROPIC_API_KEY, or MOVIE_RAG_LLM_MODEL pointed at a
#                  local model such as ollama/llama3.1:8b)
#   --local-setup  Auto-provision ClickHouse + PostgreSQL locally via apt
#                  (default: off). Without it, the databases are assumed to
#                  already exist at AAICLICK_CH_URL / AAICLICK_SQL_URL.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

# Distributed backend (default): real ClickHouse server + PostgreSQL
# orchestration — the right fit for the worker-process execution model below.
# Connection contracts match scripts/setup_clickhouse and scripts/setup_postgres.
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_LOG_DIR="${AAICLICK_LOG_DIR:-tmp/logs}"

WORKER_LOG="tmp/movie_plot_rag_worker.log"
export AAICLICK_REPORT_FILE="tmp/movie_plot_rag_report.md"
mkdir -p tmp "$AAICLICK_LOG_DIR"

# Parse flags
PARAMS_PARTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --movies)
            PARAMS_PARTS+=("\"corpus_size\": $2")
            shift 2
            ;;
        --top-k)
            PARAMS_PARTS+=("\"top_k\": $2")
            shift 2
            ;;
        --generate)
            echo "LLM answer generation enabled..."
            PARAMS_PARTS+=('"generate": true')
            shift
            ;;
        --local-setup)
            LOCAL_SETUP=1
            shift
            ;;
        *)
            echo "Unknown flag: $1" >&2
            echo "Usage: $0 [--movies N] [--top-k N] [--generate] [--local-setup]" >&2
            exit 1
            ;;
    esac
done

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

echo "## Movie Plot RAG Pipeline"
echo

# Step 1: Register the job and capture its ID
echo "Registering job..."
if [ -n "$PARAMS_ARG" ]; then
    REGISTER_OUTPUT=$($PYTHON -m movie_plot_rag --params "$PARAMS_ARG")
else
    REGISTER_OUTPUT=$($PYTHON -m movie_plot_rag)
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

# Step 3: Start execution worker in background, capturing output to log file
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
echo "### RAG Report"
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
