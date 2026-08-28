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

# Pipeline kwargs are passed straight through as `run-job --set KEY=VALUE`
# (JSON-typed), so no JSON string assembly is needed here.
KWARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --movies)
            KWARGS+=(--set "corpus_size=$2")
            shift 2
            ;;
        --top-k)
            KWARGS+=(--set "top_k=$2")
            shift 2
            ;;
        --generate)
            echo "LLM answer generation enabled..."
            KWARGS+=(--set "generate=true")
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

echo "## Movie Plot RAG Pipeline"
echo

# Workers execute the DAG's tasks; both are stopped by the EXIT trap so a
# failed run never leaves them behind.
cleanup() {
    kill $WORKER_PID $BACKGROUND_PID 2>/dev/null || true
    wait $WORKER_PID $BACKGROUND_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting workers..."
$PYTHON -m aaiclick background start > /dev/null 2>&1 &
BACKGROUND_PID=$!
$PYTHON -m aaiclick execution-worker start > "$WORKER_LOG" 2>&1 &
WORKER_PID=$!
echo "Workers started (execution: $WORKER_PID, background: $BACKGROUND_PID)"
echo

# `run-job --progress` registers the job, streams per-task progress, blocks
# until it reaches a terminal status, and exits non-zero if it failed — so no
# job-id scraping, no poll loop, and no status branching is needed here.
STATUS=0
$PYTHON -m aaiclick run-job movie_plot_rag.movie_plot_rag_pipeline "${KWARGS[@]}" --progress || STATUS=$?

echo
echo "### Worker Log"
echo
cat "$WORKER_LOG"

if [ $STATUS -eq 0 ]; then
    echo
    echo "### RAG Report"
    echo
    cat "$AAICLICK_REPORT_FILE"
    echo
    echo "Pipeline completed successfully."
else
    echo
    echo "Pipeline FAILED (see task errors above)."
fi

exit $STATUS
