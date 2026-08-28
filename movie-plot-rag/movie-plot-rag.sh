#!/usr/bin/env bash
# Movie Plot RAG: embed movie plots with a local sentence-transformers model,
# store the vectors in ClickHouse, answer natural-language queries with
# cosine-similarity SQL, and ground LLM answers in what came back.
#
# Usage: ./movie-plot-rag.sh [--movies N] [--top-k N] [--ollama]
#
# Options:
#   --movies N   Corpus size — top-N movies by IMDb vote count (default: 1000)
#   --top-k N    Retrieved movies per query (default: 3)
#   --ollama     Install and start a local Ollama server as well. Without it the
#                generation step needs an Ollama server already running, or a
#                hosted AAICLICK_AI_MODEL + AAICLICK_AI_API_KEY.

set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"

# Distributed backend: a real ClickHouse server + PostgreSQL orchestration, the
# right fit for the worker-process execution model below. Point either URL at an
# existing cluster and setup_aaiclick probes it instead of provisioning.
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_LOG_DIR="${AAICLICK_LOG_DIR:-tmp/logs}"
export AAICLICK_REPORT_FILE="tmp/movie_plot_rag_report.md"
WORKER_LOG="tmp/movie_plot_rag_worker.log"
mkdir -p tmp "$AAICLICK_LOG_DIR"

# Pipeline kwargs go straight through as `run-job --set KEY=VALUE` (JSON-typed),
# so there is no JSON string to assemble here.
SETUP=(--ai)
KWARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --movies) KWARGS+=(--set "corpus_size=$2"); shift 2 ;;
        --top-k)  KWARGS+=(--set "top_k=$2"); shift 2 ;;
        --ollama) SETUP=(--ollama); shift ;;
        *) echo "Unknown flag: $1" >&2
           echo "Usage: $0 [--movies N] [--top-k N] [--ollama]" >&2; exit 1 ;;
    esac
done

../scripts/setup_aaiclick "${SETUP[@]}"

echo "## Movie Plot RAG Pipeline"

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
$PYTHON -m aaiclick run-job movie_plot_rag.movie_plot_rag_pipeline "${KWARGS[@]}" --progress || STATUS=$?

printf '\n### Worker Log\n\n'
cat "$WORKER_LOG"
if [ $STATUS -eq 0 ]; then
    printf '\n### RAG Report\n\n'
    cat "$AAICLICK_REPORT_FILE"
fi
exit $STATUS
