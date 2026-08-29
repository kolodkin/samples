#!/usr/bin/env bash
# GitHub Exposure Scanner: enumerate an org's public repos, mirror-clone each,
# scan their full git history for leaked secrets, score exposure, and print a
# redacted report. Each finding is attributed to the commit that introduced it
# and flagged live-at-HEAD or historical-only.
#
# Usage: ./github-exposure-scanner.sh [--targets "org,org/repo,..."] [--max-repos N]
#          [--max-file-kb N] [--head-only] [--max-repo-mb N] [--max-commits N]
#          [--max-blobs N] [--clone-timeout N] [--airtable]
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
#   --airtable         Publish findings + summary to Airtable (requires
#                      AIRTABLE_API_KEY + AIRTABLE_BASE_ID)
#
# Environment:
#   GITHUB_TOKEN  — raises the GitHub API rate limit (public data still works
#                   unauthenticated, but at 60 req/hr)
#   GITHUB_REPOS  — default targets when --targets is omitted: comma-separated
#                   "org|repo" or bare "org" entries (e.g. "acme|widgets,octocat").
#                   An explicit --targets on the command line overrides it.

set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"

# Distributed backend: a real ClickHouse server + PostgreSQL orchestration, the
# right fit for the worker-process execution model below. Exporting these is what
# tells setup_aaiclick to set up servers at all; point either at an existing
# cluster and it probes that instead of provisioning.
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_LOG_DIR="${AAICLICK_LOG_DIR:-tmp/logs}"
export AAICLICK_REPORT_FILE="tmp/ghx_report.md"
WORKER_LOG="tmp/ghx_worker.log"
mkdir -p tmp "$AAICLICK_LOG_DIR"

# Pipeline kwargs go straight through as `run-job --set KEY=VALUE` (JSON-typed),
# so only --targets needs shaping — into the JSON array the job expects.
KWARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --targets)       KWARGS+=(--set "targets=[\"${2//,/\",\"}\"]"); shift 2 ;;
        --max-repos)     KWARGS+=(--set "max_repos=$2"); shift 2 ;;
        --max-file-kb)   KWARGS+=(--set "max_file_kb=$2"); shift 2 ;;
        --max-repo-mb)   KWARGS+=(--set "max_repo_mb=$2"); shift 2 ;;
        --max-commits)   KWARGS+=(--set "max_commits=$2"); shift 2 ;;
        --max-blobs)     KWARGS+=(--set "max_blobs=$2"); shift 2 ;;
        --clone-timeout) KWARGS+=(--set "clone_timeout=$2"); shift 2 ;;
        --head-only)     KWARGS+=(--set "head_only=true"); shift ;;
        --airtable)      KWARGS+=(--set "publish_airtable=true"); shift ;;
        *) echo "Unknown flag: $1" >&2
           echo "Usage: $0 [--targets LIST] [--max-repos N] [--max-file-kb N] [--head-only]" \
                "[--max-repo-mb N] [--max-commits N] [--max-blobs N] [--clone-timeout N] [--airtable]" >&2
           exit 1 ;;
    esac
done

../scripts/setup_aaiclick

echo "## GitHub Exposure Scanner Pipeline"

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
$PYTHON -m aaiclick run-job github_exposure_scanner.exposure_pipeline "${KWARGS[@]}" --progress || STATUS=$?

printf '\n### Worker Log\n\n'
cat "$WORKER_LOG"
if [ $STATUS -eq 0 ]; then
    printf '\n### Exposure Report\n\n'
    cat "$AAICLICK_REPORT_FILE"
fi
exit $STATUS
