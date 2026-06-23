#!/bin/bash
# IMDb Dataset Builder: load, curate, and profile IMDb title.basics data,
# then optionally publish the clean dataset to Hugging Face.
#
# Usage: ./imdb-dataset-builder.sh [--full] [--year-from YEAR] [--publish] [--airtable]
#
# Options:
#   --full              Run on the full ~10M row dataset (default: 500k row demo limit)
#   --year-from YEAR    Earliest startYear to keep in the curated output (default: 1980)
#   --publish           Publish the curated dataset to Hugging Face (default: off;
#                       requires HF_TOKEN — registration fails if it is unset)
#   --airtable          Publish the showcase sample to Airtable (default: off; also
#                       requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID)
#
# Environment:
#   HF_TOKEN  — Hugging Face token, required when --publish is passed

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

WORKER_LOG="tmp/imdb_worker.log"
export AAICLICK_REPORT_FILE="tmp/imdb_report.md"
mkdir -p tmp

# Parse flags
PARAMS_PARTS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --full)
            echo "Running on full IMDb dataset (~10M rows)..."
            PARAMS_PARTS+=('"limit": null')
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
        *)
            echo "Unknown flag: $1" >&2
            echo "Usage: $0 [--full] [--year-from YEAR] [--publish] [--airtable]" >&2
            exit 1
            ;;
    esac
done

if [ ${#PARAMS_PARTS[@]} -eq 0 ]; then
    echo "Running on 500k row demo (pass --full for complete dataset)..."
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
