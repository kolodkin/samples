#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

FRAMEWORKS=("aaiclick" "spark" "dask" "ray")

# Per-framework cap. A hung framework otherwise burns the whole CI budget
# (we hit GH Actions' 6-hour ceiling once on a Dask sort deadlock).
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"

mkdir -p data
rm -f data/results-*.json

echo ">>> building images"
docker compose --profile tools --profile bench build

echo ">>> generating raw.parquet"
docker compose run --rm orchestrator \
    python -m distributed_data_libs.generate_data

failed=()
for fwk in "${FRAMEWORKS[@]}"; do
    echo ">>> running $fwk (timeout=${TIMEOUT_SECS}s)"
    if ! timeout --foreground "$TIMEOUT_SECS" \
            docker compose up --abort-on-container-exit --exit-code-from "$fwk" "$fwk"; then
        ec=$?
        echo ">>> $fwk failed or timed out (exit=$ec)"
        failed+=("$fwk")
    fi
    # Stop the runner; engine sidecars (clickhouse, postgres) stay up across
    # iterations - cheap to leave running, expensive to restart per-framework.
    docker compose stop "$fwk" || true
    docker compose rm -f "$fwk" || true
done

echo ">>> rendering report"
docker compose run --rm orchestrator \
    python -m distributed_data_libs.report

echo ">>> tearing down sidecars"
docker compose --profile bench down || true

if (( ${#failed[@]} > 0 )); then
    echo ">>> FAILED frameworks: ${failed[*]}"
    exit 1
fi
