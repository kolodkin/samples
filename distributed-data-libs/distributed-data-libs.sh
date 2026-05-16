#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# Logical framework name -> compose service name. They differ for postgres
# because the engine service is "postgres" and the runner is "postgres-runner".
FRAMEWORKS=("aaiclick" "spark" "dask" "ray" "postgres")
declare -A SERVICE=(
    [aaiclick]=aaiclick
    [spark]=spark
    [dask]=dask
    [ray]=ray
    [postgres]=postgres-runner
)

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
    svc="${SERVICE[$fwk]}"
    echo ">>> running $fwk (service=$svc, timeout=${TIMEOUT_SECS}s)"
    if ! timeout --foreground "$TIMEOUT_SECS" \
            docker compose up --abort-on-container-exit --exit-code-from "$svc" "$svc"; then
        ec=$?
        echo ">>> $fwk failed or timed out (exit=$ec)"
        failed+=("$fwk")
    fi
    # Stop just the runner; engine sidecars stay up across runs (cheap to
    # leave running, expensive to restart for each framework).
    docker compose stop "$svc" || true
    docker compose rm -f "$svc" || true
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
