#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

FRAMEWORKS=("aaiclick" "spark" "dask" "ray")

mkdir -p data
rm -f data/results-*.json

echo ">>> building images"
docker compose --profile tools --profile bench build

echo ">>> generating raw.parquet"
docker compose run --rm orchestrator \
    python -m distributed_data_libs.generate_data

for fwk in "${FRAMEWORKS[@]}"; do
    echo ">>> running $fwk"
    docker compose up --abort-on-container-exit --exit-code-from "$fwk" "$fwk"
    docker compose down "$fwk" || true
done

echo ">>> rendering report"
docker compose run --rm orchestrator \
    python -m distributed_data_libs.report
