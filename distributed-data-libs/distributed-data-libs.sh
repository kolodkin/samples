#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

FRAMEWORKS=("aaiclick" "spark" "dask" "ray")

# Per-framework cap. A hung framework otherwise burns the whole CI budget
# (we hit GH Actions' 6-hour ceiling once on a Dask sort deadlock).
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"

mkdir -p data
rm -f data/results-*.json
rm -rf data/ray-logs 2>/dev/null || true
mkdir -p data/ray-logs

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
        # Dump sidecar logs - `compose up <runner>` only streams the
        # runner's stdout, so when the runner fails because of a sidecar
        # (e.g. ray-head's dashboard agent not registering) we'd otherwise
        # have nothing to debug from.
        for svc in ray-head spark-server dask-scheduler dask-worker; do
            if docker compose ps -a --services 2>/dev/null | grep -qx "$svc"; then
                echo ">>> --- $svc logs (tail) ---"
                docker compose logs --tail=200 --no-color "$svc" 2>&1 || true
                echo ">>> --- end $svc logs ---"
            fi
        done
        # ray-head writes its real diagnostics (dashboard, dashboard_agent,
        # raylet, gcs_server) to files inside /tmp/ray/session_*/logs/.
        # docker compose logs only captures stdout of `ray start --block`
        # which is nearly empty, so dump the in-session log files too.
        if [ "$fwk" = "ray" ] && [ -d data/ray-logs ]; then
            echo ">>> --- ray-head /tmp/ray internal logs ---"
            find data/ray-logs -maxdepth 4 -type f \
                \( -name 'dashboard*.log' -o -name 'dashboard*.err' \
                -o -name 'raylet.err' -o -name 'raylet.out' \
                -o -name 'gcs_server.err' -o -name 'monitor.err' \) 2>/dev/null \
                | while read -r f; do
                    echo "===== $f ====="
                    tail -150 "$f" 2>/dev/null || true
                done
            echo ">>> --- end ray-head internal logs ---"
        fi
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
