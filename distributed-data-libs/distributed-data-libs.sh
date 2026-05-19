#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

FRAMEWORKS=("aaiclick" "spark" "dask" "ray")

# Per-framework cap. A hung framework otherwise burns the whole CI budget
# (we hit GH Actions' 6-hour ceiling once on a Dask sort deadlock).
TIMEOUT_SECS="${TIMEOUT_SECS:-900}"

mkdir -p data data/ray-logs
rm -f data/results-*.json
rm -rf data/ray-logs/*

echo ">>> building images"
docker compose --profile tools --profile bench build

echo ">>> generating raw.parquet"
docker compose run --rm orchestrator \
    python -m distributed_data_libs.generate_data

failed=()
for fwk in "${FRAMEWORKS[@]}"; do
    # Ray Data's shuffle-heavy ops (groupby on 10M rows in a single
    # 4-CPU node) take ~60s each. Total Ray runtime lands around 12-15
    # minutes vs <1 minute for the others; bump its budget so a slow
    # framework doesn't get killed mid-op and reported as a hang.
    case "$fwk" in
        ray) fwk_timeout="${RAY_TIMEOUT_SECS:-1500}" ;;
        *)   fwk_timeout="$TIMEOUT_SECS" ;;
    esac
    # bench_ray.py's internal deadline (RAY_JOB_DEADLINE_S in compose)
    # must stay 100s under fwk_timeout so the client can stop_job and
    # print logs before the outer `timeout` SIGTERMs the runner.
    export RAY_JOB_DEADLINE_S=$((fwk_timeout - 100))
    echo ">>> running $fwk (timeout=${fwk_timeout}s)"
    if ! timeout --foreground "$fwk_timeout" \
            docker compose up --abort-on-container-exit --exit-code-from "$fwk" "$fwk"; then
        ec=$?
        echo ">>> $fwk failed or timed out (exit=$ec)"
        # `compose up <runner>` only streams the runner's stdout, so on
        # failure we dump the sidecars for this framework explicitly.
        case "$fwk" in
            ray)   sidecars=(ray-head) ;;
            spark) sidecars=(spark-server) ;;
            dask)  sidecars=(dask-scheduler dask-worker) ;;
            *)     sidecars=() ;;
        esac
        for svc in "${sidecars[@]}"; do
            echo ">>> --- $svc logs (tail) ---"
            docker compose logs --tail=200 --no-color "$svc" 2>&1 || true
            echo ">>> --- end $svc logs ---"
        done
        # ray-head's real diagnostics (dashboard agent, raylet,
        # gcs_server) live in files under /tmp/ray/session_*/logs/, not
        # the container's stdout, so dump them from the bind-mount.
        if [ "$fwk" = "ray" ]; then
            echo ">>> --- ray-head /tmp/ray internal logs ---"
            find data/ray-logs -maxdepth 4 -type f \
                \( -name 'dashboard*.log' -o -name 'dashboard*.err' \
                -o -name 'raylet.*' -o -name 'gcs_server.*' \
                -o -name 'monitor.*' -o -name 'job-driver-*.log' \) \
                -print -exec tail -150 {} \;
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
