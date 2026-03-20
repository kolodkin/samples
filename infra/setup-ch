#!/usr/bin/env bash
set -euo pipefail

# Install and start ClickHouse server (data in default /var/lib/clickhouse or /tmp)

echo "Installing ClickHouse..."
if ! command -v clickhouse &>/dev/null; then
    curl https://clickhouse.com/ | sh
    ./clickhouse install --noninteractive 2>/dev/null || true
    rm -f clickhouse
fi

echo "Starting ClickHouse server in background..."
# Run from /tmp so ClickHouse default data path doesn't pollute the repo
(cd /tmp && clickhouse server --daemon)

# Wait for server to be ready
echo "Waiting for ClickHouse to start..."
for i in {1..30}; do
    if clickhouse client --query "SELECT 1" &>/dev/null; then
        echo "ClickHouse is ready!"
        exit 0
    fi
    sleep 1
done

echo "ERROR: ClickHouse failed to start within 30 seconds"
exit 1
