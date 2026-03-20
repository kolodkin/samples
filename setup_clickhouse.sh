#!/usr/bin/env bash
set -euo pipefail

# Install and start ClickHouse server

echo "Installing ClickHouse..."
curl https://clickhouse.com/ | sh

echo "Starting ClickHouse server in background..."
./clickhouse server --daemon

# Wait for server to be ready
echo "Waiting for ClickHouse to start..."
for i in {1..30}; do
    if ./clickhouse client --query "SELECT 1" &>/dev/null; then
        echo "ClickHouse is ready!"
        exit 0
    fi
    sleep 1
done

echo "ERROR: ClickHouse failed to start within 30 seconds"
exit 1
