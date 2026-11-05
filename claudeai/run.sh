#!/usr/bin/env bash
# Start the distributed aaiclick backend (ClickHouse :8123 + PostgreSQL :5432 +
# migrations) that the distributed example projects expect. Delegates to
# claudeai/setup_aaiclick, which is probe-first: installs what is missing,
# restarts what is stopped, leaves running servers alone.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=claudeai/_guard.sh
. "$ROOT/claudeai/_guard.sh"

# Same defaults as the distributed <name>.sh runners.
export AAICLICK_CH_URL="${AAICLICK_CH_URL:-clickhouse://default:benchmark@localhost:8123/default}"
export AAICLICK_SQL_URL="${AAICLICK_SQL_URL:-postgresql+asyncpg://aaiclick:secret@localhost:5432/aaiclick}"

exec "$ROOT/claudeai/setup_aaiclick" "$@"
