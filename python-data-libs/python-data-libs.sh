#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

PYTHON="${PYTHON:-uv run python}"
$PYTHON -m python_data_libs
