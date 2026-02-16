#!/bin/zsh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DB_PATH="${1:-$SCRIPT_DIR/weather_data_v2.db}"
HOST="${SYNC_DASH_HOST:-127.0.0.1}"
PORT="${SYNC_DASH_PORT:-8787}"
API_LOG="${SYNC_DASH_API_LOG:-$SCRIPT_DIR/sync_api_calls.ndjson}"

python3 "$SCRIPT_DIR/sync_dashboard.py" --db "$DB_PATH" --host "$HOST" --port "$PORT" --api-log "$API_LOG"
