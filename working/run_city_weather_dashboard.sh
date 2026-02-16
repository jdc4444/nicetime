#!/bin/zsh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"
source "$PROJECT_ROOT/.venv_sun/bin/activate"
python city_weather_dashboard.py --db "$SCRIPT_DIR/weather_data_v2.db" --catalog "$SCRIPT_DIR/all_city_data.json" --api-log "$SCRIPT_DIR/sync_api_calls.ndjson" --host 127.0.0.1 --port 8791
