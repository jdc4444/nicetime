#!/bin/zsh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"
source "$PROJECT_ROOT/.venv_sun/bin/activate"

if [[ -z "${VISUAL_CROSSING_API_KEY:-}" && -f ".visualcrossing_key" ]]; then
  export VISUAL_CROSSING_API_KEY="$(tr -d '\n\r' < .visualcrossing_key)"
fi

python run_catalog_backfill.py "$@"
