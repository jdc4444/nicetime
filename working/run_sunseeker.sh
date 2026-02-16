#!/bin/zsh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"
source "$PROJECT_ROOT/.venv_sun/bin/activate"

# Visual Crossing API key (local file override, then existing env var)
if [[ -z "${VISUAL_CROSSING_API_KEY:-}" && -f ".visualcrossing_key" ]]; then
  export VISUAL_CROSSING_API_KEY="$(tr -d '\n\r' < .visualcrossing_key)"
fi

# Ensure PyQt6 can locate the macOS cocoa platform plugin/frameworks.
QT_BASE="$VIRTUAL_ENV/lib/python3.11/site-packages/PyQt6/Qt6"
unset QT_PLUGIN_PATH QT_QPA_PLATFORM_PLUGIN_PATH DYLD_FRAMEWORK_PATH
export QT_PLUGIN_PATH="$QT_BASE/plugins"
export QT_QPA_PLATFORM_PLUGIN_PATH="$QT_BASE/plugins/platforms"
export DYLD_FRAMEWORK_PATH="$QT_BASE/lib"

python sunseeker/sunseeker.py
