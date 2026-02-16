#!/usr/bin/env python3
"""rebuild_weather_app.py
Self-extracting bootstrapper for the Niceness Weather App.

When run it will:

1. Create (if missing) a Python virtual environment in ./venv
2. Install required packages into that environment
3. Create the project directory structure under ./niceness_app
4. Write the core application modules from embedded source strings
5. Optionally launch the GUI with --run

Usage:
    python rebuild_weather_app.py        # just build
    python rebuild_weather_app.py --run  # build + run the app

Requirements:
    * Python ≥3.10 with venv module

"""

import subprocess
import sys
import os
import shutil
import venv
from pathlib import Path
import argparse
import textwrap

HERE = Path(__file__).parent.resolve()
VENV_DIR = HERE / "venv"
APP_DIR = HERE / "niceness_app"

REQUIRED_PACKAGES = [
    "PyQt6>=6.6.1",
    "pandas",
    "numpy",
    "plotly",
    "requests",
]

MODULES = {
    "__init__.py": """
# Niceness Weather App package marker
__all__ = []
""",

    "weather_api.py": """
import requests

CITY_COORDS = {
    "Honolulu":        (21.3069, -157.8583),
    "Los Angeles":     (34.0522, -118.2437),
    "New York":        (40.7128, -74.0060),
    "London":          (51.5074, -0.1278),
    "Tenerife":        (28.2916, -16.6291),
    "Milos":           (36.7260, 24.4443),
}

def fetch_current_weather(lat, lon):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}&current_weather=true&temperature_unit=fahrenheit"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data["current_weather"]

def get_all_cities():
    results = []
    for city, (lat, lon) in CITY_COORDS.items():
        cw = fetch_current_weather(lat, lon)
        results.append({
            "city": city,
            "temp_f": cw["temperature"],
            "weathercode": cw["weathercode"],
        })
    return results
""",

    "niceness.py": """
# Simple niceness score: 100 - |temp - 75| * 2 - rain_penalty

def score(row):
    temp_penalty = abs(row["temp_f"] - 75) * 2
    rain_penalty = 30 if row["weathercode"] >= 60 else 0  # crude check
    return max(0, 100 - temp_penalty - rain_penalty)
""",

    "gui.py": """
import sys
from PyQt6.QtWidgets import QApplication, QTableWidget, QTableWidgetItem, QWidget, QVBoxLayout
from PyQt6.QtGui import QColor
from niceness_app.weather_api import get_all_cities
from niceness_app.niceness import score

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Niceness Weather App")
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["City", "Temp (°F)", "Niceness"])
        layout.addWidget(self.table)
        self.refresh()

    def refresh(self):
        data = get_all_cities()
        data.sort(key=lambda d: score(d), reverse=True)
        self.table.setRowCount(len(data))
        for r, row in enumerate(data):
            niceness = score(row)
            self.table.setItem(r, 0, QTableWidgetItem(row["city"]))
            self.table.setItem(r, 1, QTableWidgetItem(f"{row['temp_f']:.1f}"))
            niceness_item = QTableWidgetItem(f"{niceness:.0f}")
            # color row background based on niceness
            g = int(255 * niceness / 100)
            niceness_item.setBackground(QColor(255 - g, g, 0, 40))
            self.table.setItem(r, 2, niceness_item)


def run():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(400, 300)
    w.show()
    sys.exit(app.exec())
""",

    "run_app.py": """
from niceness_app.gui import run

if __name__ == "__main__":
    run()
"""
}

def ensure_venv():
    if VENV_DIR.exists():
        print("[✓] Virtual environment already exists.")
        return
    print("[+] Creating virtual environment...")
    venv.create(VENV_DIR, with_pip=True)
    print("[✓] Virtual environment created at", VENV_DIR)

def pip_install():
    print("[+] Installing required packages...")
    pip_exe = VENV_DIR / ("Scripts" if os.name == "nt" else "bin") / "pip"
    subprocess.check_call([str(pip_exe), "install", "--upgrade", "pip"])
    subprocess.check_call([str(pip_exe), "install", *REQUIRED_PACKAGES])
    print("[✓] Packages installed.")

def write_modules():
    print("[+] Writing application files...")
    APP_DIR.mkdir(exist_ok=True)
    for filename, source in MODULES.items():
        target = APP_DIR / filename
        target.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
        print("    -", target.relative_to(HERE))
    print("[✓] Application files ready.")

def parse_args():
    p = argparse.ArgumentParser(description="Rebuild Niceness Weather App")
    p.add_argument("--run", action="store_true", help="Launch app after building")
    return p.parse_args()

def main():
    args = parse_args()
    ensure_venv()
    pip_install()
    write_modules()
    print("\n[✓] Build complete.")
    print(f"To run the app: source {VENV_DIR}/bin/activate && python -m niceness_app.run_app")
    if args.run:
        python_exe = VENV_DIR / ("Scripts" if os.name == "nt" else "bin") / "python"
        subprocess.check_call([str(python_exe), "-m", "niceness_app.run_app"])

if __name__ == "__main__":
    main()
