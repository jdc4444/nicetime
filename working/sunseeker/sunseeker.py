import sys
import os
import sqlite3
import requests
import pandas as pd
import pickle
import time
import uuid
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import traceback
import threading
import json
import fcntl

from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QLabel, QDialog, QProgressBar, QPushButton, QLineEdit, QHBoxLayout,
    QMessageBox, QCompleter, QScrollArea, QGroupBox, QAbstractScrollArea, QFormLayout, QSpinBox, QDoubleSpinBox
)
from PyQt6.QtCore import Qt, pyqtSlot, QCoreApplication
from PyQt6.QtGui import QPalette, QColor, QBrush, QCursor, QFont

DATABASE = "weather_data_v2.db"
CACHE_FILE = "forecast_cache.pkl"
CACHE_MAX_AGE = timedelta(hours=1)
ALL_CITIES_UI_CACHE_FILE = "all_cities_ui_cache.pkl"
ALL_CITIES_UI_CACHE_MAX_AGE = timedelta(hours=24)
ALLCOUNTRIES_FILE = "allcountries.txt"
SYNC_LOG_FILE = "sync_runs.log"
API_CALL_LOG_FILE = "sync_api_calls.ndjson"
RUN_LOCK_FILE = "weather_data_v2.sync.lock"
VISUAL_CROSSING_KEY = os.environ.get("VISUAL_CROSSING_API_KEY", "").strip()
WEATHER_PROVIDER = "visualcrossing"
VC_STATE_LOCK = threading.Lock()
VC_DISABLED_UNTIL = None
VC_NEXT_ALLOWED_AT = None
VC_MIN_INTERVAL_SEC = float(os.environ.get("VC_MIN_INTERVAL_SEC", "0.75"))
VC_MAX_WORKERS = int(os.environ.get("VC_MAX_WORKERS", "1"))
ESTIMATED_WINDOW_DAYS = int(os.environ.get("ESTIMATED_WINDOW_DAYS", "365"))
WEATHER_COLUMNS = [
    "city", "date", "tmax_c", "tmin_c", "tavg_c", "feelslike_max_c", "feelslike_min_c", "feelslike_c", "dewpoint_c",
    "humidity_pct", "cloudcover_pct", "visibility_km", "precip_mm", "precip_prob_pct", "precip_cover_pct", "precip_type",
    "snow_mm", "snowdepth_mm", "windspeed_kph", "windgust_kph", "winddir_deg", "pressure_mb", "solarradiation_wm2",
    "solarenergy_mj_m2", "uvindex", "moonphase", "conditions_text", "icon", "description_text", "source_provider",
    "stations_text", "severerisk", "weathercode", "sunrise", "sunset", "data_source", "updated_at",
]

CITY_COUNTRY = {
    "Honolulu": "Honolulu, USA",
    "Todos Santos": "Todos Santos, Mexico",
    "Tenerife": "Tenerife, Spain",
    "Los Angeles": "Los Angeles, USA",
    "Medellin": "Medellin, Colombia",
    "Mexico City": "Mexico City, Mexico",
    "Rio de Janeiro": "Rio de Janeiro, Brazil",
    "Fortaleza": "Fortaleza, Brazil",
    "Abu Dhabi": "Abu Dhabi, UAE",
    "Las Vegas": "Las Vegas, USA",
    "Tucson": "Tucson, USA",
    "Buenos Aires": "Buenos Aires, Argentina",
    "Sydney": "Sydney, Australia",
    "Sao Paolo": "Sao Paolo, Brazil",
    "Berlin": "Berlin, Germany",
    "Copenhagen": "Copenhagen, Denmark",
    "Santa Fe": "Santa Fe, USA",
    "Amsterdam": "Amsterdam, Netherlands",
    "New York": "New York, USA",
    "London": "London, UK",
    "Tokyo": "Tokyo, Japan",
    "Barcelona": "Barcelona, Spain",
    "Athens": "Athens, Greece",
    "Valencia": "Valencia, Spain",
    "Shanghai": "Shanghai, China",
    "Austin": "Austin, USA",
    "Milos": "Milos, Greece",
    "Santiago": "Santiago, Chile",
    "Lisbon": "Lisbon, Portugal",
    "El Paso": "El Paso, USA",
    "Palm Springs": "Palm Springs, USA"
}

CITY_COORDS = {
    "Honolulu":        (21.3069, -157.8583),
    "Todos Santos":    (23.4469, -110.2231),
    "Tenerife":        (28.2916, -16.6291),
    "Los Angeles":     (34.0522, -118.2437),
    "Medellin":        (6.2442, -75.5812),
    "Mexico City":     (19.4326, -99.1332),
    "Rio de Janeiro":  (-22.9068, -43.1729),
    "Fortaleza":       (-3.7319, -38.5267),
    "Abu Dhabi":       (24.4539, 54.3773),
    "Las Vegas":       (36.1699, -115.1398),
    "Tucson":          (32.2226, -110.9747),
    "Buenos Aires":    (-34.6037, -58.3816),
    "Sydney":          (-33.8688, 151.2093),
    "Sao Paolo":       (-23.5505, -46.6333),
    "Berlin":          (52.5200, 13.4050),
    "Copenhagen":      (55.6761, 12.5683),
    "Santa Fe":        (35.6870, -105.9378),
    "Amsterdam":       (52.3676, 4.9041),
    "New York":        (40.7128, -74.0060),
    "London":          (51.5074, -0.1278),
    "Tokyo":           (35.6762, 139.6503),
    "Barcelona":       (41.3851, 2.1734),
    "Athens":          (37.9838, 23.7275),
    "Valencia":        (39.4699, -0.3763),
    "Shanghai":        (31.2304, 121.4737),
    "Austin":          (30.2672, -97.7431),
    "Milos":           (36.7260, 24.4443),
    "Santiago":        (-33.4489, -70.6693),
    "Lisbon":          (38.7223, -9.1393),
    "El Paso":         (31.7619, -106.4850),
    "Palm Springs":    (33.8303, -116.5453)
}

ALL_CITIES = {}
if os.path.exists("allcountries.txt"):
    import csv
    with open("allcountries.txt", "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if len(row) < 11:
                continue
            country_code = row[0].strip()
            place_name = row[2].strip()
            if not place_name:
                continue
            lat_str = row[9].strip() if len(row) > 9 else ""
            lon_str = row[10].strip() if len(row) > 10 else ""
            if not lat_str or not lon_str:
                continue
            try:
                lat = float(lat_str)
                lon = float(lon_str)
            except ValueError:
                continue
            key = f"{place_name}, {country_code}"
            if key not in CITY_COORDS:
                ALL_CITIES[key] = (lat, lon)

def get_estimated_window_dates():
    """
    Rolling one-year expected-data window starting today (UTC), inclusive.
    Default is 365 days including today.
    """
    start_dt = datetime.now(timezone.utc).date()
    span = max(1, ESTIMATED_WINDOW_DAYS)
    end_dt = start_dt + timedelta(days=span - 1)
    return start_dt.isoformat(), end_dt.isoformat()

START_DATE, END_DATE = get_estimated_window_dates()
SUNNY_CODES = [0,1,2]

ZIP_CITIES = {}
if os.path.exists("ziplist.txt"):
    with open("ziplist.txt", "r", encoding="utf-8") as f:
        next(f)  # Skip header if present
        for line in f:
            try:
                city, country, continent, zipcode = line.strip().split(',')
                key = f"{city}, {country}"
                ZIP_CITIES[key] = None
            except ValueError:
                continue

def c_to_f(c):
    return (c * 9/5) + 32

def append_sync_log(message: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {message}"
    print(line)
    try:
        with open(SYNC_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def _scrub_api_key(text: str) -> str:
    if not text:
        return text
    if VISUAL_CROSSING_KEY:
        return text.replace(VISUAL_CROSSING_KEY, "***")
    return text

def append_api_call_log(payload: dict):
    entry = dict(payload or {})
    entry["ts"] = datetime.now(timezone.utc).isoformat()
    for k in ("url", "error"):
        if k in entry and isinstance(entry[k], str):
            entry[k] = _scrub_api_key(entry[k])
    try:
        with open(API_CALL_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _column_exists(conn, table_name: str, column_name: str) -> bool:
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name})")
    return any(row[1] == column_name for row in c.fetchall())

def _ensure_column(conn, table_name: str, column_name: str, col_type: str):
    if not _column_exists(conn, table_name, column_name):
        c = conn.cursor()
        c.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {col_type}")

def _source_priority(source: str) -> int:
    # Forecast supersedes estimated.
    return 2 if source == "forecast" else 1

def _create_weather_table(conn, table_name: str):
    c = conn.cursor()
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            city TEXT,
            date TEXT,
            tmax_c REAL,
            tmin_c REAL,
            weathercode INT,
            sunrise TEXT,
            sunset TEXT,
            PRIMARY KEY (city, date)
        )
        """
    )
    _ensure_column(conn, table_name, "data_source", "TEXT DEFAULT 'estimated'")
    _ensure_column(conn, table_name, "updated_at", "TEXT")
    _ensure_column(conn, table_name, "tavg_c", "REAL")
    _ensure_column(conn, table_name, "feelslike_max_c", "REAL")
    _ensure_column(conn, table_name, "feelslike_min_c", "REAL")
    _ensure_column(conn, table_name, "feelslike_c", "REAL")
    _ensure_column(conn, table_name, "dewpoint_c", "REAL")
    _ensure_column(conn, table_name, "humidity_pct", "REAL")
    _ensure_column(conn, table_name, "cloudcover_pct", "REAL")
    _ensure_column(conn, table_name, "visibility_km", "REAL")
    _ensure_column(conn, table_name, "precip_mm", "REAL")
    _ensure_column(conn, table_name, "precip_prob_pct", "REAL")
    _ensure_column(conn, table_name, "precip_cover_pct", "REAL")
    _ensure_column(conn, table_name, "precip_type", "TEXT")
    _ensure_column(conn, table_name, "snow_mm", "REAL")
    _ensure_column(conn, table_name, "snowdepth_mm", "REAL")
    _ensure_column(conn, table_name, "windspeed_kph", "REAL")
    _ensure_column(conn, table_name, "windgust_kph", "REAL")
    _ensure_column(conn, table_name, "winddir_deg", "REAL")
    _ensure_column(conn, table_name, "pressure_mb", "REAL")
    _ensure_column(conn, table_name, "solarradiation_wm2", "REAL")
    _ensure_column(conn, table_name, "solarenergy_mj_m2", "REAL")
    _ensure_column(conn, table_name, "uvindex", "REAL")
    _ensure_column(conn, table_name, "moonphase", "REAL")
    _ensure_column(conn, table_name, "conditions_text", "TEXT")
    _ensure_column(conn, table_name, "icon", "TEXT")
    _ensure_column(conn, table_name, "description_text", "TEXT")
    _ensure_column(conn, table_name, "source_provider", "TEXT")
    _ensure_column(conn, table_name, "stations_text", "TEXT")
    _ensure_column(conn, table_name, "severerisk", "REAL")

def _upsert_weather_row(conn, table_name: str, values: dict):
    cols = ", ".join(WEATHER_COLUMNS)
    placeholders = ", ".join(["?"] * len(WEATHER_COLUMNS))
    conn.execute(
        f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})",
        tuple(values.get(k) for k in WEATHER_COLUMNS),
    )

def _vc_gate():
    """
    Global Visual Crossing pacing gate.
    Ensures we don't exceed plan concurrency/rate and burst into 429s.
    """
    global VC_NEXT_ALLOWED_AT
    with VC_STATE_LOCK:
        now = datetime.now(timezone.utc)
        if VC_NEXT_ALLOWED_AT and now < VC_NEXT_ALLOWED_AT:
            wait = (VC_NEXT_ALLOWED_AT - now).total_seconds()
            if wait > 0:
                time.sleep(wait)
        VC_NEXT_ALLOWED_AT = datetime.now(timezone.utc) + timedelta(seconds=max(0.0, VC_MIN_INTERVAL_SEC))

def get_db_conn(path: str = DATABASE) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def acquire_run_lock(lock_path: str):
    fh = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(f"{os.getpid()}\n")
        fh.flush()
        return fh
    except Exception:
        try:
            fh.close()
        except Exception:
            pass
        return None

def init_db():
    """Initialize all database tables"""
    conn = get_db_conn(DATABASE)
    c = conn.cursor()
    # Weather tables:
    # - daily_data_estimated: full 1-year expected baseline
    # - daily_data_forecast: short-horizon forecast snapshots by city/date
    # - daily_data: materialized "best available" (forecast overrides estimated) for legacy read paths
    _create_weather_table(conn, "daily_data")
    _create_weather_table(conn, "daily_data_estimated")
    _create_weather_table(conn, "daily_data_forecast")

    # One-time migration from legacy daily_data into split tables.
    est_count = c.execute("SELECT COUNT(*) FROM daily_data_estimated").fetchone()[0]
    fc_count = c.execute("SELECT COUNT(*) FROM daily_data_forecast").fetchone()[0]
    legacy_count = c.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0]
    if legacy_count > 0 and est_count == 0 and fc_count == 0:
        cols = ", ".join(WEATHER_COLUMNS)
        c.execute(
            f"INSERT OR IGNORE INTO daily_data_estimated ({cols}) "
            f"SELECT {cols} FROM daily_data WHERE COALESCE(data_source, 'estimated') <> 'forecast'"
        )
        c.execute(
            f"INSERT OR IGNORE INTO daily_data_forecast ({cols}) "
            f"SELECT {cols} FROM daily_data WHERE data_source='forecast'"
        )
    
    # City coordinates table
    c.execute("""
        CREATE TABLE IF NOT EXISTS city_coords (
            city TEXT PRIMARY KEY,
            lat REAL,
            lon REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sync_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sync_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            finished_at TEXT,
            status TEXT,
            total_cities INTEGER,
            historical_complete INTEGER,
            historical_missing INTEGER,
            forecast_fresh INTEGER,
            forecast_stale INTEGER,
            historical_updated INTEGER,
            forecast_updated INTEGER,
            errors INTEGER,
            notes TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sync_city_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            city TEXT,
            stage TEXT,
            status TEXT,
            message TEXT,
            ts TEXT
        )
    """)

    
    conn.commit()
    
    # Populate city_coords from allcountries.txt if needed
    if os.path.exists(ALLCOUNTRIES_FILE):
        c.execute("SELECT COUNT(*) FROM city_coords")
        if c.fetchone()[0] == 0:  # Only populate if empty
            with open(ALLCOUNTRIES_FILE, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter='\t')
                for row in reader:
                    if len(row) < 11:
                        continue
                    country_code = row[0].strip()
                    place_name = row[2].strip()
                    if not place_name:
                        continue
                    lat_str = row[9].strip()
                    lon_str = row[10].strip()
                    if not lat_str or not lon_str:
                        continue
                    try:
                        lat = float(lat_str)
                        lon = float(lon_str)
                    except ValueError:
                        continue

                    city_key = f"{place_name}, {country_code}"
                    c.execute("""
                        INSERT OR IGNORE INTO city_coords (city, lat, lon)
                        VALUES (?, ?, ?)
                    """, (city_key, lat, lon))
            conn.commit()
    
    conn.close()

def get_sync_meta(conn, key: str, default: str = "") -> str:
    c = conn.cursor()
    c.execute("SELECT value FROM sync_meta WHERE key=?", (key,))
    row = c.fetchone()
    return row[0] if row else default

def set_sync_meta(conn, key: str, value: str):
    c = conn.cursor()
    c.execute(
        "INSERT INTO sync_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()

def insert_sync_city_log(conn, run_id: str, city: str, stage: str, status: str, message: str):
    c = conn.cursor()
    c.execute(
        "INSERT INTO sync_city_log (run_id, city, stage, status, message, ts) VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, city, stage, status, message, datetime.now(timezone.utc).isoformat()),
    )

def get_city_coords(city_name):
    """Get coordinates for a city from the database"""
    conn = get_db_conn(DATABASE)
    c = conn.cursor()
    c.execute("SELECT lat, lon FROM city_coords WHERE city = ?", (city_name,))
    row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None

def have_data_for_city(conn, city):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM daily_data_estimated WHERE city=? AND date>=? AND date<=?", (city, START_DATE, END_DATE))
    count = c.fetchone()[0]
    total_days = (datetime.strptime(END_DATE, "%Y-%m-%d") - datetime.strptime(START_DATE, "%Y-%m-%d")).days + 1
    return count == total_days


def fetch_historical(lat: float, lon: float, start_date: str, end_date: str) -> Dict[str, Any]:
    url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,weathercode,sunrise,sunset",
        "timezone": "UTC"
    }
    
    max_retries = 5
    base_delay = 1  # Start with 1 second delay
    
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    time.sleep(delay)
                    continue
            raise  # Re-raise the exception if it's not a 429 or we're out of retries

def _visualcrossing_weathercode(icon: str, conditions: str, precip_prob: float) -> int:
    icon_l = (icon or "").lower()
    cond_l = (conditions or "").lower()
    if "clear" in icon_l or "sunny" in cond_l:
        return 0
    if "partly" in icon_l:
        return 1
    if "cloud" in icon_l:
        return 3
    if any(x in icon_l or x in cond_l for x in ["rain", "drizzle", "shower"]):
        return 61
    if any(x in icon_l or x in cond_l for x in ["snow", "sleet", "ice"]):
        return 71
    if any(x in icon_l or x in cond_l for x in ["thunder", "storm"]):
        return 95
    if "fog" in icon_l or "fog" in cond_l:
        return 45
    if precip_prob is not None and precip_prob >= 50:
        return 61
    return 3

def fetch_visualcrossing_estimated_window(lat: float, lon: float, start_date: str, end_date: str, city: str = "") -> Dict[str, Any]:
    global VC_DISABLED_UNTIL
    if not VISUAL_CROSSING_KEY:
        raise RuntimeError("VISUAL_CROSSING_API_KEY is not set")
    now = datetime.now(timezone.utc)
    with VC_STATE_LOCK:
        if VC_DISABLED_UNTIL is not None and now < VC_DISABLED_UNTIL:
            raise RuntimeError(f"Visual Crossing temporarily disabled until {VC_DISABLED_UNTIL.isoformat()}")
    location = f"{lat},{lon}"
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"
    params = {
        "unitGroup": "metric",
        "include": "days",
        "key": VISUAL_CROSSING_KEY,
        "contentType": "json",
    }
    _vc_gate()
    r = requests.get(url, params=params, timeout=60)
    called_url = r.url if hasattr(r, "url") else url
    if r.status_code == 429:
        with VC_STATE_LOCK:
            VC_DISABLED_UNTIL = datetime.now(timezone.utc) + timedelta(hours=6)
        append_api_call_log({
            "city": city,
            "kind": "estimated_window",
            "provider": WEATHER_PROVIDER,
            "status_code": 429,
            "ok": False,
            "url": called_url,
            "start_date": start_date,
            "end_date": end_date,
            "lat": lat,
            "lon": lon,
            "records": 0,
            "error": "rate_limited",
        })
        raise requests.HTTPError(
            f"429 rate limited; disabling VC for this run until {VC_DISABLED_UNTIL.isoformat()}",
            response=r,
        )
    r.raise_for_status()
    data = r.json()
    days = data.get("days", []) if isinstance(data, dict) else []
    sample_tmax = None
    sample_tmin = None
    sample_date = None
    if days:
        sample = days[0]
        sample_tmax = sample.get("tempmax")
        sample_tmin = sample.get("tempmin")
        sample_date = sample.get("datetime")
        sample_precip = sample.get("precip")
        sample_precipprob = sample.get("precipprob")
        sample_solar = sample.get("solarradiation")
    else:
        sample_precip = None
        sample_precipprob = None
        sample_solar = None
    append_api_call_log({
        "city": city,
        "kind": "estimated_window",
        "provider": WEATHER_PROVIDER,
        "status_code": r.status_code,
        "ok": True,
        "url": called_url,
        "start_date": start_date,
        "end_date": end_date,
        "lat": lat,
        "lon": lon,
        "records": len(days),
        "sample_tmax_c": sample_tmax,
        "sample_tmin_c": sample_tmin,
        "sample_date": sample_date,
        "sample_precip_mm": sample_precip,
        "sample_precip_prob_pct": sample_precipprob,
        "sample_solarradiation_wm2": sample_solar,
    })
    return data

def process_visualcrossing_days(days: list[dict]) -> pd.DataFrame:
    rows = []
    for d in days:
        dt = pd.to_datetime(d.get("datetime"))
        sunrise_epoch = d.get("sunriseEpoch")
        sunset_epoch = d.get("sunsetEpoch")
        sunrise = pd.to_datetime(sunrise_epoch, unit="s", utc=True) if sunrise_epoch is not None else pd.NaT
        sunset = pd.to_datetime(sunset_epoch, unit="s", utc=True) if sunset_epoch is not None else pd.NaT
        tmax_c = d.get("tempmax")
        tmin_c = d.get("tempmin")
        icon = d.get("icon", "")
        conditions = d.get("conditions", "")
        precip_prob = d.get("precipprob")
        code = _visualcrossing_weathercode(icon, conditions, precip_prob)
        rows.append(
            {
                "time": dt,
                "tmax_c": tmax_c,
                "tmin_c": tmin_c,
                "tavg_c": d.get("temp"),
                "feelslike_max_c": d.get("feelslikemax"),
                "feelslike_min_c": d.get("feelslikemin"),
                "feelslike_c": d.get("feelslike"),
                "dewpoint_c": d.get("dew"),
                "humidity_pct": d.get("humidity"),
                "cloudcover_pct": d.get("cloudcover"),
                "visibility_km": d.get("visibility"),
                "precip_mm": d.get("precip"),
                "precip_prob_pct": d.get("precipprob"),
                "precip_cover_pct": d.get("precipcover"),
                "precip_type": ",".join(d.get("preciptype", [])) if isinstance(d.get("preciptype"), list) else (d.get("preciptype") or ""),
                "snow_mm": d.get("snow"),
                "snowdepth_mm": d.get("snowdepth"),
                "windspeed_kph": d.get("windspeed"),
                "windgust_kph": d.get("windgust"),
                "winddir_deg": d.get("winddir"),
                "pressure_mb": d.get("pressure"),
                "solarradiation_wm2": d.get("solarradiation"),
                "solarenergy_mj_m2": d.get("solarenergy"),
                "uvindex": d.get("uvindex"),
                "moonphase": d.get("moonphase"),
                "conditions_text": d.get("conditions", ""),
                "icon": d.get("icon", ""),
                "description_text": d.get("description", ""),
                "source_provider": d.get("source", ""),
                "stations_text": ",".join(d.get("stations", [])) if isinstance(d.get("stations"), list) else (d.get("stations") or ""),
                "severerisk": d.get("severerisk"),
                "weathercode": code,
                "sunrise": sunrise,
                "sunset": sunset,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], utc=True)
    return df

def fetch_estimated_history(lat: float, lon: float, start_date: str, end_date: str, city: str = "") -> pd.DataFrame:
    # Visual Crossing only.
    if VISUAL_CROSSING_KEY:
        try:
            vc = fetch_visualcrossing_estimated_window(lat, lon, start_date, end_date, city=city)
            days = vc.get("days", [])
            if days:
                return process_visualcrossing_days(days)
        except Exception as e:
            msg = str(e)
            if "temporarily disabled until" in msg:
                # Avoid flooding logs once VC is known unavailable.
                pass
            else:
                append_sync_log(f"[estimated] Visual Crossing failed for {lat},{lon}: {e}.")
                if "429 rate limited" in msg:
                    append_sync_log("[estimated] Visual Crossing quota/rate limit hit. Remaining cities will skip estimated history this run.")
    return pd.DataFrame()

def store_data(conn, city, df, source: str = "estimated"):
    c = conn.cursor()
    split_table = "daily_data_forecast" if source == "forecast" else "daily_data_estimated"
    for _, row in df.iterrows():
        day = row["time"].strftime("%Y-%m-%d")
        values = {
            "city": city,
            "date": day,
            "tmax_c": row.get("tmax_c"),
            "tmin_c": row.get("tmin_c"),
            "tavg_c": row.get("tavg_c"),
            "feelslike_max_c": row.get("feelslike_max_c"),
            "feelslike_min_c": row.get("feelslike_min_c"),
            "feelslike_c": row.get("feelslike_c"),
            "dewpoint_c": row.get("dewpoint_c"),
            "humidity_pct": row.get("humidity_pct"),
            "cloudcover_pct": row.get("cloudcover_pct"),
            "visibility_km": row.get("visibility_km"),
            "precip_mm": row.get("precip_mm"),
            "precip_prob_pct": row.get("precip_prob_pct"),
            "precip_cover_pct": row.get("precip_cover_pct"),
            "precip_type": row.get("precip_type"),
            "snow_mm": row.get("snow_mm"),
            "snowdepth_mm": row.get("snowdepth_mm"),
            "windspeed_kph": row.get("windspeed_kph"),
            "windgust_kph": row.get("windgust_kph"),
            "winddir_deg": row.get("winddir_deg"),
            "pressure_mb": row.get("pressure_mb"),
            "solarradiation_wm2": row.get("solarradiation_wm2"),
            "solarenergy_mj_m2": row.get("solarenergy_mj_m2"),
            "uvindex": row.get("uvindex"),
            "moonphase": row.get("moonphase"),
            "conditions_text": row.get("conditions_text"),
            "icon": row.get("icon"),
            "description_text": row.get("description_text"),
            "source_provider": row.get("source_provider"),
            "stations_text": row.get("stations_text"),
            "severerisk": row.get("severerisk"),
            "weathercode": row.get("weathercode"),
            "sunrise": row.get("sunrise").isoformat() if not pd.isna(row.get("sunrise")) else None,
            "sunset": row.get("sunset").isoformat() if not pd.isna(row.get("sunset")) else None,
            "data_source": source,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        # Store immutable source rows.
        _upsert_weather_row(conn, split_table, values)

        # Update materialized best table for legacy paths.
        c.execute("SELECT data_source FROM daily_data WHERE city=? AND date=?", (city, day))
        old = c.fetchone()
        old_source = old[0] if old and old[0] else "estimated"
        if not old or _source_priority(source) >= _source_priority(old_source):
            _upsert_weather_row(conn, "daily_data", values)
    conn.commit()

def process_daily_data(daily_data: Dict[str, Any]) -> pd.DataFrame:
    df = pd.DataFrame({
        "time": daily_data["time"],
        "tmax_c": daily_data["temperature_2m_max"],
        "tmin_c": daily_data["temperature_2m_min"],
        "weathercode": daily_data["weathercode"],
        "sunrise": daily_data["sunrise"],
        "sunset": daily_data["sunset"]
    })
    df["time"] = pd.to_datetime(df["time"])
    df["sunrise"] = pd.to_datetime(df["sunrise"])
    df["sunset"] = pd.to_datetime(df["sunset"])
    return df

def process_forecast_daily_data(fore_daily: Dict[str, Any]) -> pd.DataFrame:
    def col(key, default=None):
        return fore_daily.get(key, default if default is not None else [None] * len(fore_daily.get("time", [])))

    df = pd.DataFrame({
        "time": fore_daily["time"],
        "tmax_c": fore_daily["temperature_2m_max"],
        "tmin_c": fore_daily["temperature_2m_min"],
        "tavg_c": col("temperature_2m_mean"),
        "feelslike_max_c": col("feelslike_max_c"),
        "feelslike_min_c": col("feelslike_min_c"),
        "feelslike_c": col("feelslike_c"),
        "dewpoint_c": col("dewpoint_c"),
        "humidity_pct": col("humidity_pct"),
        "cloudcover_pct": col("cloudcover_pct"),
        "visibility_km": col("visibility_km"),
        "precip_mm": col("precip_mm"),
        "precip_prob_pct": col("precip_prob_pct"),
        "precip_cover_pct": col("precip_cover_pct"),
        "precip_type": col("precip_type", [""] * len(fore_daily.get("time", []))),
        "snow_mm": col("snow_mm"),
        "snowdepth_mm": col("snowdepth_mm"),
        "windspeed_kph": col("windspeed_kph"),
        "windgust_kph": col("windgust_kph"),
        "winddir_deg": col("winddir_deg"),
        "pressure_mb": col("pressure_mb"),
        "solarradiation_wm2": col("solarradiation_wm2"),
        "solarenergy_mj_m2": col("solarenergy_mj_m2"),
        "uvindex": col("uvindex"),
        "moonphase": col("moonphase"),
        "conditions_text": col("conditions_text", [""] * len(fore_daily.get("time", []))),
        "icon": col("icon", [""] * len(fore_daily.get("time", []))),
        "description_text": col("description_text", [""] * len(fore_daily.get("time", []))),
        "source_provider": col("source_provider", [""] * len(fore_daily.get("time", []))),
        "stations_text": col("stations_text", [""] * len(fore_daily.get("time", []))),
        "severerisk": col("severerisk"),
        "weathercode": fore_daily["weathercode"],
        "sunrise": fore_daily["sunrise"],
        "sunset": fore_daily["sunset"]
    })
    df["time"] = pd.to_datetime(df["time"])
    df["sunrise"] = pd.to_datetime(df["sunrise"])
    df["sunset"] = pd.to_datetime(df["sunset"])
    return df

def load_data_from_db(conn, city):
    c = conn.cursor()
    c.execute("SELECT date,tmax_c,tmin_c,weathercode,sunrise,sunset FROM daily_data WHERE city=? AND date>=? AND date<=? ORDER BY date",
              (city, START_DATE, END_DATE))
    rows = c.fetchall()
    df = pd.DataFrame(rows, columns=["date", "tmax_c", "tmin_c", "weathercode", "sunrise", "sunset"])
    df["time"] = pd.to_datetime(df["date"])
    df["sunrise"] = pd.to_datetime(df["sunrise"])
    df["sunset"] = pd.to_datetime(df["sunset"])
    return df

def monthly_aggregates_from_db(conn, city):
    """
    Fast monthly aggregate path that avoids loading full daily rows into pandas.
    Returns the same shape as monthly_aggregates().
    """
    c = conn.cursor()
    sunny_codes = tuple(SUNNY_CODES)
    placeholders = ",".join(["?"] * len(sunny_codes))
    query = f"""
        SELECT
            CAST(SUBSTR(date, 6, 2) AS INTEGER) AS month,
            AVG(tmax_c) AS tmax_mean_c,
            AVG(tmin_c) AS tmin_mean_c,
            AVG(CASE WHEN weathercode IN ({placeholders}) THEN 1.0 ELSE 0.0 END) * 30.0 AS sunny_day_30,
            AVG((julianday(sunset) - julianday(sunrise)) * 24.0) AS day_length_hrs
        FROM daily_data
        WHERE city=? AND date>=? AND date<=?
        GROUP BY CAST(SUBSTR(date, 6, 2) AS INTEGER)
        ORDER BY month
    """
    c.execute(query, (*sunny_codes, city, START_DATE, END_DATE))
    rows = c.fetchall()
    if not rows:
        return None

    by_month = {}
    for month, tmax_mean_c, tmin_mean_c, sunny_day_30, day_length_hrs in rows:
        tmax_mean = c_to_f(tmax_mean_c) if tmax_mean_c is not None else float("nan")
        tmin_mean = c_to_f(tmin_mean_c) if tmin_mean_c is not None else float("nan")
        avg_day_f = (tmax_mean + tmin_mean) / 2 if not pd.isna(tmax_mean) and not pd.isna(tmin_mean) else float("nan")
        sunny_day = max(0.0, min(float(sunny_day_30), 30.0)) if sunny_day_30 is not None else float("nan")
        by_month[int(month)] = (avg_day_f, sunny_day, day_length_hrs, tmax_mean, tmin_mean)

    monthly_data = []
    for m in range(1, 13):
        if m in by_month:
            avg_day_f, sunny_day, day_length_hrs, tmax_mean, tmin_mean = by_month[m]
            monthly_data.append((m, avg_day_f, sunny_day, day_length_hrs, tmax_mean, tmin_mean))
        else:
            monthly_data.append((m, float("nan"), float("nan"), float("nan"), float("nan"), float("nan")))

    return pd.DataFrame(
        monthly_data,
        columns=["month", "avg_day_f", "sunny_day", "day_length_hrs", "tmax_mean", "tmin_mean"]
    )

def monthly_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    df["month"] = df["time"].dt.month
    df["tmax_f"] = c_to_f(df["tmax_c"])
    df["tmin_f"] = c_to_f(df["tmin_c"])
    df["avg_day_f"] = (df["tmax_f"] + df["tmin_f"]) / 2
    df["sunny_day"] = df["weathercode"].apply(lambda w: 1 if w in SUNNY_CODES else 0)
    df["day_length_hrs"] = (df["sunset"] - df["sunrise"]).dt.total_seconds() / 3600.0

    monthly_data = []
    for m in range(1, 13):
        mdf = df[df["month"] == m]
        if mdf.empty:
            monthly_data.append((m, float('nan'), float('nan'), float('nan'), float('nan'), float('nan'), float('nan'), float('nan'), float('nan')))
            continue

        avg_day_f_m = mdf["avg_day_f"].mean()
        tmax_mean = mdf["tmax_f"].mean()
        tmin_mean = mdf["tmin_f"].mean()

        # Normalize to an expected sunny-days-per-30-days value for this month.
        # Historical data spans multiple years, so raw sums can exceed 30.
        sunny_days_avg = mdf["sunny_day"].mean() * 30.0
        sunny_days_avg = max(0.0, min(float(sunny_days_avg), 30.0))

        day_length_avg = mdf["day_length_hrs"].mean()
        monthly_data.append((m, avg_day_f_m, sunny_days_avg, day_length_avg, tmax_mean, tmin_mean))

    monthly_df = pd.DataFrame(
        monthly_data,
        columns=["month", "avg_day_f", "sunny_day", "day_length_hrs", "tmax_mean", "tmin_mean"]
    )
    return monthly_df

def month_name(m):
    return ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][m-1]

def _fetch_visualcrossing_forecast_bundle(lat: float, lon: float, days: int = 16, city: str = ""):
    if not VISUAL_CROSSING_KEY:
        raise RuntimeError("VISUAL_CROSSING_API_KEY is not set")
    start_dt = datetime.now(timezone.utc).date()
    end_dt = start_dt + timedelta(days=max(1, days) - 1)
    location = f"{lat},{lon}"
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_dt.isoformat()}/{end_dt.isoformat()}"
    params = {
        "unitGroup": "metric",
        "include": "days,current",
        "key": VISUAL_CROSSING_KEY,
        "contentType": "json",
    }
    _vc_gate()
    r = requests.get(url, params=params, timeout=60)
    called_url = r.url if hasattr(r, "url") else url
    if r.status_code >= 400:
        append_api_call_log({
            "city": city,
            "kind": "forecast_bundle",
            "provider": WEATHER_PROVIDER,
            "status_code": r.status_code,
            "ok": False,
            "url": called_url,
            "lat": lat,
            "lon": lon,
            "records": 0,
            "error": f"http_{r.status_code}",
        })
    r.raise_for_status()
    vc = r.json()

    rows = vc.get("days", [])[:days]
    daily = {
        "time": [],
        "weathercode": [],
        "sunrise": [],
        "sunset": [],
        "temperature_2m_max": [],
        "temperature_2m_min": [],
        "temperature_2m_mean": [],
        "feelslike_max_c": [],
        "feelslike_min_c": [],
        "feelslike_c": [],
        "dewpoint_c": [],
        "humidity_pct": [],
        "cloudcover_pct": [],
        "visibility_km": [],
        "precip_mm": [],
        "precip_prob_pct": [],
        "precip_cover_pct": [],
        "precip_type": [],
        "snow_mm": [],
        "snowdepth_mm": [],
        "windspeed_kph": [],
        "windgust_kph": [],
        "winddir_deg": [],
        "pressure_mb": [],
        "solarradiation_wm2": [],
        "solarenergy_mj_m2": [],
        "uvindex": [],
        "moonphase": [],
        "conditions_text": [],
        "icon": [],
        "description_text": [],
        "source_provider": [],
        "stations_text": [],
        "severerisk": [],
    }
    for d in rows:
        daily["time"].append(str(d.get("datetime")))
        sunrise_epoch = d.get("sunriseEpoch")
        sunset_epoch = d.get("sunsetEpoch")
        sunrise_iso = pd.to_datetime(sunrise_epoch, unit="s", utc=True).isoformat() if sunrise_epoch is not None else None
        sunset_iso = pd.to_datetime(sunset_epoch, unit="s", utc=True).isoformat() if sunset_epoch is not None else None
        daily["sunrise"].append(sunrise_iso)
        daily["sunset"].append(sunset_iso)
        daily["temperature_2m_max"].append(d.get("tempmax"))
        daily["temperature_2m_min"].append(d.get("tempmin"))
        daily["temperature_2m_mean"].append(d.get("temp"))
        daily["feelslike_max_c"].append(d.get("feelslikemax"))
        daily["feelslike_min_c"].append(d.get("feelslikemin"))
        daily["feelslike_c"].append(d.get("feelslike"))
        daily["dewpoint_c"].append(d.get("dew"))
        daily["humidity_pct"].append(d.get("humidity"))
        daily["cloudcover_pct"].append(d.get("cloudcover"))
        daily["visibility_km"].append(d.get("visibility"))
        daily["precip_mm"].append(d.get("precip"))
        daily["precip_prob_pct"].append(d.get("precipprob"))
        daily["precip_cover_pct"].append(d.get("precipcover"))
        daily["precip_type"].append(",".join(d.get("preciptype", [])) if isinstance(d.get("preciptype"), list) else (d.get("preciptype") or ""))
        daily["snow_mm"].append(d.get("snow"))
        daily["snowdepth_mm"].append(d.get("snowdepth"))
        daily["windspeed_kph"].append(d.get("windspeed"))
        daily["windgust_kph"].append(d.get("windgust"))
        daily["winddir_deg"].append(d.get("winddir"))
        daily["pressure_mb"].append(d.get("pressure"))
        daily["solarradiation_wm2"].append(d.get("solarradiation"))
        daily["solarenergy_mj_m2"].append(d.get("solarenergy"))
        daily["uvindex"].append(d.get("uvindex"))
        daily["moonphase"].append(d.get("moonphase"))
        daily["conditions_text"].append(d.get("conditions", ""))
        daily["icon"].append(d.get("icon", ""))
        daily["description_text"].append(d.get("description", ""))
        daily["source_provider"].append(d.get("source", ""))
        daily["stations_text"].append(",".join(d.get("stations", [])) if isinstance(d.get("stations"), list) else (d.get("stations") or ""))
        daily["severerisk"].append(d.get("severerisk"))
        daily["weathercode"].append(_visualcrossing_weathercode(d.get("icon", ""), d.get("conditions", ""), d.get("precipprob")))

    fore_json = {
        "latitude": lat,
        "longitude": lon,
        "daily": daily,
    }
    cur = vc.get("currentConditions", {}) or {}
    cur_temp_c = cur.get("temp")
    if cur_temp_c is None and rows:
        cur_temp_c = rows[0].get("temp")
    cur_json = {
        "current_weather": {
            "temperature": cur_temp_c
        }
    }
    append_api_call_log({
        "city": city,
        "kind": "forecast_bundle",
        "provider": WEATHER_PROVIDER,
        "status_code": r.status_code,
        "ok": True,
        "url": called_url,
        "lat": lat,
        "lon": lon,
        "records": len(daily["time"]),
        "current_temp_c": cur_temp_c,
        "sample_tmax_c": daily["temperature_2m_max"][0] if daily["temperature_2m_max"] else None,
        "sample_tmin_c": daily["temperature_2m_min"][0] if daily["temperature_2m_min"] else None,
        "sample_precip_mm": daily["precip_mm"][0] if daily["precip_mm"] else None,
        "sample_precip_prob_pct": daily["precip_prob_pct"][0] if daily["precip_prob_pct"] else None,
        "sample_solarradiation_wm2": daily["solarradiation_wm2"][0] if daily["solarradiation_wm2"] else None,
    })
    return fore_json, cur_json

def fetch_current_forecast_data(lat, lon):
    fore_json, _ = _fetch_visualcrossing_forecast_bundle(lat, lon, days=16)
    return fore_json

def fetch_current(lat, lon):
    _, cur_json = _fetch_visualcrossing_forecast_bundle(lat, lon, days=16)
    return cur_json

def compute_daytime_avg_temp(tmax_f, tmin_f):
    # Approximate a daytime low temperature closer to the high.
    daytime_low_f = tmax_f - (tmax_f - tmin_f) / 4.0
    daytime_avg_f = (tmax_f + daytime_low_f) / 2.0
    return daytime_avg_f

def compute_niceness(temp_f, sunny_days, day_length_hrs):
    """
    Default niceness computation with temperature range 50F - 105F,
    including partial scoring.
    """
    if temp_f < 50 or temp_f > 105:
        temp_score = 0.0
    elif 50 <= temp_f < 70:
        temp_score = (temp_f - 50) / 20.0 * 0.5
    elif 70 <= temp_f < 75:
        temp_score = 0.5 + ((temp_f - 70) / 5.0) * 0.5
    elif 75 <= temp_f <= 85:
        temp_score = 1.0
    elif 85 < temp_f <= 90:
        temp_score = 1.0 - ((temp_f - 85) / 5.0) * 0.5
    else:  # 90 < temp_f <= 105
        temp_score = 0.5 - ((temp_f - 90) / 15.0) * 0.5

    sunny_score = max(0.0, min(sunny_days / 30.0, 1.0))
    day_length_score = max(0.0, min(day_length_hrs / 24.0, 1.0))
    sun_day_score = (sunny_score + day_length_score) / 2.0

    niceness = 0.5 * temp_score + 0.5 * sun_day_score
    return niceness

def compute_city_niceness(tmax_f, tmin_f, sunny_days, day_length_hrs):
    # Uses an approximate daytime average in the niceness calculation.
    daytime_avg_f = compute_daytime_avg_temp(tmax_f, tmin_f)
    return compute_niceness(daytime_avg_f, sunny_days, day_length_hrs)

def load_forecast_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'rb') as f:
            data = pickle.load(f)
            return data
    return {}

def save_forecast_cache(cache):
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(cache, f)

def load_all_cities_ui_cache(max_age: timedelta = ALL_CITIES_UI_CACHE_MAX_AGE):
    if not os.path.exists(ALL_CITIES_UI_CACHE_FILE):
        return None
    try:
        with open(ALL_CITIES_UI_CACHE_FILE, "rb") as f:
            payload = pickle.load(f)
        saved_at = payload.get("saved_at")
        if isinstance(saved_at, str):
            saved_at = datetime.fromisoformat(saved_at)
        if not isinstance(saved_at, datetime):
            return None
        if saved_at.tzinfo is None:
            saved_at = saved_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - saved_at > max_age:
            return None
        if not isinstance(payload.get("current_data_list"), list):
            return None
        if not isinstance(payload.get("monthly_dict"), dict):
            return None
        return payload
    except Exception:
        return None

def save_all_cities_ui_cache(current_data_list, monthly_dict):
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "current_data_list": current_data_list,
        "monthly_dict": monthly_dict,
    }
    with open(ALL_CITIES_UI_CACHE_FILE, "wb") as f:
        pickle.dump(payload, f)

def is_forecast_fresh(city: str, cache: dict, hours: int = 24) -> bool:
    """
    Returns True if 'city' forecast data in 'cache' was fetched within 'hours' hours.
    """
    now = datetime.now(timezone.utc)
    if city not in cache or 'time' not in cache[city]:
        return False
    provider = cache[city].get("provider")
    if provider != WEATHER_PROVIDER:
        return False
    last_fetch = cache[city]['time']
    return (now - last_fetch) < timedelta(hours=hours)

def build_target_city_map(forecast_cache: dict) -> dict[str, tuple[float, float]]:
    """
    Build the full sync target set.
    Priority:
    1) Existing forecast cache coordinates (typically the largest set; e.g. ~2431 cities)
    2) Hardcoded CITY_COORDS fallback entries
    """
    targets: dict[str, tuple[float, float]] = {}

    for city, payload in forecast_cache.items():
        if not isinstance(city, str) or not isinstance(payload, dict):
            continue
        fore = payload.get("fore_json", {})
        try:
            lat = float(fore.get("latitude"))
            lon = float(fore.get("longitude"))
            targets[city] = (lat, lon)
        except Exception:
            continue

    for city, latlon in CITY_COORDS.items():
        if city not in targets and isinstance(latlon, tuple) and len(latlon) == 2:
            targets[city] = latlon

    return targets

def sync_status_snapshot(conn, city_names: list[str], forecast_cache: dict):
    hist_complete = 0
    hist_missing = 0
    forecast_fresh = 0
    forecast_stale = 0
    for city in city_names:
        if have_data_for_city(conn, city):
            hist_complete += 1
        else:
            hist_missing += 1
        if is_forecast_fresh(city, forecast_cache, hours=24):
            forecast_fresh += 1
        else:
            forecast_stale += 1
    return {
        "hist_complete": hist_complete,
        "hist_missing": hist_missing,
        "forecast_fresh": forecast_fresh,
        "forecast_stale": forecast_stale,
    }

class LoadingDialog(QDialog):
    def __init__(self, max_cities):
        super().__init__()
        self.setWindowTitle("Loading Weather Data...")
        layout = QVBoxLayout()

        self.label_fetch = QLabel("Fetching historical data...")
        self.pb_fetch = QProgressBar()
        self.pb_fetch.setMaximum(max_cities)
        self.pb_fetch.setValue(0)

        self.label_process = QLabel("Processing monthly data...")
        self.pb_process = QProgressBar()
        self.pb_process.setMaximum(max_cities)
        self.pb_process.setValue(0)

        self.label_current = QLabel("Fetching current & forecast data...")
        self.pb_current = QProgressBar()
        self.pb_current.setMaximum(max_cities)
        self.pb_current.setValue(0)

        layout.addWidget(self.label_fetch)
        layout.addWidget(self.pb_fetch)
        layout.addWidget(self.label_process)
        layout.addWidget(self.pb_process)
        layout.addWidget(self.label_current)
        layout.addWidget(self.pb_current)

        self.setLayout(layout)
        self.resize(400, 200)

    def update_fetch(self, value):
        self.pb_fetch.setValue(value)

    def update_process(self, value):
        self.pb_process.setValue(value)

    def update_current(self, value):
        self.pb_current.setValue(value)

class NumericTableWidgetItem(QTableWidgetItem):
    def __init__(self, value):
        if pd.isna(value):
            self.numeric_val = float('nan')
            display_value = "N/A"
        else:
            self.numeric_val = float(value)
            display_value = f"{self.numeric_val:.4f}"
        super().__init__(display_value)

    def __lt__(self, other):
        if isinstance(other, NumericTableWidgetItem):
            return self.numeric_val < other.numeric_val
        return super().__lt__(other)

def is_nice_strict(avg_temp, sunny_days, day_length):
    return (avg_temp > 70) and (sunny_days > 12) and (day_length > 10)

def is_nice_light(avg_temp, sunny_days, day_length):
    return (avg_temp > 60) and (sunny_days > 10) and (day_length > 10)

def highlight_cell(item, avg_temp, sunny_days, day_length):
    item.setBackground(QBrush(QColor(255,255,255)))
    item.setForeground(QBrush(Qt.GlobalColor.black))

    if pd.isna(avg_temp) or pd.isna(sunny_days) or pd.isna(day_length):
        return

    if avg_temp > 90:
        item.setBackground(QBrush(QColor(255,0,0)))  # red
        item.setForeground(QBrush(QColor(255,255,255)))
    elif avg_temp < 50:
        item.setBackground(QBrush(QColor(0,0,255)))  # blue
        item.setForeground(QBrush(QColor(255,255,255)))
    elif is_nice_strict(avg_temp, sunny_days, day_length):
        # Keep foreground black so text is readable on bright yellow
        item.setBackground(QBrush(QColor(255,255,0)))  # bright yellow
        item.setForeground(QBrush(QColor(0,0,0)))
    elif is_nice_light(avg_temp, sunny_days, day_length):
        # Keep foreground black so text is readable on light yellow
        item.setBackground(QBrush(QColor(255,255,224)))  # light yellow
        item.setForeground(QBrush(QColor(0,0,0)))

def get_geo_boundaries(lat, lon):
    """
    Calls geojson-places-api on localhost:3000 with the correct path-based route:
    /lookup/:lat/:lon

    Returns a dict with continent_code, country_a2, and country_a3 if found,
    or None if the server returns 404 or no data.
    """
    base_url = "http://localhost:3000"
    # Use path parameters instead of query params
    lookup_url = f"{base_url}/lookup/{lat}/{lon}"
    try:
        r = requests.get(lookup_url)
        if r.status_code != 200:
            return None
        data = r.json()
        if data is None:
            return None
        return {
            "continent_code": data.get("continent_code"),
            "country_a2": data.get("country_a2"), 
            "country_a3": data.get("country_a3")
        }
    except Exception:
        return None

class WeatherApp(QWidget):
    def __init__(self, current_data_list, monthly_dict, all_city_data, forecast_cache):
        super().__init__()
        self.setWindowTitle("Weather Overview")

        self.current_data_list = current_data_list
        self.monthly_dict = monthly_dict
        self.all_city_data = all_city_data
        self.forecast_cache = forecast_cache
        self.current_detail_city = None
        self.default_city_detail = "New York"

        # Set default pinned city to New York
        self.pinned_city = "New York"
        self.recent_cities = []

        font = QFont()
        font.setPointSize(12)
        self.setFont(font)

        layout = QVBoxLayout()
        self.tab_widget = QTabWidget()

        # Current Tab
        self.current_tab = QWidget()
        current_layout = QVBoxLayout()
        self.current_table = self.create_current_table(self.current_data_list, self.monthly_dict)
        current_layout.addWidget(self.current_table)
        self.current_tab.setLayout(current_layout)

        self.last_sorted_column_current = None
        self.last_sort_order_current = Qt.SortOrder.DescendingOrder
        self.last_sorted_column_monthly = None
        self.last_sort_order_monthly = Qt.SortOrder.DescendingOrder

        self.current_table.horizontalHeader().sectionClicked.connect(self.on_current_header_clicked)
        self.current_table.horizontalHeader().sectionDoubleClicked.connect(self.on_current_header_double_clicked)
        self.current_table.cellDoubleClicked.connect(self.on_current_table_double_click)
        # Default sort by niceness descending
        self.current_table.sortItems(0, Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_current = 0
        self.last_sort_order_current = Qt.SortOrder.DescendingOrder

        # Monthly Tab
        self.monthly_tab = QWidget()
        monthly_layout = QVBoxLayout()
        self.monthly_table = self.create_monthly_table(self.monthly_dict)
        monthly_layout.addWidget(self.monthly_table)
        self.monthly_tab.setLayout(monthly_layout)

        self.monthly_table.horizontalHeader().sectionClicked.connect(self.on_monthly_header_clicked)
        self.monthly_table.horizontalHeader().sectionDoubleClicked.connect(self.on_monthly_header_double_clicked)
        self.monthly_table.cellDoubleClicked.connect(self.on_monthly_table_double_click)

        # Detail Tab
        self.detail_tab = QWidget()
        self.detail_layout = QVBoxLayout()
        self.detail_layout.setSpacing(20)
        self.detail_label = QLabel("Select a city in the other tabs to view details.")
        detail_font = QFont()
        detail_font.setPointSize(16)
        detail_font.setBold(True)
        self.detail_label.setFont(detail_font)
        self.detail_layout.addWidget(self.detail_label)

        self.remove_city_button = QPushButton("Remove City")
        self.remove_city_button.setEnabled(False)
        self.remove_city_button.setFont(QFont("", 14, QFont.Weight.Bold))
        self.remove_city_button.setStyleSheet("padding: 10px;")
        self.remove_city_button.clicked.connect(self.remove_current_city)

        detail_container = QWidget()
        detail_container.setLayout(self.detail_layout)
        detail_scroll = QScrollArea()
        detail_scroll.setWidget(detail_container)
        detail_scroll.setWidgetResizable(True)

        detail_main_layout = QVBoxLayout()
        detail_main_layout.addWidget(detail_scroll)
        self.detail_tab.setLayout(detail_main_layout)

        # Itinerary Tab
        self.itinerary_tab = QWidget()
        itinerary_layout = QVBoxLayout()
        
        # Add a label to display "Updated as of" and forecast info
        self.itinerary_info_label = QLabel("")
        itinerary_info_layout = QHBoxLayout()
        itinerary_info_layout.addStretch()
        itinerary_info_layout.addWidget(self.itinerary_info_label)
        itinerary_layout.addLayout(itinerary_info_layout)
        
        # Create the itinerary table
        self.itinerary_table = QTableWidget()
        self.itinerary_table.setColumnCount(11)
        itinerary_headers = ["Month"] + [f"Rank {i}" for i in range(1, 11)]
        self.itinerary_table.setHorizontalHeaderLabels(itinerary_headers)
        self.itinerary_table.setRowCount(12)
        self.itinerary_table.setAlternatingRowColors(True)
        self.itinerary_table.verticalHeader().setDefaultSectionSize(50)
        self.itinerary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.itinerary_table.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.itinerary_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.itinerary_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.itinerary_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.itinerary_table.horizontalHeader().setSectionsMovable(True)
        self.itinerary_table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
        
        itinerary_layout.addWidget(self.itinerary_table)
        self.itinerary_tab.setLayout(itinerary_layout)

        self.tab_widget.addTab(self.current_tab, "Current Weather")
        self.tab_widget.addTab(self.monthly_tab, "Monthly Calendar")
        self.tab_widget.addTab(self.detail_tab, "City Detail")
        self.tab_widget.addTab(self.itinerary_tab, "Itinerary")

        add_city_layout = QHBoxLayout()
        self.city_input = QLineEdit()
        all_keys = ["all cities", "refresh stale cities"] + list(ZIP_CITIES.keys())
        completer = QCompleter(all_keys)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.city_input.setCompleter(completer)
        self.add_city_button = QPushButton("Add City")
        self.add_city_button.clicked.connect(self.add_city)

        add_city_layout.addWidget(QLabel("Add City:"))
        add_city_layout.addWidget(self.city_input)
        add_city_layout.addWidget(self.add_city_button)

        layout.addWidget(self.tab_widget)
        layout.addLayout(add_city_layout)
        self.setLayout(layout)
        self.resize(1600, 900)

        # Connect signals for table clicks
        self.current_table.cellClicked.connect(self.on_current_table_click)
        self.monthly_table.cellClicked.connect(self.on_monthly_table_click)

        # Make sure the app opens on the Current Weather tab
        self.tab_widget.setCurrentIndex(0)
        
        # Show the default city detail
        self.show_city_detail(self.default_city_detail)

        # User Preferences tab init
        self.pref_min_temp = 60  # Default minimum ideal temperature in Fahrenheit
        self.pref_max_temp = 80  # Default maximum ideal temperature in Fahrenheit
        self.pref_temp_weight = 0.5  # Default weight for temperature in niceness calculation
        self.use_preferences = False  # Flag to determine if user preferences are applied

        self.create_preferences_tab()
        self.tab_widget.addTab(self.preferences_tab, "Preferences")

        # Re-check niceness definitions, refresh the UI
        self.update_all_niceness_and_refresh()

        # -- Initialize a dict to store continent/country lookups
        self.city_geo_info = {}

        # Load continent data now that we have current_data_list
        self.load_continent_data()

        # Create the new Continent tab after data is loaded
        self.create_continent_tab()

    def set_itinerary_label(self, text):
        self.itinerary_info_label.setText(text)

    def _city_already_loaded(self, city_name: str) -> bool:
        if city_name in self.monthly_dict or city_name in self.all_city_data:
            return True
        return any(r.get("city") == city_name for r in self.current_data_list)

    def remove_current_city(self):
        if not self.current_detail_city:
            return
        city = self.current_detail_city
        if city in self.all_city_data:
            del self.all_city_data[city]
        if city in self.monthly_dict:
            del self.monthly_dict[city]
        self.current_data_list = [c for c in self.current_data_list if c["city"] != city]
        if city in self.forecast_cache:
            del self.forecast_cache[city]

        # If pinned city is the removed city, unpin it
        if self.pinned_city == city:
            self.pinned_city = None

        # Remove from recent_cities
        self.recent_cities = [c for c in self.recent_cities if c != city]

        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()

        # Update detail tab
        if self.recent_cities:
            self.show_city_detail(self.recent_cities[-1])
        elif self.pinned_city:
            self.show_city_detail(self.pinned_city)
        else:
            for i in reversed(range(self.detail_layout.count())):
                w = self.detail_layout.itemAt(i).widget()
                if w and w not in [self.detail_label, self.remove_city_button]:
                    w.setParent(None)
            self.detail_label.setText("City removed. Select another city.")
            self.remove_city_button.setEnabled(False)

        self.current_detail_city = None

    def refresh_itinerary_tab(self):
        monthly_data = {}
        for city, mdf in self.monthly_dict.items():
            monthly_data[city] = mdf.set_index("month")

        top_cities_by_month = {}
        for month in range(1,13):
            city_scores = []
            for city, mdf in monthly_data.items():
                if month in mdf.index:
                    nic = mdf.at[month, "niceness"]
                    if not pd.isna(nic):
                        city_scores.append((city, nic))
            city_scores.sort(key=lambda x: x[1], reverse=True)
            top_cities_by_month[month] = city_scores[:10]

        self.itinerary_table.clear()
        self.itinerary_table.setColumnCount(11)
        itinerary_headers = ["Month"] + [f"Rank {i}" for i in range(1, 11)]
        self.itinerary_table.setHorizontalHeaderLabels(itinerary_headers)
        self.itinerary_table.setRowCount(12)
        self.itinerary_table.setAlternatingRowColors(True)
        self.itinerary_table.verticalHeader().setDefaultSectionSize(50)
        self.itinerary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.itinerary_table.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.itinerary_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.itinerary_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.itinerary_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.itinerary_table.horizontalHeader().setSectionsMovable(True)
        self.itinerary_table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

        if any(top_cities_by_month.values()):
            for i, mon in enumerate(range(1, 13)):
                month_item = QTableWidgetItem(month_name(mon))
                self.itinerary_table.setItem(i, 0, month_item)
                for col_clear in range(1,11):
                    self.itinerary_table.setItem(i, col_clear, None)

                for j, (city, nic_val) in enumerate(top_cities_by_month[mon], start=1):
                    row_m = monthly_data[city].loc[mon] if (city in monthly_data and mon in monthly_data[city].index) else None
                    if row_m is not None:
                        avg_f = row_m["avg_day_f"]
                        sunny = row_m["sunny_day"]
                        hrs = row_m["day_length_hrs"]
                    else:
                        avg_f, sunny, hrs = float('nan'), float('nan'), float('nan')
                    city_str = f"{CITY_COUNTRY.get(city, city)} ({nic_val:.2f})"
                    city_item = QTableWidgetItem(city_str)
                    city_item.setData(Qt.ItemDataRole.UserRole, city)
                    city_item.setForeground(QBrush(Qt.GlobalColor.blue))
                    font = city_item.font()
                    font.setUnderline(True)
                    city_item.setFont(font)
                    highlight_cell(city_item, avg_f, sunny, hrs)
                    self.itinerary_table.setItem(i, j, city_item)
        else:
            self.itinerary_table.setRowCount(1)
            self.itinerary_table.setColumnCount(1)
            self.itinerary_table.setHorizontalHeaderLabels(["Itinerary"])
            no_data_item = QTableWidgetItem("No itinerary data available.")
            self.itinerary_table.setItem(0, 0, no_data_item)

        self.itinerary_table.cellClicked.connect(self.on_itinerary_table_click)

    def abbreviated_monthly_text(self, tmax, tmin, sunny):
        tmax_str = "N/A" if pd.isna(tmax) else f"{tmax:.0f}"
        tmin_str = "N/A" if pd.isna(tmin) else f"{tmin:.0f}"
        sunny_str = "N/A" if pd.isna(sunny) else f"{int(sunny)}/30"
        return f"High: {tmax_str}F Low: {tmin_str}F Sun: {sunny_str}"

    def create_current_table(self, data, monthly_dict):
        # New headers order: Niceness, City, Temp, Sunny Next 16 Days, Sunny Next 30 Days, Day Length
        headers = ["Niceness","City","Temp (H/DL/L)","Sunny Next 16 Days","Sunny Next 30 Days","Day Length"]
        table = QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(data))
        table.setAlternatingRowColors(True)
        table.verticalHeader().setDefaultSectionSize(30)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.horizontalHeader().setSectionsMovable(True)
        now = datetime.now(timezone.utc)
        next_month = (now.month % 12) + 1

        for i, row in enumerate(data):
            tmax_f = row.get("tmax_f", float('nan'))
            tmin_f = row.get("tmin_f", float('nan'))
            daytime_low = (tmax_f + tmin_f)/2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else float('nan')
            if pd.isna(tmax_f) or pd.isna(tmin_f):
                triple_str = "N/A"
                triple_val = float('nan')
            else:
                triple_str = f"{tmax_f:.0f}F/{daytime_low:.0f}F/{tmin_f:.0f}F"
                triple_val = (tmax_f + daytime_low + tmin_f)/3.0

            nice_val = row["niceness"]
            nice_str = f"{nice_val:.2f}" if not pd.isna(nice_val) else "N/A"
            nice_item = NumericTableWidgetItem(nice_val)
            nice_item.setText(nice_str)

            city_text = CITY_COUNTRY.get(row["city"], row["city"])
            city_item = QTableWidgetItem(city_text)
            city_item.setData(Qt.ItemDataRole.UserRole, row["city"])
            city_item.setForeground(QBrush(Qt.GlobalColor.blue))
            font = city_item.font()
            font.setUnderline(True)
            city_item.setFont(font)

            forecast_sunny_count = row.get("forecast_sunny_count", 0)
            forecast_item = NumericTableWidgetItem(forecast_sunny_count)
            forecast_item.setText(str(forecast_sunny_count))

            s_val = row["next_month_sunny_days"]
            s_display = f"{s_val:.0f}" if not pd.isna(s_val) else "N/A"
            next_30_item = NumericTableWidgetItem(s_val)
            next_30_item.setText(s_display)

            dl_val = row.get("est_next_month_day_length", float('nan'))
            dl_display = f"{dl_val:.0f}" if not pd.isna(dl_val) else "N/A"
            dl_item = NumericTableWidgetItem(dl_val)
            dl_item.setText(dl_display)

            triple_item = NumericTableWidgetItem(triple_val)
            triple_item.setText(triple_str)

            table.setItem(i, 0, nice_item)
            table.setItem(i, 1, city_item)
            table.setItem(i, 2, triple_item)
            table.setItem(i, 3, forecast_item)
            table.setItem(i, 4, next_30_item)
            table.setItem(i, 5, dl_item)

            mdf = monthly_dict.get(row["city"], pd.DataFrame())
            row_m = mdf[mdf["month"] == next_month]
            if not row_m.empty:
                avg_f = row_m["avg_day_f"].iloc[0]
                sunny_v = row_m["sunny_day"].iloc[0]
                hrs = row_m["day_length_hrs"].iloc[0]
            else:
                avg_f, sunny_v, hrs = float('nan'), float('nan'), float('nan')

            for c in range(table.columnCount()):
                it = table.item(i, c)
                if it:
                    highlight_cell(it, avg_f, sunny_v, hrs)

        table.setSortingEnabled(True)
        return table

    def create_monthly_table(self, data):
        headers = ["City"] + [month_name(m) for m in range(1,13)]
        table = QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(data))
        table.setAlternatingRowColors(True)
        table.verticalHeader().setDefaultSectionSize(50)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.horizontalHeader().setSectionsMovable(True)

        city_list = list(data.keys())
        for i, city in enumerate(city_list):
            monthly_df = data[city]
            city_item = QTableWidgetItem(CITY_COUNTRY.get(city, city))
            city_item.setData(Qt.ItemDataRole.UserRole, city)
            city_item.setForeground(QBrush(Qt.GlobalColor.blue))
            font = city_item.font()
            font.setUnderline(True)
            city_item.setFont(font)
            table.setItem(i, 0, city_item)

            for col, m in enumerate(range(1,13), start=1):
                row_m = monthly_df[monthly_df["month"] == m]
                if row_m.empty:
                    item = NumericTableWidgetItem(float('nan'))
                    item.setText("N/A")
                    table.setItem(i, col, item)
                else:
                    tmax = row_m["tmax_mean"].iloc[0]
                    tmin = row_m["tmin_mean"].iloc[0]
                    sunny = row_m["sunny_day"].iloc[0]
                    avg_f = row_m["avg_day_f"].iloc[0]
                    hrs = row_m["day_length_hrs"].iloc[0]
                    
                    # Retrieve niceness score for this month
                    niceness = row_m["niceness"].iloc[0]
    
                    txt = self.abbreviated_monthly_text(tmax, tmin, sunny)
                    # Now we use niceness as the value to sort by instead of avg_f:
                    sort_val = niceness  
                    item = NumericTableWidgetItem(sort_val)
                    item.setText(txt)
                    highlight_cell(item, avg_f, sunny, hrs)
                    fitem = item.font()
                    fitem.setBold(False)
                    item.setFont(fitem)
                    table.setItem(i, col, item)
    
        table.setSortingEnabled(True)
        return table

    @pyqtSlot(int)
    def on_current_header_clicked(self, col):
        # For simplicity, just toggle descending by default on single click
        self.current_table.sortItems(col, Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_current = col
        self.last_sort_order_current = Qt.SortOrder.DescendingOrder

    @pyqtSlot(int)
    def on_current_header_double_clicked(self, col):
        # double click toggles order
        if self.last_sorted_column_current == col and self.last_sort_order_current == Qt.SortOrder.DescendingOrder:
            self.current_table.sortItems(col, Qt.SortOrder.AscendingOrder)
            self.last_sort_order_current = Qt.SortOrder.AscendingOrder
        else:
            if self.last_sorted_column_current == col and self.last_sort_order_current == Qt.SortOrder.AscendingOrder:
                self.current_table.sortItems(col, Qt.SortOrder.DescendingOrder)
                self.last_sort_order_current = Qt.SortOrder.DescendingOrder
            else:
                self.current_table.sortItems(col, Qt.SortOrder.AscendingOrder)
                self.last_sorted_column_current = col
                self.last_sort_order_current = Qt.SortOrder.AscendingOrder

    @pyqtSlot(int)
    def on_monthly_header_clicked(self, col):
        self.monthly_table.sortItems(col, Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_monthly = col
        self.last_sort_order_monthly = Qt.SortOrder.DescendingOrder

    @pyqtSlot(int)
    def on_monthly_header_double_clicked(self, col):
        if self.last_sorted_column_monthly == col and self.last_sort_order_monthly == Qt.SortOrder.DescendingOrder:
            self.monthly_table.sortItems(col, Qt.SortOrder.AscendingOrder)
            self.last_sort_order_monthly = Qt.SortOrder.AscendingOrder
        else:
            if self.last_sorted_column_monthly == col and self.last_sort_order_monthly == Qt.SortOrder.AscendingOrder:
                self.monthly_table.sortItems(col, Qt.SortOrder.DescendingOrder)
                self.last_sort_order_monthly = Qt.SortOrder.DescendingOrder
            else:
                self.monthly_table.sortItems(col, Qt.SortOrder.AscendingOrder)
                self.last_sorted_column_monthly = col
                self.last_sort_order_monthly = Qt.SortOrder.AscendingOrder

    @pyqtSlot(int,int)
    def on_monthly_table_double_click(self, row, column):
        city_item = self.monthly_table.item(row, 0)
        if city_item:
            city_key = city_item.data(Qt.ItemDataRole.UserRole)
            if city_key in self.monthly_dict:
                self.show_city_detail(city_key)

    @pyqtSlot(int,int)
    def on_current_table_double_click(self, row, column):
        city_item = self.current_table.item(row, 1)
        if city_item:
            city_key = city_item.data(Qt.ItemDataRole.UserRole)
            if city_key in self.monthly_dict:
                self.show_city_detail(city_key)

    @pyqtSlot(int,int)
    def on_current_table_click(self, row, column):
        # city column = 1 now
        if column == 1:
            city_item = self.current_table.item(row, column)
            if city_item is not None:
                city_key = city_item.data(Qt.ItemDataRole.UserRole)
                if city_key in self.monthly_dict:
                    self.show_city_detail(city_key)
                else:
                    self.update_detail_tab("<b>No monthly data found for this city.</b>", enable_remove=False)

    @pyqtSlot(int,int)
    def on_monthly_table_click(self, row, column):
        if column == 0:
            city_item = self.monthly_table.item(row, column)
            if city_item is not None:
                city_key = city_item.data(Qt.ItemDataRole.UserRole)
                if city_key in self.monthly_dict:
                    self.show_city_detail(city_key)
                else:
                    self.update_detail_tab("<b>No monthly data found for this city.</b>", enable_remove=False)

    @pyqtSlot(int,int)
    def on_itinerary_table_click(self, row, column):
        if column > 0:
            item = self.itinerary_table.item(row, column)
            if item is not None:
                city_key = item.data(Qt.ItemDataRole.UserRole)
                if city_key and city_key in self.monthly_dict:
                    self.show_city_detail(city_key)
                else:
                    self.update_detail_tab("<b>No monthly data found for this city.</b>", enable_remove=False)

    def update_detail_tab(self, text, enable_remove=False):
        for i in reversed(range(self.detail_layout.count())):
            widget = self.detail_layout.itemAt(i).widget()
            if widget and widget not in [self.detail_label, self.remove_city_button]:
                widget.setParent(None)
        self.detail_label.setText(text)
        self.remove_city_button.setEnabled(enable_remove)

    def show_city_detail(self, city):
        self.current_detail_city = city
        self.remove_city_button.setEnabled(True)
        self.detail_label.setText("")

        if city in self.recent_cities:
            self.recent_cities.remove(city)
        self.recent_cities.append(city)

        for i in reversed(range(self.detail_layout.count())):
            widget = self.detail_layout.itemAt(i).widget()
            if widget and widget not in [self.detail_label, self.remove_city_button]:
                widget.setParent(None)

        # Show only two cities: current city on left (highlighted), pinned if different on right
        cities_to_show = [city]
        if self.pinned_city and self.pinned_city != city:
            cities_to_show.append(self.pinned_city)

        hbox = QHBoxLayout()

        for idx, detail_city in enumerate(cities_to_show):
            city_widget = self.create_city_detail_widget(detail_city, is_main=(detail_city == city))
            hbox.addWidget(city_widget)

        container = QWidget()
        container.setLayout(hbox)
        self.detail_layout.addWidget(container)

        self.tab_widget.setCurrentIndex(2)

    def create_city_detail_widget(self, city, is_main=False):
        cur_data = next((x for x in self.current_data_list if x["city"] == city), None)

        def fmt_or_na(val, fmt="{:.0f}"):
            if pd.isna(val):
                return "N/A"
            return fmt.format(val)

        now = datetime.now(timezone.utc)
        next_month = (now.month % 12) + 1
        mdf = self.monthly_dict.get(city, pd.DataFrame())
        row_m = mdf[mdf["month"]==next_month]

        summary_box = QGroupBox()
        font = summary_box.font()
        font.setBold(True)
        summary_box.setFont(font)

        summary_layout = QVBoxLayout()

        title_label = QLabel(city)
        title_font = QFont()
        title_font.setPointSize(60)  # 5x bigger
        title_font.setBold(True)
        title_label.setFont(title_font)
        summary_layout.addWidget(title_label)

        if cur_data is not None:
            tmax_str = fmt_or_na(cur_data.get("tmax_f", float('nan')))
            tmin_str = fmt_or_na(cur_data.get("tmin_f", float('nan')))
            sunny_val = cur_data.get("next_month_sunny_days", float('nan'))
            sunny_str = fmt_or_na(sunny_val)
            if sunny_str != "N/A":
                sunny_str = f"{sunny_str}/30"

            dl_val = row_m["day_length_hrs"].iloc[0] if not row_m.empty else cur_data.get("est_next_month_day_length", 12.0)
            dl_str = fmt_or_na(dl_val)
            nic_val = cur_data["niceness"]
            nic_str = "N/A" if pd.isna(nic_val) else f"{nic_val:.2f}"

            # Create data labels with larger font
            data_font = QFont()
            data_font.setPointSize(30)  # 5x bigger

            data_layout = QVBoxLayout()
            
            temp_label = QLabel(f"High/Low: {tmax_str}F / {tmin_str}F")
            temp_label.setFont(data_font)
            data_layout.addWidget(temp_label)
            
            sunny_label = QLabel(f"Expected Next 30 Sunny Days: {sunny_str}")
            sunny_label.setFont(data_font)
            data_layout.addWidget(sunny_label)
            
            length_label = QLabel(f"Next Month Day Length: {dl_str} hours")
            length_label.setFont(data_font)
            data_layout.addWidget(length_label)
            
            nice_label = QLabel(f"Today's Niceness: {nic_str}")
            nice_label.setFont(data_font)
            data_layout.addWidget(nice_label)

            summary_layout.addLayout(data_layout)
        else:
            data_label = QLabel("<b>No current data found for this city.</b>")
            summary_layout.addWidget(data_label)

        pin_button = QPushButton("Pin City" if self.pinned_city != city else "Unpin City")
        pin_button.setFont(QFont("", 12))
        def toggle_pin():
            if self.pinned_city == city:
                self.pinned_city = None
            else:
                self.pinned_city = city
            self.show_city_detail(city)
        pin_button.clicked.connect(toggle_pin)
        summary_layout.addWidget(pin_button)

        # Modify this section to safely handle the remove button
        if city == self.current_detail_city:
            # Create a new remove button for this detail widget instead of moving the existing one
            remove_button = QPushButton("Remove City")
            remove_button.setFont(QFont("", 12))
            remove_button.clicked.connect(self.remove_current_city)
            summary_layout.addWidget(remove_button)

        summary_box.setLayout(summary_layout)

        # Monthly box - removed title
        monthly_box = QGroupBox()
        font = monthly_box.font()
        font.setBold(True)
        monthly_box.setFont(font)
        monthly_box_layout = QVBoxLayout()

        if city in self.monthly_dict:
            mdf = self.monthly_dict[city]
            if mdf.empty:
                no_data_label = QLabel("No monthly data available.")
                monthly_box_layout.addWidget(no_data_label)
            else:
                current_month = datetime.now(timezone.utc).month
                row_m = mdf[mdf["month"] == current_month]
                
                if not row_m.empty:
                    avg_f = row_m["avg_day_f"].iloc[0]
                    sunny = row_m["sunny_day"].iloc[0]
                    hrs = row_m["day_length_hrs"].iloc[0]
                    tmax = row_m["tmax_mean"].iloc[0]
                    tmin = row_m["tmin_mean"].iloc[0]

                    # Create a container widget for the month data with only outer border
                    month_container = QWidget()
                    month_layout = QVBoxLayout()
                    month_container.setLayout(month_layout)

                    month_label = QLabel(month_name(current_month))
                    month_font = QFont()
                    month_font.setPointSize(30)
                    month_font.setBold(True)
                    month_label.setFont(month_font)
                    month_layout.addWidget(month_label)

                    data_font = QFont()
                    data_font.setPointSize(30)

                    temp_label = QLabel(f"High/Low: {tmax:.0f}F / {tmin:.0f}F")
                    temp_label.setFont(data_font)
                    month_layout.addWidget(temp_label)

                    sunny_label = QLabel(f"Average Sunny Days: {sunny:.0f}/30")
                    sunny_label.setFont(data_font)
                    month_layout.addWidget(sunny_label)

                    length_label = QLabel(f"Day Length: {hrs:.1f} hours")
                    length_label.setFont(data_font)
                    month_layout.addWidget(length_label)

                    # Set background color based on temperature and conditions
                    # Modified to only have outer border
                    base_style = """
                        QWidget {
                            border: 2px solid black;
                            padding: 10px;
                            %s
                        }
                        QLabel {
                            border: none;
                            %s
                        }
                    """

                    if is_nice_strict(avg_f, sunny, hrs):
                        month_container.setStyleSheet(base_style % ("background-color: #FFFF00;", ""))
                    elif is_nice_light(avg_f, sunny, hrs):
                        month_container.setStyleSheet(base_style % ("background-color: #FFFFE0;", ""))
                    elif avg_f > 90:
                        month_container.setStyleSheet(base_style % ("background-color: #FF0000;", "color: white;"))
                    elif avg_f < 50:
                        month_container.setStyleSheet(base_style % ("background-color: #0000FF;", "color: white;"))
                    else:
                        month_container.setStyleSheet(base_style % ("background-color: white;", ""))

                    monthly_box_layout.addWidget(month_container)
                else:
                    no_data_label = QLabel("No data available for current month.")
                    monthly_box_layout.addWidget(no_data_label)
        else:
            no_data_label = QLabel("No monthly data found for this city.")
            monthly_box_layout.addWidget(no_data_label)

        monthly_box.setLayout(monthly_box_layout)

        city_vlayout = QVBoxLayout()
        city_vlayout.addWidget(summary_box)
        city_vlayout.addWidget(monthly_box)

        city_widget = QWidget()
        city_widget.setLayout(city_vlayout)

        # Always apply the "main city" styling:
        summary_box.setStyleSheet("QGroupBox { border: none; }")
        monthly_box.setStyleSheet("QGroupBox { border: none; }")

        city_widget.setStyleSheet("""
            QWidget#mainContainer { 
                border: 3px solid #e0f7ff;
                border-radius: 5px;
                background-color: white;
                padding: 10px;
            }
            QGroupBox {
                background-color: white;
            }
            QWidget {
                background-color: white;
                border: none;
            }
        """)
        city_widget.setObjectName("mainContainer")

        return city_widget

    def add_city(self):
        city_name = self.city_input.text().strip()
        cmd = city_name.lower()

        if cmd == "refresh stale cities":
            self.refresh_stale_cities()
            return
        
        # Handle "all cities" case first
        if cmd == "all cities":
            debug_log_path = os.path.abspath("all_cities_debug.log")
            total = len(ZIP_CITIES)
            added = 0
            already_present = 0
            failed = 0
            for idx, c in enumerate(reversed(list(ZIP_CITIES.keys())), start=1):
                before_count = len(self.current_data_list)
                try:
                    print(f"[{idx}/{total}] Processing {c}...")
                    # Always attempt to add; _add_single_city will use DB cache when available.
                    # Fast path: use only local DB/cache data; skip network calls.
                    self._add_single_city(c, refresh_ui=False, allow_network=False, persist_cache=False)
                    after_count = len(self.current_data_list)
                    if after_count > before_count:
                        added += 1
                    else:
                        already_present += 1
                except Exception as e:
                    failed += 1
                    with open(debug_log_path, "a", encoding="utf-8") as f:
                        f.write(f"\n[{datetime.now().isoformat()}] city={c} error={e}\n")
                        f.write(traceback.format_exc())

                if idx % 10 == 0:
                    QApplication.processEvents()

            # Refresh once at the end to avoid excessive UI churn and crashes.
            self.refresh_current_table()
            self.refresh_monthly_table()
            self.refresh_itinerary_tab()
            save_forecast_cache(self.forecast_cache)
            save_all_cities_ui_cache(self.current_data_list, self.monthly_dict)

            summary = (
                f"All-cities run finished.\n\n"
                f"Total candidates: {total}\n"
                f"Added to UI: {added}\n"
                f"Already in UI/unchanged: {already_present}\n"
                f"Failed with exception: {failed}"
            )
            if failed > 0:
                summary += f"\n\nDebug log: {debug_log_path}"
            QMessageBox.information(self, "All Cities Summary", summary)
            return

        # Try to get coordinates from different sources
        coords = None
        
        # 1. Check CITY_COORDS first
        if city_name in CITY_COORDS:
            coords = CITY_COORDS[city_name]
        
        # 2. Check database
        elif coords := get_city_coords(city_name):
            pass
        
        # 3. Check ZIP_CITIES and geocode if needed
        elif city_name in ZIP_CITIES:
            if ZIP_CITIES[city_name] is None:
                try:
                    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1"
                    r = requests.get(url)
                    r.raise_for_status()
                    data = r.json()
                    if data.get("results"):
                        result = data["results"][0]
                        coords = (result["latitude"], result["longitude"])
                        ZIP_CITIES[city_name] = coords
                except Exception:
                    pass
            else:
                coords = ZIP_CITIES[city_name]
        
        if not coords:
            QMessageBox.warning(self, "Error", f"Could not find coordinates for {city_name}")
            return

        # Rest of add_city method remains the same...
        lat, lon = coords
        if self._city_already_loaded(city_name):
            return

        conn = get_db_conn(DATABASE)
        try:
            if not have_data_for_city(conn, city_name):
                df = fetch_estimated_history(lat, lon, START_DATE, END_DATE, city=city_name)
                if not df.empty:
                    store_data(conn, city_name, df)
                    self.all_city_data[city_name] = df
                else:
                    QMessageBox.warning(self, "Error", f"No Visual Crossing historical data for {city_name}")
                    return
            else:
                df = load_data_from_db(conn, city_name)
                self.all_city_data[city_name] = df
        finally:
            conn.close()

        mdf = monthly_aggregates(self.all_city_data[city_name])
        mdf["niceness"] = mdf.apply(
            lambda r: compute_city_niceness(
                r["tmax_mean"], 
                r["tmin_mean"], 
                r["sunny_day"], 
                r["day_length_hrs"]
            ), 
            axis=1
        )  # ← Make sure this closing parenthesis is here

        self.monthly_dict[city_name] = mdf

        today = datetime.now(timezone.utc)
        next_month = (today.month % 12) + 1
        target_month_row = mdf[mdf["month"]==next_month]
        if target_month_row.empty:
            historical_sunny_avg = 15.0
        else:
            historical_sunny_avg = target_month_row["sunny_day"].iloc[0]

        if city_name in self.forecast_cache:
            fore_json = self.forecast_cache[city_name]['fore_json']
            cur_json = self.forecast_cache[city_name]['cur_json']
        else:
            try:
                fore_json, cur_json = _fetch_visualcrossing_forecast_bundle(lat, lon, days=16, city=city_name)
                self.forecast_cache[city_name] = {
                    'fore_json': fore_json,
                    'cur_json': cur_json,
                    'time': datetime.now(timezone.utc),
                    'provider': WEATHER_PROVIDER,
                }
            except:
                fore_json = {}
                cur_json = {}

        current_temp_f=float('nan')
        tmax_f=float('nan')
        tmin_f=float('nan')
        forecast_sunny_count = 0
        forecast_days = 0

        if "current_weather" in cur_json:
            current_temp_c = cur_json["current_weather"]["temperature"]
            current_temp_f = c_to_f(current_temp_c)

        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            forecast_df = process_forecast_daily_data(fore_json["daily"])
            combined_df = pd.concat([self.all_city_data[city_name], forecast_df], ignore_index=True)
            self.all_city_data[city_name] = combined_df

            daily_dates = pd.to_datetime(fore_json["daily"]["time"])
            daily_tmax = fore_json["daily"]["temperature_2m_max"]
            daily_tmin = fore_json["daily"]["temperature_2m_min"]
            daily_codes = fore_json["daily"]["weathercode"]

            forecast_sunny_count = sum(1 for c in daily_codes if c in SUNNY_CODES)
            forecast_days = len(daily_codes)

            today_str = today.strftime("%Y-%m-%d")
            idx_today = None
            for i2, d in enumerate(daily_dates):
                if d.strftime("%Y-%m-%d") == today_str:
                    idx_today = i2
                    break
            if idx_today is not None:
                tmax_f = c_to_f(daily_tmax[idx_today])
                tmin_f = c_to_f(daily_tmin[idx_today])
            else:
                if len(daily_tmax) > 0:
                    tmax_f = c_to_f(daily_tmax[0])
                    tmin_f = c_to_f(daily_tmin[0])

            sunny_fraction_hist = historical_sunny_avg/30.0
            if forecast_days < 30:
                remainder = 30 - forecast_days
                remainder_sunny = remainder * sunny_fraction_hist
                next_month_sunny_days = forecast_sunny_count + remainder_sunny
            else:
                next_month_sunny_days = forecast_sunny_count
        else:
            next_month_sunny_days = historical_sunny_avg
        next_month_sunny_days = max(0.0, min(float(next_month_sunny_days), 30.0))

        row_m = mdf[mdf["month"] == next_month]
        if not row_m.empty:
            est_next_month_day_length = row_m["day_length_hrs"].iloc[0]
        else:
            est_next_month_day_length = 12.0

        ref_temp = (tmax_f+tmin_f)/2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
        niceness = compute_niceness(ref_temp, next_month_sunny_days, est_next_month_day_length)

        new_city_current = {
            "city": city_name,
            "current_temp_f": current_temp_f,
            "next_month_sunny_days": next_month_sunny_days,
            "est_next_month_day_length": est_next_month_day_length,
            "niceness": niceness,
            "tmax_f": tmax_f,
            "tmin_f": tmin_f,
            "forecast_sunny_count": forecast_sunny_count,
            "forecast_days": forecast_days
        }
        self.current_data_list.append(new_city_current)
        save_forecast_cache(self.forecast_cache)

        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()

        QMessageBox.information(self, "Success", f"City {city_name} added successfully!")
        self.show_city_detail(city_name)

    def _add_single_city(self, city_name, refresh_ui=True, allow_network=True, persist_cache=True):
        if self._city_already_loaded(city_name):
            return

        # Check if it's a full city,country string from ALL_CITIES
        if city_name in ALL_CITIES:
            lat, lon = ALL_CITIES[city_name]
        else:
            # Check if it's in ZIP_CITIES
            if city_name in ZIP_CITIES:
                # If we don't have coordinates yet, we need to fetch them
                if ZIP_CITIES[city_name] is None:
                    if not allow_network:
                        return
                    # Use a geocoding service to get coordinates
                    try:
                        url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1"
                        r = requests.get(url)
                        r.raise_for_status()
                        data = r.json()
                        if data.get("results"):
                            result = data["results"][0]
                            lat = result["latitude"]
                            lon = result["longitude"]
                            ZIP_CITIES[city_name] = (lat, lon)
                        else:
                            return
                    except Exception as e:
                        return
                else:
                    lat, lon = ZIP_CITIES[city_name]
            else:
                # Check if it's a simple city name from CITY_COORDS
                city_key = next((k for k in CITY_COORDS.keys() if k.lower() == city_name.lower()), None)
                if city_key:
                    city_name = city_key  # Use the properly cased version
                    lat, lon = CITY_COORDS[city_key]
                else:
                    return

        # Fetch or use cached historical data
        mdf = None
        conn = get_db_conn(DATABASE)
        try:
            if not have_data_for_city(conn, city_name):
                if not allow_network:
                    return
                try:
                    df = fetch_estimated_history(lat, lon, START_DATE, END_DATE, city=city_name)
                    if not df.empty:
                        store_data(conn, city_name, df)
                        self.all_city_data[city_name] = df
                    else:
                        return
                except requests.exceptions.RequestException:
                    # Silently fail on request errors
                    return
            else:
                if allow_network:
                    df = load_data_from_db(conn, city_name)
                    self.all_city_data[city_name] = df
                else:
                    mdf = monthly_aggregates_from_db(conn, city_name)
                    if mdf is None:
                        return
        finally:
            conn.close()

        if mdf is None:
            mdf = monthly_aggregates(self.all_city_data[city_name])
        mdf["niceness"] = mdf.apply(
            lambda r: compute_city_niceness(
                r["tmax_mean"], 
                r["tmin_mean"], 
                r["sunny_day"], 
                r["day_length_hrs"]
            ), 
            axis=1
        )  # ← Make sure this closing parenthesis is here

        self.monthly_dict[city_name] = mdf

        today = datetime.now(timezone.utc)
        next_month = (today.month % 12) + 1
        target_month_row = mdf[mdf["month"]==next_month]
        if target_month_row.empty:
            historical_sunny_avg = 15.0
        else:
            historical_sunny_avg = target_month_row["sunny_day"].iloc[0]

        if city_name in self.forecast_cache:
            fore_json = self.forecast_cache[city_name]['fore_json']
            cur_json = self.forecast_cache[city_name]['cur_json']
        else:
            if not allow_network:
                fore_json = {}
                cur_json = {}
            else:
                try:
                    fore_json, cur_json = _fetch_visualcrossing_forecast_bundle(lat, lon, days=16, city=city_name)
                    self.forecast_cache[city_name] = {
                        'fore_json': fore_json,
                        'cur_json': cur_json,
                        'time': datetime.now(timezone.utc),
                        'provider': WEATHER_PROVIDER,
                    }
                except:
                    fore_json = {}
                    cur_json = {}

        current_temp_f = float('nan')
        est_next_month_day_length = 12.0
        tmax_f = float('nan')
        tmin_f = float('nan')

        # Re-check next_month in case we need it again below
        next_month = (today.month % 12) + 1

        if "current_weather" in cur_json:
            current_temp_c = cur_json["current_weather"]["temperature"]
            current_temp_f = c_to_f(current_temp_c)

        forecast_sunny_count = 0
        forecast_days = 0
        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            # Fast local-only batch mode does not need to materialize a merged
            # historical+forecast dataframe; we only need the daily arrays below.
            if allow_network:
                forecast_df = process_forecast_daily_data(fore_json["daily"])
                combined_df = pd.concat([df, forecast_df], ignore_index=True)
                self.all_city_data[city_name] = combined_df

            daily_dates = pd.to_datetime(fore_json["daily"]["time"])
            daily_tmax = fore_json["daily"]["temperature_2m_max"]
            daily_tmin = fore_json["daily"]["temperature_2m_min"]
            daily_codes = fore_json["daily"]["weathercode"]

            forecast_sunny_count = sum(1 for c in daily_codes if c in SUNNY_CODES)
            forecast_days = len(daily_codes)

            today_str = today.strftime("%Y-%m-%d")
            idx_today = None
            for i2, d in enumerate(daily_dates):
                if d.strftime("%Y-%m-%d") == today_str:
                    idx_today = i2
                    break
            if idx_today is not None:
                tmax_f = c_to_f(daily_tmax[idx_today])
                tmin_f = c_to_f(daily_tmin[idx_today])
            else:
                if len(daily_tmax) > 0:
                    tmax_f = c_to_f(daily_tmax[0])
                    tmin_f = c_to_f(daily_tmin[0])

            sunny_fraction_hist = historical_sunny_avg / 30.0
            if forecast_days < 30:
                remainder = 30 - forecast_days
                remainder_sunny = remainder * sunny_fraction_hist
                next_month_sunny_days = forecast_sunny_count + remainder_sunny
            else:
                next_month_sunny_days = forecast_sunny_count
        else:
            # If no forecast data, fallback to historical average
            next_month_sunny_days = historical_sunny_avg
        next_month_sunny_days = max(0.0, min(float(next_month_sunny_days), 30.0))

        row_m = self.monthly_dict[city_name][
            self.monthly_dict[city_name]["month"] == next_month
        ]
        if not row_m.empty:
            est_next_month_day_length = row_m["day_length_hrs"].iloc[0]

        ref_temp = (tmax_f + tmin_f) / 2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
        niceness = compute_niceness(ref_temp, next_month_sunny_days, est_next_month_day_length)

        print(f"Done {city_name}.")

        # Return data if you need it, or just append to self.current_data_list
        new_city_current = {
            "city": city_name,
            "current_temp_f": current_temp_f,
            "next_month_sunny_days": next_month_sunny_days,
            "est_next_month_day_length": est_next_month_day_length,
            "niceness": niceness,
            "tmax_f": tmax_f,
            "tmin_f": tmin_f,
            "forecast_sunny_count": forecast_sunny_count,
            "forecast_days": forecast_days
        }
        self.current_data_list.append(new_city_current)
        if persist_cache:
            save_forecast_cache(self.forecast_cache)

        # Refresh UI tables only when explicitly requested.
        if refresh_ui:
            self.refresh_current_table()
            self.refresh_monthly_table()
            self.refresh_itinerary_tab()

    def _resolve_city_coords_local(self, city_name):
        # Prefer known in-memory mappings first.
        if city_name in CITY_COORDS:
            return CITY_COORDS[city_name]
        if city_name in ZIP_CITIES and ZIP_CITIES[city_name] is not None:
            return ZIP_CITIES[city_name]

        # Fallback to DB-backed coordinates.
        coords = get_city_coords(city_name)
        if coords:
            return coords
        return None

    def _compute_current_row(self, city_name, mdf, fore_json, cur_json):
        today = datetime.now(timezone.utc)
        next_month = (today.month % 12) + 1
        target_month_row = mdf[mdf["month"] == next_month]
        if target_month_row.empty:
            historical_sunny_avg = 15.0
        else:
            historical_sunny_avg = target_month_row["sunny_day"].iloc[0]

        current_temp_f = float('nan')
        tmax_f = float('nan')
        tmin_f = float('nan')
        forecast_sunny_count = 0
        forecast_days = 0

        if "current_weather" in cur_json:
            current_temp_c = cur_json["current_weather"]["temperature"]
            current_temp_f = c_to_f(current_temp_c)

        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            daily_dates = pd.to_datetime(fore_json["daily"]["time"])
            daily_tmax = fore_json["daily"]["temperature_2m_max"]
            daily_tmin = fore_json["daily"]["temperature_2m_min"]
            daily_codes = fore_json["daily"]["weathercode"]

            forecast_sunny_count = sum(1 for c in daily_codes if c in SUNNY_CODES)
            forecast_days = len(daily_codes)

            today_str = today.strftime("%Y-%m-%d")
            idx_today = None
            for i2, d in enumerate(daily_dates):
                if d.strftime("%Y-%m-%d") == today_str:
                    idx_today = i2
                    break
            if idx_today is not None:
                tmax_f = c_to_f(daily_tmax[idx_today])
                tmin_f = c_to_f(daily_tmin[idx_today])
            elif len(daily_tmax) > 0:
                tmax_f = c_to_f(daily_tmax[0])
                tmin_f = c_to_f(daily_tmin[0])

            sunny_fraction_hist = historical_sunny_avg / 30.0
            if forecast_days < 30:
                remainder = 30 - forecast_days
                remainder_sunny = remainder * sunny_fraction_hist
                next_month_sunny_days = forecast_sunny_count + remainder_sunny
            else:
                next_month_sunny_days = forecast_sunny_count
        else:
            next_month_sunny_days = historical_sunny_avg
        next_month_sunny_days = max(0.0, min(float(next_month_sunny_days), 30.0))

        row_m = mdf[mdf["month"] == next_month]
        if not row_m.empty:
            est_next_month_day_length = row_m["day_length_hrs"].iloc[0]
        else:
            est_next_month_day_length = 12.0

        ref_temp = (tmax_f + tmin_f) / 2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
        niceness = compute_niceness(ref_temp, next_month_sunny_days, est_next_month_day_length)

        return {
            "city": city_name,
            "current_temp_f": current_temp_f,
            "next_month_sunny_days": next_month_sunny_days,
            "est_next_month_day_length": est_next_month_day_length,
            "niceness": niceness,
            "tmax_f": tmax_f,
            "tmin_f": tmin_f,
            "forecast_sunny_count": forecast_sunny_count,
            "forecast_days": forecast_days
        }

    def _refresh_single_stale_city(self, city_name):
        coords = self._resolve_city_coords_local(city_name)
        if not coords:
            return False
        lat, lon = coords

        # Ensure monthly data is available without heavy in-memory daily loads.
        if city_name in self.monthly_dict:
            mdf = self.monthly_dict[city_name]
        else:
            conn = get_db_conn(DATABASE)
            try:
                if not have_data_for_city(conn, city_name):
                    return False
                mdf = monthly_aggregates_from_db(conn, city_name)
            finally:
                conn.close()
            if mdf is None:
                return False
            mdf["niceness"] = mdf.apply(
                lambda r: compute_city_niceness(
                    r["tmax_mean"],
                    r["tmin_mean"],
                    r["sunny_day"],
                    r["day_length_hrs"]
                ),
                axis=1
            )
            self.monthly_dict[city_name] = mdf

        try:
            fore_json, cur_json = _fetch_visualcrossing_forecast_bundle(lat, lon, days=16, city=city_name)
        except requests.exceptions.RequestException:
            return False
        except Exception:
            return False

        self.forecast_cache[city_name] = {
            "fore_json": fore_json,
            "cur_json": cur_json,
            "time": datetime.now(timezone.utc),
            "provider": WEATHER_PROVIDER,
        }

        row = self._compute_current_row(city_name, mdf, fore_json, cur_json)
        replaced = False
        for i, existing in enumerate(self.current_data_list):
            if existing.get("city") == city_name:
                self.current_data_list[i] = row
                replaced = True
                break
        if not replaced:
            self.current_data_list.append(row)
        return True

    def refresh_stale_cities(self):
        stale = [c for c in ZIP_CITIES.keys() if not is_forecast_fresh(c, self.forecast_cache, hours=24)]
        if not stale:
            QMessageBox.information(self, "Stale Refresh", "All ZIP cities are already fresh (within 24h).")
            return

        total = len(stale)
        updated = 0
        failed = 0
        for idx, city in enumerate(stale, start=1):
            print(f"[stale {idx}/{total}] Refreshing {city}...")
            ok = self._refresh_single_stale_city(city)
            if ok:
                updated += 1
            else:
                failed += 1
            if idx % 10 == 0:
                QApplication.processEvents()

        save_forecast_cache(self.forecast_cache)
        save_all_cities_ui_cache(self.current_data_list, self.monthly_dict)
        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()

        QMessageBox.information(
            self,
            "Stale Refresh Summary",
            f"Candidates: {total}\nUpdated: {updated}\nFailed/Skipped: {failed}"
        )

    def refresh_current_table(self):
        new_table = self.create_current_table(self.current_data_list, self.monthly_dict)
        self.current_table.horizontalHeader().sectionClicked.disconnect()
        self.current_table.horizontalHeader().sectionDoubleClicked.disconnect()
        self.current_table.cellDoubleClicked.disconnect()
        self.tab_widget.widget(0).layout().replaceWidget(self.current_table, new_table)
        self.current_table.deleteLater()
        self.current_table = new_table
        self.current_table.horizontalHeader().sectionClicked.connect(self.on_current_header_clicked)
        self.current_table.horizontalHeader().sectionDoubleClicked.connect(self.on_current_header_double_clicked)
        self.current_table.cellClicked.connect(self.on_current_table_click)
        self.current_table.cellDoubleClicked.connect(self.on_current_table_double_click)
        # Default sort by niceness descending
        self.current_table.sortItems(0, Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_current = 0
        self.last_sort_order_current = Qt.SortOrder.DescendingOrder

    def refresh_monthly_table(self):
        new_table = self.create_monthly_table(self.monthly_dict)
        self.monthly_table.horizontalHeader().sectionClicked.disconnect()
        self.monthly_table.horizontalHeader().sectionDoubleClicked.disconnect()
        self.monthly_table.cellDoubleClicked.disconnect()
        self.monthly_table.cellClicked.disconnect()
        self.tab_widget.widget(1).layout().replaceWidget(self.monthly_table, new_table)
        self.monthly_table.deleteLater()
        self.monthly_table = new_table
        self.monthly_table.horizontalHeader().sectionClicked.connect(self.on_monthly_header_clicked)
        self.monthly_table.horizontalHeader().sectionDoubleClicked.connect(self.on_monthly_header_double_clicked)
        self.monthly_table.cellClicked.connect(self.on_monthly_table_click)
        self.monthly_table.cellDoubleClicked.connect(self.on_monthly_table_double_click)

    def update_ziplist_entry(self, city_name, fetched_value):
        # city_name is e.g. "Austin, USA"
        if "," not in city_name:
            return  # skip if malformed

        # parse out the city and country
        parts = city_name.split(",")
        ccity = parts[0].strip()
        ccountry = parts[1].strip()
        key = f"{ccity}, {ccountry}"

        if key not in ZIP_CITIES:
            return

        ZIP_CITIES[key]["fetched"] = fetched_value

        # Now rewrite entire ziplist with updated flags
        with open("ziplist.txt", "w", encoding="utf-8") as out:
            out.write("city,country,fetched\n")  # header row
            for name, data in ZIP_CITIES.items():
                # name is something like "Austin, USA"
                c, cn = name.split(",")
                c = c.strip()
                cn = cn.strip()
                out.write(f"{c},{cn},{data['fetched']}\n")

    # {{ New method: Preferences tab UI }}
    def create_preferences_tab(self):
        self.preferences_tab = QWidget()
        layout = QFormLayout()

        self.min_temp_spin = QSpinBox()
        self.min_temp_spin.setRange(0, 150)
        self.min_temp_spin.setValue(self.pref_min_temp)
        layout.addRow("Min Ideal Temp (°F):", self.min_temp_spin)

        self.max_temp_spin = QSpinBox()
        self.max_temp_spin.setRange(0, 150)
        self.max_temp_spin.setValue(self.pref_max_temp)
        layout.addRow("Max Ideal Temp (°F):", self.max_temp_spin)

        self.temp_weight_spin = QDoubleSpinBox()
        self.temp_weight_spin.setRange(0.0, 1.0)
        self.temp_weight_spin.setSingleStep(0.05)
        self.temp_weight_spin.setValue(self.pref_temp_weight)
        layout.addRow("Temperature Weight (0.0 - 1.0):", self.temp_weight_spin)

        apply_button = QPushButton("Apply")
        apply_button.clicked.connect(self.on_apply_preferences)
        layout.addRow(apply_button)

        self.preferences_tab.setLayout(layout)

    # {{ New method: apply user prefs and recalc niceness }}
    def on_apply_preferences(self):
        self.pref_min_temp = self.min_temp_spin.value()
        self.pref_max_temp = self.max_temp_spin.value()
        self.pref_temp_weight = self.temp_weight_spin.value()
        self.use_preferences = True
        self.update_all_niceness_and_refresh()

    # {{ New method: re-compute niceness for all cities }}
    def update_all_niceness_and_refresh(self):
        # Recompute niceness for each current_data_list entry
        for row in self.current_data_list:
            tmax_f = row["tmax_f"]
            tmin_f = row["tmin_f"]
            sunny_days = row["next_month_sunny_days"]
            day_length = row["est_next_month_day_length"]

            if not self.use_preferences:
                row["niceness"] = compute_city_niceness(tmax_f, tmin_f, sunny_days, day_length)
            else:
                row["niceness"] = self.compute_adjusted_niceness(tmax_f, tmin_f, sunny_days, day_length)

        # Recompute niceness in each monthly dataframe
        for city, mdf in self.monthly_dict.items():
            def calc_niceness(r):
                if not self.use_preferences:
                    return compute_city_niceness(r["tmax_mean"], r["tmin_mean"], r["sunny_day"], r["day_length_hrs"])
                else:
                    return self.compute_adjusted_niceness(r["tmax_mean"], r["tmin_mean"], r["sunny_day"], r["day_length_hrs"])
            mdf["niceness"] = mdf.apply(calc_niceness, axis=1)

        # Refresh displayed tables
        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()
        if self.current_detail_city:
            self.show_city_detail(self.current_detail_city)

    # {{ New method: user-adjusted niceness based on min/max temp & weighting }}
    def compute_adjusted_niceness(self, tmax_f, tmin_f, sunny_days, day_length_hrs):
        if self.pref_min_temp > self.pref_max_temp:
            self.pref_min_temp, self.pref_max_temp = self.pref_max_temp, self.pref_min_temp

        daytime_avg_f = compute_daytime_avg_temp(tmax_f, tmin_f)
        if daytime_avg_f < 50 or daytime_avg_f > 105:
            temp_score = 0.0
        elif 50 <= daytime_avg_f < self.pref_min_temp:
            temp_score = (daytime_avg_f - 50) / float(self.pref_min_temp - 50)
        elif self.pref_min_temp <= daytime_avg_f <= self.pref_max_temp:
            temp_score = 1.0
        else:  # Above max temp but <= 105
            temp_score = 1.0 - (daytime_avg_f - self.pref_max_temp) / float(105 - self.pref_max_temp)

        temp_score = max(0.0, min(temp_score, 1.0))

        sunny_score = max(0.0, min(sunny_days / 30.0, 1.0))
        day_length_score = max(0.0, min(day_length_hrs / 24.0, 1.0))

        sun_day_score = (sunny_score + day_length_score) / 2.0
        niceness = (self.pref_temp_weight * temp_score) + ((1.0 - self.pref_temp_weight) * sun_day_score)
        return max(0.0, min(niceness, 1.0))

    def load_continent_data(self):
        """
        For each city in current_data_list, get the lat/lon from CITY_COORDS and call get_geo_boundaries.
        Store the result in self.city_geo_info[city].
        """
        for row in self.current_data_list:
            city = row["city"]
            # Only do lookup if city is in CITY_COORDS
            if city in CITY_COORDS:
                lat, lon = CITY_COORDS[city]
                info = get_geo_boundaries(lat, lon)
                if info is None:
                    # If the server returns 404 or no data
                    self.city_geo_info[city] = {
                        "continent_code": "N/A",
                        "country_a2": "N/A",
                        "country_a3": "N/A"
                    }
                else:
                    self.city_geo_info[city] = info
            else:
                # If we don't have coordinates, just set N/A
                self.city_geo_info[city] = {
                    "continent_code": "N/A",
                    "country_a2": "N/A",
                    "country_a3": "N/A"
                }

    def create_continent_tab(self):
        """
        Create a new tab called 'Continent' that displays each city and its continent/country info.
        """
        self.continent_tab = QWidget()
        continent_layout = QVBoxLayout()

        headers = ["City", "Continent Code", "Country Code (A2)", "Country Code (A3)"]
        table = QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(self.current_data_list))
        table.setAlternatingRowColors(True)
        table.verticalHeader().setDefaultSectionSize(30)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.horizontalHeader().setSectionsMovable(True)

        for i, row in enumerate(self.current_data_list):
            city = row["city"]
            info = self.city_geo_info.get(city, {
                "continent_code": "N/A",
                "country_a2": "N/A",
                "country_a3": "N/A"
            })

            city_name_item = QTableWidgetItem(city)
            cont_item = QTableWidgetItem(info["continent_code"])
            c2_item = QTableWidgetItem(info["country_a2"])
            c3_item = QTableWidgetItem(info["country_a3"])

            table.setItem(i, 0, city_name_item)
            table.setItem(i, 1, cont_item)
            table.setItem(i, 2, c2_item)
            table.setItem(i, 3, c3_item)

        continent_layout.addWidget(table)
        self.continent_tab.setLayout(continent_layout)
        self.tab_widget.addTab(self.continent_tab, "Continent")

def _configure_qt_runtime():
    """
    Make Qt startup resilient by setting plugin paths from the active PyQt6 install.
    This avoids relying on inherited shell env vars.
    """
    pyver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = []

    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        candidates.append(os.path.join(venv, "lib", pyver, "site-packages", "PyQt6", "Qt6", "plugins"))

    try:
        import PyQt6 as _pyqt6_pkg
        pkg_root = os.path.dirname(_pyqt6_pkg.__file__)
        candidates.append(os.path.join(pkg_root, "Qt6", "plugins"))
    except Exception:
        pkg_root = None

    chosen_plugins = None
    for p in candidates:
        cocoa = os.path.join(p, "platforms", "libqcocoa.dylib")
        if os.path.exists(cocoa):
            chosen_plugins = p
            break

    if chosen_plugins:
        os.environ["QT_PLUGIN_PATH"] = chosen_plugins
        os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = os.path.join(chosen_plugins, "platforms")
        QCoreApplication.setLibraryPaths([chosen_plugins])

        # Ensure Qt frameworks are visible to the dynamic loader on macOS.
        if pkg_root:
            qt_lib = os.path.join(pkg_root, "Qt6", "lib")
            if os.path.isdir(qt_lib):
                existing = os.environ.get("DYLD_FRAMEWORK_PATH", "")
                os.environ["DYLD_FRAMEWORK_PATH"] = f"{qt_lib}:{existing}" if existing else qt_lib


def main():
    _configure_qt_runtime()
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # Light mode palette (forces black text and white background)
    palette = QPalette()
    palette.setColor(QPalette.ColorRole.Window, QColor(255, 255, 255))
    palette.setColor(QPalette.ColorRole.WindowText, QColor(0, 0, 0))
    palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor(245, 245, 245))
    palette.setColor(QPalette.ColorRole.Text, QColor(0, 0, 0))
    palette.setColor(QPalette.ColorRole.PlaceholderText, QColor(128, 128, 128))
    palette.setColor(QPalette.ColorRole.Button, QColor(240, 240, 240))
    palette.setColor(QPalette.ColorRole.ButtonText, QColor(0, 0, 0))
    palette.setColor(QPalette.ColorRole.Highlight, QColor(200, 200, 200))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor(0, 0, 0))
    palette.setColor(QPalette.ColorRole.Light, QColor(255, 255, 255))
    palette.setColor(QPalette.ColorRole.Midlight, QColor(240, 240, 240))
    palette.setColor(QPalette.ColorRole.Mid, QColor(200, 200, 200))
    palette.setColor(QPalette.ColorRole.Dark, QColor(160, 160, 160))
    palette.setColor(QPalette.ColorRole.Shadow, QColor(105, 105, 105))
    palette.setColor(QPalette.ColorRole.Link, QColor(0, 0, 255))
    app.setPalette(palette)

    # Force black text and white background in style sheet (will override dark-mode settings)
    app.setStyleSheet("""
        QWidget {
            background-color: white;
            color: black;
        }
        /* Force black text on everything (labels, buttons, etc.) */
        * {
            color: black !important;
            background-color: white !important;
        }
        QTableWidget {
            alternate-background-color: #f5f5f5;
        }
        QHeaderView::section {
            background-color: #e0e0e0;
            color: black !important;
        }
        QPushButton {
            background-color: #f0f0f0;
            border: 1px solid #c0c0c0;
        }
        QLineEdit {
            border: 1px solid #c0c0c0;
        }
        /* Force label text black */
        QLabel {
            color: black !important;
        }
    """)

    global START_DATE, END_DATE
    START_DATE, END_DATE = get_estimated_window_dates()
    lock_fh = acquire_run_lock(RUN_LOCK_FILE)
    if lock_fh is None:
        append_sync_log("Another sync process is already running. Exiting this run to avoid DB lock conflicts.")
        QMessageBox.warning(None, "Sync Already Running", "Another sync process is already running.\nPlease wait for it to finish.")
        return
    # Keep lock handle alive for process lifetime.
    app._run_lock_fh = lock_fh
    init_db()

    forecast_cache = load_forecast_cache()
    city_map = build_target_city_map(forecast_cache)
    city_list = list(city_map.items())
    city_names = [c for c, _ in city_list]

    append_sync_log(f"Using DB: {DATABASE}")
    append_sync_log(f"Provider: {WEATHER_PROVIDER} (workers={max(1, VC_MAX_WORKERS)}, min_interval={VC_MIN_INTERVAL_SEC:.2f}s)")
    append_sync_log(f"Estimated window: {START_DATE} .. {END_DATE}")
    append_sync_log(f"Target cities: {len(city_list)}")

    loading = LoadingDialog(len(city_list))
    loading.show()

    conn = get_db_conn(DATABASE)
    before = sync_status_snapshot(conn, city_names, forecast_cache)
    append_sync_log(
        "Before sync: "
        f"estimated complete={before['hist_complete']}, missing={before['hist_missing']}; "
        f"forecast fresh={before['forecast_fresh']}, stale={before['forecast_stale']}"
    )

    today_key = datetime.now(timezone.utc).date().isoformat()
    last_daily_sync = get_sync_meta(conn, "last_daily_sync_date", "")
    should_sync = (last_daily_sync != today_key)
    if should_sync:
        append_sync_log("Daily sync: enabled (not yet run today).")
    else:
        append_sync_log("Daily sync: skipped (already ran today).")

    run_id = str(uuid.uuid4())
    c_meta = conn.cursor()
    c_meta.execute(
        "UPDATE sync_runs SET status='abandoned', finished_at=?, notes=COALESCE(notes,'') || ' ; superseded_by=' || ? "
        "WHERE status='running'",
        (datetime.now(timezone.utc).isoformat(), run_id),
    )
    c_meta.execute(
        "INSERT OR REPLACE INTO sync_runs (run_id, started_at, status, total_cities) VALUES (?, ?, ?, ?)",
        (run_id, datetime.now(timezone.utc).isoformat(), "running", len(city_list)),
    )
    conn.commit()

    historical_updated = 0
    forecast_updated = 0
    errors = 0

    print("Fetching estimated baseline data...")
    cities_needing_estimated = set()
    if should_sync:
        for city_name in city_names:
            if not have_data_for_city(conn, city_name):
                cities_needing_estimated.add(city_name)

    def fetch_city_data(city, latlon):
        try:
            if city not in cities_needing_estimated:
                return city, False, pd.DataFrame(), None
            lat, lon = latlon
            df_est = fetch_estimated_history(lat, lon, START_DATE, END_DATE, city=city)
            return city, True, df_est, None
        except Exception as e:
            return city, True, pd.DataFrame(), str(e)

    all_city_data = {}
    done_count = 0
    with ThreadPoolExecutor(max_workers=max(1, VC_MAX_WORKERS)) as executor:
        futures = {executor.submit(fetch_city_data, c, l): c for c, l in city_list}
        for fut in as_completed(futures):
            city_name, needed_fetch, df_est, err_msg = fut.result()
            hist_rows = 0
            if err_msg:
                errors += 1
                insert_sync_city_log(conn, run_id, city_name, "estimated", "error", err_msg)
            elif needed_fetch and not df_est.empty:
                try:
                    store_data(conn, city_name, df_est, source="estimated")
                    hist_rows = len(df_est)
                except Exception as e:
                    errors += 1
                    insert_sync_city_log(conn, run_id, city_name, "estimated", "error", f"store_failed: {e}")
            elif needed_fetch:
                insert_sync_city_log(conn, run_id, city_name, "estimated", "no_data", "provider returned no rows")
            else:
                insert_sync_city_log(conn, run_id, city_name, "estimated", "complete", "already complete")

            df = load_data_from_db(conn, city_name)
            if not df.empty:
                all_city_data[city_name] = df

            if hist_rows > 0:
                historical_updated += 1
                insert_sync_city_log(conn, run_id, city_name, "estimated", "updated", f"rows={hist_rows}")
            done_count += 1
            loading.update_fetch(done_count)
            if done_count % 50 == 0:
                append_sync_log(f"Estimated progress: {done_count}/{len(city_list)}")

    print("Processing monthly data...")
    done_count = 0
    monthly_dict = {}
    with ThreadPoolExecutor(max_workers=max(1, VC_MAX_WORKERS)) as executor:
        def monthly_task(c):
            mdf = monthly_aggregates(all_city_data[c])
            mdf["niceness"] = mdf.apply(
                lambda r: compute_city_niceness(r["tmax_mean"], r["tmin_mean"], r["sunny_day"], r["day_length_hrs"]),
                axis=1
            )
            return c, mdf

        futures = {executor.submit(monthly_task, c): c for c in all_city_data}
        for fut in as_completed(futures):
            city_name, mdf = fut.result()
            monthly_dict[city_name] = mdf
            done_count += 1
            loading.update_process(done_count)

    print("Fetching current & forecast data...")
    def fetch_current_data(city, latlon):
        lat, lon = latlon
        local_df = all_city_data.get(city, pd.DataFrame())
        now = datetime.now(timezone.utc)

        fore_json = {}
        cur_json = {}
        forecast_was_updated = False
        forecast_err = None
        try:
            can_use_cached = (city in forecast_cache and is_forecast_fresh(city, forecast_cache, hours=24))
            if can_use_cached or not should_sync:
                if city in forecast_cache:
                    fore_json = forecast_cache[city].get('fore_json', {})
                    cur_json = forecast_cache[city].get('cur_json', {})
            else:
                fore_json, cur_json = _fetch_visualcrossing_forecast_bundle(lat, lon, days=16, city=city)
                forecast_cache[city] = {
                    'fore_json': fore_json,
                    'cur_json': cur_json,
                    'time': now,
                    'provider': WEATHER_PROVIDER,
                }
                forecast_was_updated = True
        except Exception as e:
            forecast_err = str(e)
            fore_json = forecast_cache.get(city, {}).get('fore_json', {})
            cur_json = forecast_cache.get(city, {}).get('cur_json', {})

        forecast_df = pd.DataFrame()
        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            forecast_df = process_forecast_daily_data(fore_json["daily"])

        current_temp_f = float('nan')
        est_next_month_day_length = 12.0
        tmax_f = float('nan')
        tmin_f = float('nan')

        today = datetime.now(timezone.utc)
        next_month = (today.month % 12) + 1
        mdf = monthly_dict.get(city)
        if mdf is None:
            mdf = pd.DataFrame(columns=["month", "sunny_day", "day_length_hrs"])

        target_month_row = mdf[mdf["month"] == next_month] if not mdf.empty else pd.DataFrame()
        historical_sunny_avg = 15.0 if target_month_row.empty else target_month_row["sunny_day"].iloc[0]

        if "current_weather" in cur_json:
            current_temp_c = cur_json["current_weather"]["temperature"]
            current_temp_f = c_to_f(current_temp_c)

        forecast_sunny_count = 0
        forecast_days = 0
        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            daily_dates = pd.to_datetime(fore_json["daily"]["time"])
            daily_tmax = fore_json["daily"]["temperature_2m_max"]
            daily_tmin = fore_json["daily"]["temperature_2m_min"]
            daily_codes = fore_json["daily"]["weathercode"]

            forecast_sunny_count = sum(1 for c in daily_codes if c in SUNNY_CODES)
            forecast_days = len(daily_codes)

            today_str = today.strftime("%Y-%m-%d")
            idx_today = None
            for i2, d in enumerate(daily_dates):
                if d.strftime("%Y-%m-%d") == today_str:
                    idx_today = i2
                    break
            if idx_today is not None:
                tmax_f = c_to_f(daily_tmax[idx_today])
                tmin_f = c_to_f(daily_tmin[idx_today])
            elif len(daily_tmax) > 0:
                tmax_f = c_to_f(daily_tmax[0])
                tmin_f = c_to_f(daily_tmin[0])

            sunny_fraction_hist = historical_sunny_avg / 30.0
            if forecast_days < 30:
                remainder = 30 - forecast_days
                remainder_sunny = remainder * sunny_fraction_hist
                next_month_sunny_days = forecast_sunny_count + remainder_sunny
            else:
                next_month_sunny_days = forecast_sunny_count
        else:
            next_month_sunny_days = historical_sunny_avg
        next_month_sunny_days = max(0.0, min(float(next_month_sunny_days), 30.0))

        row_m = mdf[mdf["month"] == next_month] if not mdf.empty else pd.DataFrame()
        if not row_m.empty:
            est_next_month_day_length = row_m["day_length_hrs"].iloc[0]

        ref_temp = (tmax_f + tmin_f) / 2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
        niceness = compute_niceness(ref_temp, next_month_sunny_days, est_next_month_day_length)

        return {
            "city": city,
            "current_temp_f": current_temp_f,
            "next_month_sunny_days": next_month_sunny_days,
            "est_next_month_day_length": est_next_month_day_length,
            "niceness": niceness,
            "tmax_f": tmax_f,
            "tmin_f": tmin_f,
            "forecast_sunny_count": forecast_sunny_count,
            "forecast_days": forecast_days
        }, forecast_was_updated, forecast_df, forecast_err

    current_data_list = []
    done_count = 0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_current_data, c, l): c for c, l in city_list}
        for fut in as_completed(futures):
            row, was_updated, forecast_df, forecast_err = fut.result()
            city_name = row.get("city", "")
            if not forecast_df.empty:
                try:
                    store_data(conn, city_name, forecast_df, source="forecast")
                    existing_df = all_city_data.get(city_name, pd.DataFrame())
                    if not existing_df.empty:
                        all_city_data[city_name] = pd.concat([existing_df, forecast_df], ignore_index=True)
                    else:
                        all_city_data[city_name] = forecast_df
                except Exception as e:
                    errors += 1
                    insert_sync_city_log(conn, run_id, city_name, "forecast", "error", f"store_failed: {e}")
            elif forecast_err:
                errors += 1
                insert_sync_city_log(conn, run_id, city_name, "forecast", "error", forecast_err)

            current_data_list.append(row)
            if was_updated:
                forecast_updated += 1
                insert_sync_city_log(conn, run_id, city_name, "forecast", "updated", "refreshed")
            done_count += 1
            loading.update_current(done_count)
            if done_count % 50 == 0:
                append_sync_log(f"Forecast progress: {done_count}/{len(city_list)}")

    save_forecast_cache(forecast_cache)
    save_all_cities_ui_cache(current_data_list, monthly_dict)

    after = sync_status_snapshot(conn, city_names, forecast_cache)
    append_sync_log(
        "After sync: "
        f"estimated complete={after['hist_complete']}, missing={after['hist_missing']}; "
        f"forecast fresh={after['forecast_fresh']}, stale={after['forecast_stale']}"
    )

    c_meta.execute(
        """
        UPDATE sync_runs
        SET finished_at=?, status=?, historical_complete=?, historical_missing=?, forecast_fresh=?, forecast_stale=?,
            historical_updated=?, forecast_updated=?, errors=?, notes=?
        WHERE run_id=?
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            "ok" if errors == 0 else "partial",
            after["hist_complete"],
            after["hist_missing"],
            after["forecast_fresh"],
            after["forecast_stale"],
            historical_updated,
            forecast_updated,
            errors,
            f"window={START_DATE}..{END_DATE}",
            run_id,
        ),
    )
    conn.commit()
    # Mark daily sync complete only when estimated coverage is complete.
    # This preserves resume semantics when a run is partial due to rate limits.
    if should_sync and after["hist_missing"] == 0:
        set_sync_meta(conn, "last_daily_sync_date", today_key)
        append_sync_log("Daily sync marker set for today (estimated backfill complete).")
    elif should_sync:
        append_sync_log("Daily sync marker NOT set (estimated backfill still incomplete; resume allowed).")
    conn.close()
    loading.close()

    window = WeatherApp(current_data_list, monthly_dict, all_city_data, forecast_cache)
    now = datetime.now(timezone.utc)
    forecast_until = "N/A"
    for c in forecast_cache:
        fore_json = forecast_cache[c]['fore_json']
        if "daily" in fore_json and "time" in fore_json["daily"]:
            times = pd.to_datetime(fore_json["daily"]["time"])
            forecast_until = times.max().strftime("%Y-%m-%d")
            break

    window.set_itinerary_label(f"Updated as of: {now.strftime('%Y-%m-%d %H:%M UTC')}   Forecast until: {forecast_until}")
    
    # Initialize a dictionary to track fetch status of each city
    city_fetch_status = {}
    
    # ADDED: Check how many ZIP_CITIES ended up in monthly_dict/current_data_list:
    zip_cities_set = set(ZIP_CITIES.keys())
    loaded_cities = set(city for city in monthly_dict.keys())  # cities that have monthly data
    displayed_cities = set(city["city"] for city in current_data_list)  # cities in current data
    
    zip_cities_displayed = zip_cities_set.intersection(displayed_cities)
    zip_cities_not_displayed = zip_cities_set - displayed_cities
    
    # Update fetch status based on loaded_cities
    for city in zip_cities_set:
        if city in loaded_cities:
            city_fetch_status[city] = "Successfully fetched data"
        elif city in all_city_data:
            city_fetch_status[city] = "Fetch failed"
        else:
            city_fetch_status[city] = "Never called"
    
    print("\n=== ZIP CITIES REPORT ===")
    print(f"Total ZIP cities: {len(zip_cities_set)}")
    print(f"Displayed (loaded) ZIP cities: {len(zip_cities_displayed)}")
    if zip_cities_displayed:
        print("These ZIP cities are displayed:")
        for c in zip_cities_displayed:
            print(f"  - {c}")
    
    print(f"\nNot displayed ZIP cities: {len(zip_cities_not_displayed)}")
    if zip_cities_not_displayed:
        print("These ZIP cities are not displayed (no data or could not fetch):")
        for c in zip_cities_not_displayed:
            # Provide detailed reasons based on fetch status
            reason = city_fetch_status.get(c, "unknown reason")
            print(f"  - {c} (Reason: {reason})")
    
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
