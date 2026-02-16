import sys
import os
import sqlite3
import requests
import pandas as pd
import pickle
import time
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv

# QtWebEngine needs to be installed separately:
# pip install PyQt6-WebEngine
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEngineProfile, QWebEnginePage, QWebEngineSettings
from PyQt6.QtCore import QUrl
import folium

from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QLabel, QDialog, QProgressBar, QPushButton, QLineEdit, QHBoxLayout,
    QMessageBox, QCompleter, QScrollArea, QGroupBox, QAbstractScrollArea, QFormLayout, QSpinBox, QDoubleSpinBox
)
from PyQt6.QtCore import Qt, pyqtSlot
from PyQt6.QtGui import QPalette, QColor, QBrush, QCursor, QFont

DATABASE = "weather_data.db"
CACHE_FILE = "forecast_cache.pkl"
CACHE_MAX_AGE = timedelta(hours=1)
ALLCOUNTRIES_FILE = "allcountries.txt"

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

START_DATE = "2022-01-01"
END_DATE = "2023-12-31"
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

def init_db():
    """Initialize all database tables"""
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    
    # Weather data table
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_data (
            city TEXT,
            date TEXT,
            tmax_c REAL,
            tmin_c REAL,
            weathercode INT,
            sunrise TEXT,
            sunset TEXT,
            PRIMARY KEY (city, date)
        )
    """)
    
    # City coordinates table
    c.execute("""
        CREATE TABLE IF NOT EXISTS city_coords (
            city TEXT PRIMARY KEY,
            lat REAL,
            lon REAL
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

def get_city_coords(city_name):
    """Get coordinates for a city from the database"""
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute("SELECT lat, lon FROM city_coords WHERE city = ?", (city_name,))
    row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None

def have_data_for_city(conn, city):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM daily_data WHERE city=? AND date>=? AND date<=?", (city, START_DATE, END_DATE))
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

def store_data(conn, city, df):
    c = conn.cursor()
    for _, row in df.iterrows():
        c.execute("""
            INSERT OR IGNORE INTO daily_data (city, date, tmax_c, tmin_c, weathercode, sunrise, sunset)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (city, row["time"].strftime("%Y-%m-%d"), row["tmax_c"], row["tmin_c"], row["weathercode"], row["sunrise"].isoformat(), row["sunset"].isoformat()))
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
    df = pd.DataFrame({
        "time": fore_daily["time"],
        "tmax_c": fore_daily["temperature_2m_max"],
        "tmin_c": fore_daily["temperature_2m_min"],
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

        # Now count *all* sunny days without dividing by 2:
        sunny_days_avg = mdf["sunny_day"].sum()

        day_length_avg = mdf["day_length_hrs"].mean()
        monthly_data.append((m, avg_day_f_m, sunny_days_avg, day_length_avg, tmax_mean, tmin_mean))

    monthly_df = pd.DataFrame(
        monthly_data,
        columns=["month", "avg_day_f", "sunny_day", "day_length_hrs", "tmax_mean", "tmin_mean"]
    )
    return monthly_df

def month_name(m):
    return ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][m-1]

def fetch_current_forecast_data(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "weathercode,sunrise,sunset,temperature_2m_max,temperature_2m_min",
        "timezone": "UTC",
        "forecast_days": "16"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def fetch_current(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true",
        "daily": "weathercode,sunrise,sunset,temperature_2m_max,temperature_2m_min",
        "forecast_days": "16",
        "timezone": "UTC"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

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

def is_forecast_fresh(city: str, cache: dict, hours: int = 24) -> bool:
    """
    Returns True if 'city' forecast data in 'cache' was fetched within 'hours' hours.
    """
    now = datetime.now(timezone.utc)
    if city not in cache or 'time' not in cache[city]:
        return False
    last_fetch = cache[city]['time']
    return (now - last_fetch) < timedelta(hours=hours)

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

def update_allesco_niceness():
    """
    Updates ALLESCOOORDS.csv by adding niceness scores and color indicators
    while preserving existing latitude and longitude data.
    """
    import pandas as pd
    from datetime import datetime
    from PyQt6.QtGui import QColor
    
    print("Starting ALLESCOOORDS.csv update...")
    
    # Check if file exists
    if not os.path.exists('ALLESCOOORDS.csv'):
        print("Error: ALLESCOOORDS.csv not found")
        return
        
    try:
        # Read existing coordinate data
        df = pd.read_csv('ALLESCOOORDS.csv')
        
        # Add new columns if they don't exist
        if 'niceness' not in df.columns:
            df['niceness'] = None
        if 'color_r' not in df.columns:
            df['color_r'] = None
        if 'color_g' not in df.columns:
            df['color_g'] = None
        if 'color_b' not in df.columns:
            df['color_b'] = None
            
        # ... rest of the function implementation ...
    except Exception as e:
        print(f"Error updating ALLESCOOORDS.csv: {e}")

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
        all_keys = ["all cities"] + list(ZIP_CITIES.keys())
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

        # In WeatherApp.__init__, after creating the Continent tab:
        self.create_map_tab()

        # Update ALLESCOOORDS.csv with niceness scores and color indicators
        update_allesco_niceness()

        # Add this near the end of __init__
        print("Calling update_allesco_niceness()...")
        update_allesco_niceness()
        print("Finished update_allesco_niceness()")

    def set_itinerary_label(self, text):
        self.itinerary_info_label.setText(text)

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
        
        # Handle "all cities" case first
        if city_name.lower() == "all cities":
            conn = sqlite3.connect(DATABASE)
            try:
                for c in reversed(list(ZIP_CITIES.keys())):
                    if not have_data_for_city(conn, c):
                        print(f"Fetching data for {c}...")
                        self._add_single_city(c)
                    else:
                        print(f"Skipping {c} - data already exists.")
            finally:
                conn.close()
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
        if city_name in self.all_city_data:
            return

        conn = sqlite3.connect(DATABASE)
        try:
            if not have_data_for_city(conn, city_name):
                hist_json = fetch_historical(lat, lon, START_DATE, END_DATE)
                if "daily" in hist_json:
                    df = process_daily_data(hist_json["daily"])
                    store_data(conn, city_name, df)
                    self.all_city_data[city_name] = df
                else:
                    QMessageBox.warning(self, "Error", f"No historical data for {city_name}")
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
                fore_json = fetch_current_forecast_data(lat, lon)
                cur_json = fetch_current(lat, lon)
                self.forecast_cache[city_name] = {'fore_json': fore_json, 'cur_json': cur_json, 'time': datetime.now(timezone.utc)}
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

    def _add_single_city(self, city_name):
        print(f"Attempting to add city: '{city_name}'")
        
        # Debug prints to check data sources
        print(f"ALL_CITIES available: {len(ALL_CITIES)}")
        print(f"ZIP_CITIES available: {len(ZIP_CITIES)}")
        print(f"CITY_COORDS available: {len(CITY_COORDS)}")
        
        # Check file existence
        print(f"allcountries.txt exists: {os.path.exists('allcountries.txt')}")
        print(f"ziplist.txt exists: {os.path.exists('ziplist.txt')}")
        
        # Check if city is found in any source
        print(f"City in ALL_CITIES: {city_name in ALL_CITIES}")
        print(f"City in ZIP_CITIES: {city_name in ZIP_CITIES}")
        print(f"City in CITY_COORDS: {city_name in CITY_COORDS}")

        # Check if it's a full city,country string from ALL_CITIES
        if city_name in ALL_CITIES:
            lat, lon = ALL_CITIES[city_name]
        else:
            # Check if it's in ZIP_CITIES
            if city_name in ZIP_CITIES:
                # If we don't have coordinates yet, we need to fetch them
                if ZIP_CITIES[city_name] is None:
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

        if city_name in self.all_city_data:
            return

        # Fetch or use cached historical data
        conn = sqlite3.connect(DATABASE)
        try:
            if not have_data_for_city(conn, city_name):
                try:
                    hist_json = fetch_historical(lat, lon, START_DATE, END_DATE)
                    if "daily" in hist_json:
                        df = process_daily_data(hist_json["daily"])
                        store_data(conn, city_name, df)
                        self.all_city_data[city_name] = df
                    else:
                        return
                except requests.exceptions.RequestException:
                    # Silently fail on request errors
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
                fore_json = fetch_current_forecast_data(lat, lon)
                cur_json = fetch_current(lat, lon)
                self.forecast_cache[city_name] = {'fore_json': fore_json, 'cur_json': cur_json, 'time': datetime.now(timezone.utc)}
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
        save_forecast_cache(self.forecast_cache)

        # Refresh UI tables
        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()

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

    def generate_folium_map_html(self, filename):
        """
        Generates an HTML file containing a Folium map with all cities plotted.
        Returns the path to the generated HTML file.
        """
        # Create a map centered on a middle point with a clean style
        m = folium.Map(
            location=[20, 0],
            zoom_start=2,
            tiles='CartoDB positron',  # Clean, light style
            prefer_canvas=True  # Smoother rendering
        )
        
        # Add markers for each city
        for row in self.current_data_list:
            city = row["city"]
            if city in CITY_COORDS:
                lat, lon = CITY_COORDS[city]
                
                # Get niceness value for color
                niceness = row.get('niceness', 0)
                
                # Create color gradient from red (0) to green (1)
                color = f'#{int((1-niceness)*255):02x}{int(niceness*255):02x}00'
                
                # Format popup content with weather info and custom styling
                popup_content = f"""
                    <div style="font-family: Arial, sans-serif; padding: 10px;">
                        <h3 style="margin: 0 0 10px 0;">{CITY_COUNTRY.get(city, city)}</h3>
                        <p style="margin: 5px 0;">
                            <strong>Temperature:</strong> {row.get('tmax_f', 'N/A')}°F / {row.get('tmin_f', 'N/A')}°F
                        </p>
                        <p style="margin: 5px 0;">
                            <strong>Niceness:</strong> {row.get('niceness', 'N/A'):.2f}
                        </p>
                        <p style="margin: 5px 0;">
                            <strong>Sunny Days:</strong> {row.get('next_month_sunny_days', 'N/A'):.0f}/30
                        </p>
                    </div>
                """
                
                # Create circle marker with popup
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=8,
                    popup=folium.Popup(popup_content, max_width=300),
                    tooltip=city,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.7,
                    weight=2
                ).add_to(m)
        
        # Remove zoom controls for cleaner look
        m.options['zoomControl'] = False
        
        # Add custom CSS to style the map
        custom_css = """
        <style>
            .leaflet-popup-content-wrapper {
                background: rgba(255, 255, 255, 0.9);
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .leaflet-popup-tip {
                background: rgba(255, 255, 255, 0.9);
            }
        </style>
        """
        
        m.get_root().html.add_child(folium.Element(custom_css))
        
        # Save the map
        html_path = os.path.abspath(filename)
        m.save(html_path)
        # Save the map
        html_path = os.path.abspath(filename)
        m.save(html_path)
        return html_path

    def create_map_tab(self):
        """
        Creates a 'City Map' tab that displays all cities in self.current_data_list 
        on a Folium map.
        """
        # 1) Configure the default QWebEngineProfile to allow local + remote URLs and JavaScript
        profile = QWebEngineProfile.defaultProfile()
        profile.settings().setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)
        profile.settings().setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessFileUrls, True)
        profile.settings().setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, True)

        # 2) Generate the map HTML file
        map_html_path = self.generate_folium_map_html("cities_map.html")

        # 3) Create a QWebEnginePage using that profile, then tie it to a QWebEngineView
        page = QWebEnginePage(profile)
        self.map_view = QWebEngineView()
        self.map_view.setPage(page)

        # 4) Load the local HTML file
        self.map_view.setUrl(QUrl.fromLocalFile(map_html_path))

        # 5) Put the view inside a new QWidget/tab
        self.map_tab = QWidget()
        map_layout = QVBoxLayout(self.map_tab)
        map_layout.addWidget(self.map_view)
        self.tab_widget.addTab(self.map_tab, "City Map")

def main():
    # Add remote debugging port for QtWebEngine
    os.environ["QTWEBENGINE_REMOTE_DEBUGGING"] = "9222"
    
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

    init_db()
    
    # Load forecast cache first to check for additional cities
    forecast_cache = load_forecast_cache()
    
    # Just use hard-coded cities from CITY_COORDS for initial loading
    city_list = list(CITY_COORDS.items())
    
    loading = LoadingDialog(len(city_list))
    loading.show()

    conn = sqlite3.connect(DATABASE)

    print("Fetching historical data...")
    def fetch_city_data(city, latlon):
        print(f"Fetching data for {city}...")
        local_conn = sqlite3.connect(DATABASE)
        try:
            if not have_data_for_city(local_conn, city):
                lat, lon = latlon if isinstance(latlon, tuple) else (latlon, forecast_cache[city]['fore_json']['longitude'])
                hist_json = fetch_historical(lat, lon, START_DATE, END_DATE)
                if "daily" in hist_json:
                    df = process_daily_data(hist_json["daily"])
                    store_data(local_conn, city, df)
            df = load_data_from_db(local_conn, city)
            print(f"Completed {city}.")
            return city, df
        finally:
            local_conn.close()

    all_city_data = {}
    done_count = 0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_city_data, c, l): c for c, l in city_list}
        for fut in as_completed(futures):
            city_name, df = fut.result()
            all_city_data[city_name] = df
            done_count += 1
            loading.update_fetch(done_count)
            print(f"Progress: {done_count}/{len(city_list)} cities loaded.")

    forecast_cache = load_forecast_cache()

    print("Processing monthly data...")
    done_count = 0
    monthly_dict = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        def monthly_task(c):
            mdf = monthly_aggregates(all_city_data[c])
            mdf["niceness"] = mdf.apply(
                lambda r: compute_city_niceness(
                    r["tmax_mean"], 
                    r["tmin_mean"], 
                    r["sunny_day"], 
                    r["day_length_hrs"]
                ), 
                axis=1
            )  # ← Make sure this closing parenthesis is here
            return c, mdf

        futures = {executor.submit(monthly_task, c): c for c in all_city_data}
        for fut in as_completed(futures):
            city_name, mdf = fut.result()
            monthly_dict[city_name] = mdf
            done_count += 1
            loading.update_process(done_count)
            print(f"Processed monthly data for {city_name}. {done_count}/{len(all_city_data)}")

    print("Fetching current & forecast data...")
    def fetch_current_data(city, latlon):
        print(f"Fetching current/forecast data for {city}...")
        lat, lon = latlon
        local_df = all_city_data[city]
        now = datetime.now(timezone.utc)
        if city in forecast_cache:
            fore_json = forecast_cache[city]['fore_json']
            cur_json = forecast_cache[city]['cur_json']
        else:
            try:
                fore_json = fetch_current_forecast_data(lat, lon)
                cur_json = fetch_current(lat, lon)
                forecast_cache[city] = {'fore_json': fore_json, 'cur_json': cur_json, 'time': now}
            except:
                fore_json = {}
                cur_json = {}

        current_temp_f = float('nan')
        est_next_month_day_length = 12.0
        tmax_f = float('nan')
        tmin_f = float('nan')

        today = datetime.now(timezone.utc)
        next_month = (today.month % 12) + 1
        mdf = monthly_dict[city]
        target_month_row = mdf[mdf["month"]==next_month]
        if target_month_row.empty:
            historical_sunny_avg = 15.0
        else:
            historical_sunny_avg = target_month_row["sunny_day"].iloc[0]

        if "current_weather" in cur_json:
            current_temp_c = cur_json["current_weather"]["temperature"]
            current_temp_f = c_to_f(current_temp_c)

        forecast_sunny_count = 0
        forecast_days = 0
        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            forecast_df = process_forecast_daily_data(fore_json["daily"])
            combined_df = pd.concat([local_df, forecast_df], ignore_index=True)
            all_city_data[city] = combined_df

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

        row_m = mdf[mdf["month"] == next_month]
        if not row_m.empty:
            est_next_month_day_length = row_m["day_length_hrs"].iloc[0]

        ref_temp = (tmax_f+tmin_f)/2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
        niceness = compute_niceness(ref_temp, next_month_sunny_days, est_next_month_day_length)

        print(f"Done {city}.")
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
        }

    current_data_list = []
    done_count = 0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_current_data, c, l): c for c, l in city_list}
        for fut in as_completed(futures):
            row = fut.result()
            current_data_list.append(row)
            done_count += 1
            loading.update_current(done_count)
            print(f"Forecast progress: {done_count}/{len(city_list)}")

    save_forecast_cache(forecast_cache)
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