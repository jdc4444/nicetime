import sys, os, sqlite3, requests, pandas as pd, pickle, time, math, urllib.parse
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import plotly.graph_objects as go
from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QLabel, QDialog, QProgressBar, QPushButton, QLineEdit, QHBoxLayout,
    QMessageBox, QCompleter, QScrollArea, QGroupBox, QAbstractScrollArea, QFormLayout, QSpinBox, QDoubleSpinBox,
    QComboBox
)
from PyQt6.QtCore import Qt, pyqtSlot, QUrl
from PyQt6.QtGui import QPalette, QColor, QBrush, QCursor, QFont
from PyQt6.QtGui import QDesktopServices
import geopandas as gpd

DATABASE = "weather_data.db"
CACHE_FILE = "forecast_cache.pkl"
CACHE_MAX_AGE = timedelta(hours=1)
OPEN_METEO_API_KEY = "3jOyP7Ow5oWElmE5"
VISUAL_CROSSING_API_KEY = "24Y8MXUF6FEW32LABQTT3Z9P4"

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

# Add after CITY_COUNTRY dictionary and before CITY_COORDS
CITIES_BY_COUNTRY = {}
for city, country in CITY_COUNTRY.items():
    country_name = country.split(", ")[-1]
    if country_name not in CITIES_BY_COUNTRY:
        CITIES_BY_COUNTRY[country_name] = []
    CITIES_BY_COUNTRY[country_name].append(city)

CITY_COORDS = {
    "Honolulu": (21.3069, -157.8583),
    "Todos Santos": (23.4469, -110.2231),
    "Tenerife": (28.2916, -16.6291),
    "Los Angeles": (34.0522, -118.2437),
    "Medellin": (6.2442, -75.5812),
    "Mexico City": (19.4326, -99.1332),
    "Rio de Janeiro": (-22.9068, -43.1729),
    "Fortaleza": (-3.7319, -38.5267),
    "Abu Dhabi": (24.4539, 54.3773),
    "Las Vegas": (36.1699, -115.1398),
    "Tucson": (32.2226, -110.9747),
    "Buenos Aires": (-34.6037, -58.3816),
    "Sydney": (-33.8688, 151.2093),
    "Sao Paolo": (-23.5505, -46.6333),
    "Berlin": (52.5200, 13.4050),
    "Copenhagen": (55.6761, 12.5683),
    "Santa Fe": (35.6870, -105.9378),
    "Amsterdam": (52.3676, 4.9041),
    "New York": (40.7128, -74.0060),
    "London": (51.5074, -0.1278),
    "Tokyo": (35.6762, 139.6503),
    "Barcelona": (41.3851, 2.1734),
    "Athens": (37.9838, 23.7275),
    "Valencia": (39.4699, -0.3763),
    "Shanghai": (31.2304, 121.4737),
    "Austin": (30.2672, -97.7431),
    "Milos": (36.7260, 24.4443),
    "Santiago": (-33.4489, -70.6693),
    "Lisbon": (38.7223, -9.1393),
    "El Paso": (31.7619, -106.4850),
    "Palm Springs": (33.8303, -116.5453)
}

ALL_CITIES = {}
if os.path.exists("allcountries.txt"):
    import csv
    with open("allcountries.txt", "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if len(row) < 11: continue
            country_code, place_name = row[0].strip(), row[2].strip()
            if not place_name: continue
            lat_str, lon_str = (row[9].strip(), row[10].strip()) if len(row) > 10 else ("", "")
            if not lat_str or not lon_str: continue
            try:
                lat, lon = float(lat_str), float(lon_str)
            except:
                continue
            key = f"{place_name}, {country_code}"
            if key not in CITY_COORDS:
                ALL_CITIES[key] = (lat, lon)

START_DATE, END_DATE = "2022-01-01", "2023-12-31"
SUNNY_CODES = [0, 1, 2]

ZIP_CITIES = {}
if os.path.exists("ziplist.txt"):
    with open("ziplist.txt", "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            try:
                city, country, continent, zipcode = line.strip().split(',')
                key = f"{city}, {country}"
                ZIP_CITIES[key] = None
            except:
                continue

def c_to_f(c): return (c*9/5)+32

def have_data_for_city(conn, city):
    """Check if we have historical data for a city in the database."""
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM daily_data WHERE city = ?", (city,))
    count = c.fetchone()[0]
    return count > 0

def init_db():
    print("\n[DB] Initializing database...")
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
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
    conn.commit()
    conn.close()
    print("[DB] Database initialized successfully")

def fetch_historical_visual_crossing(lat, lon, start_date, end_date):
    print(f"\n[VISUAL-CROSSING] Fetching historical data for coordinates ({lat}, {lon}) from {start_date} to {end_date}")
    base = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
    location = f"{lat},{lon}"
    path = f"{urllib.parse.quote(location)}/{start_date}/{end_date}"
    params = {
        "unitGroup": "metric",
        "include": "days",
        "elements": "datetime,tempmax,tempmin,icon,sunrise,sunset",
        "contentType": "json",
        "key": VISUAL_CROSSING_API_KEY,
    }
    url = base + path + "?" + urllib.parse.urlencode(params, safe=",")
    
    try:
        print(f"[VISUAL-CROSSING] Making API request to {url}")
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        # Convert Visual Crossing format to match Open-Meteo format
        daily_data = {
            "time": [],
            "temperature_2m_max": [],
            "temperature_2m_min": [],
            "weathercode": [],
            "sunrise": [],
            "sunset": []
        }
        
        # Map Visual Crossing weather icons to Open-Meteo weather codes
        icon_to_code = {
            "clear-day": 0,
            "partly-cloudy-day": 1,
            "cloudy": 2,
            "rain": 3,
            "snow": 4,
            "fog": 5,
            "wind": 6,
            "thunder-rain": 7
        }
        
        print(f"[VISUAL-CROSSING] Processing {len(data.get('days', []))} days of data")
        for day in data.get("days", []):
            daily_data["time"].append(day["datetime"])
            daily_data["temperature_2m_max"].append(day["tempmax"])
            daily_data["temperature_2m_min"].append(day["tempmin"])
            daily_data["weathercode"].append(icon_to_code.get(day["icon"], 0))
            daily_data["sunrise"].append(day["sunrise"])
            daily_data["sunset"].append(day["sunset"])
        
        print(f"[VISUAL-CROSSING] Successfully converted data to Open-Meteo format")
        return {"daily": daily_data}
    except requests.exceptions.HTTPError as e:
        print(f"[VISUAL-CROSSING] HTTP Error: {e}")
        if e.response.status_code == 429:
            print("[VISUAL-CROSSING] Rate limit exceeded")
        raise
    except Exception as e:
        print(f"[VISUAL-CROSSING] Error: {e}")
        raise

def fetch_historical(lat, lon, start_date, end_date):
    print(f"\n[FETCH] Historical data for coordinates ({lat}, {lon}) from {start_date} to {end_date}")
    url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,weathercode,sunrise,sunset",
        "timezone": "UTC",
        "apikey": OPEN_METEO_API_KEY
    }
    max_retries = 5
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            print(f"[OPEN-METEO] Attempt {attempt + 1}/{max_retries} to fetch historical data")
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            print(f"[OPEN-METEO] Successfully retrieved historical data with {len(data['daily']['time'])} days")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    print(f"[OPEN-METEO] Rate limited (429), trying Visual Crossing...")
                    try:
                        vc_data = fetch_historical_visual_crossing(lat, lon, start_date, end_date)
                        print(f"[VISUAL-CROSSING] Successfully retrieved fallback data")
                        return vc_data
                    except Exception as vc_error:
                        print(f"[VISUAL-CROSSING] Failed: {vc_error}")
                        print(f"[OPEN-METEO] Waiting {base_delay*(2**attempt)} seconds before retry...")
                        time.sleep(base_delay*(2**attempt))
                        continue
            print(f"[OPEN-METEO] HTTP Error: {e}")
            raise
        except Exception as e:
            print(f"[OPEN-METEO] Error: {e}")
            raise

def store_data(conn,city,df):
    print(f"\n[STORE] Storing data for {city} with {len(df)} days")
    c=conn.cursor()
    for _,row in df.iterrows():
        c.execute("INSERT OR IGNORE INTO daily_data (city,date,tmax_c,tmin_c,weathercode,sunrise,sunset) VALUES (?,?,?,?,?,?,?)",
        (city,row["time"].strftime("%Y-%m-%d"),row["tmax_c"],row["tmin_c"],row["weathercode"],row["sunrise"].isoformat(),row["sunset"].isoformat()))
    conn.commit()
    print(f"[SUCCESS] Stored data for {city}")

def process_daily_data(daily_data):
    df = pd.DataFrame({
        "time": daily_data["time"],
        "tmax_c": daily_data["temperature_2m_max"],
        "tmin_c": daily_data["temperature_2m_min"],
        "weathercode": daily_data["weathercode"],
        "sunrise": daily_data["sunrise"],
        "sunset": daily_data["sunset"]
    })
    
    # Handle time column
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d")
    
    # Handle sunrise/sunset with flexible format parsing
    def parse_datetime(dt_str):
        try:
            # Try Open-Meteo format first
            return pd.to_datetime(dt_str, format="%Y-%m-%dT%H:%M")
        except ValueError:
            try:
                # Try Visual Crossing format (HH:MM:SS)
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                return pd.to_datetime(f"{today}T{dt_str}", format="%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # If all else fails, return None
                return None
    
    df["sunrise"] = df["sunrise"].apply(parse_datetime)
    df["sunset"] = df["sunset"].apply(parse_datetime)
    
    return df

def process_forecast_daily_data(fore_daily):
    df = pd.DataFrame({
        "time": fore_daily["time"],
        "tmax_c": fore_daily["temperature_2m_max"],
        "tmin_c": fore_daily["temperature_2m_min"],
        "weathercode": fore_daily["weathercode"],
        "sunrise": fore_daily["sunrise"],
        "sunset": fore_daily["sunset"]
    })
    # Explicitly specify date format for each datetime column
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d")
    df["sunrise"] = pd.to_datetime(df["sunrise"], format="%Y-%m-%dT%H:%M")
    df["sunset"] = pd.to_datetime(df["sunset"], format="%Y-%m-%dT%H:%M")
    return df

def load_data_from_db(conn,city):
    print(f"\n[LOAD] Loading data from database for {city}")
    c=conn.cursor()
    c.execute("SELECT date,tmax_c,tmin_c,weathercode,sunrise,sunset FROM daily_data WHERE city=? AND date>=? AND date<=? ORDER BY date",(city,START_DATE,END_DATE))
    rows=c.fetchall()
    df=pd.DataFrame(rows,columns=["date","tmax_c","tmin_c","weathercode","sunrise","sunset"])
    df["time"]=pd.to_datetime(df["date"])
    df["sunrise"]=pd.to_datetime(df["sunrise"])
    df["sunset"]=pd.to_datetime(df["sunset"])
    print(f"[SUCCESS] Loaded {len(df)} days of data for {city}")
    return df

def monthly_aggregates(df):
    df["month"]=df["time"].dt.month
    df["tmax_f"]=c_to_f(df["tmax_c"]);df["tmin_f"]=c_to_f(df["tmin_c"])
    df["avg_day_f"]=(df["tmax_f"]+df["tmin_f"])/2
    df["sunny_day"]=df["weathercode"].apply(lambda w:1 if w in SUNNY_CODES else 0)
    df["day_length_hrs"]=(df["sunset"]-df["sunrise"]).dt.total_seconds()/3600.0
    monthly_data=[]
    for m in range(1,13):
        mdf=df[df["month"]==m]
        if mdf.empty:
            monthly_data.append((m,float('nan'),float('nan'),float('nan'),float('nan'),float('nan')))
            continue
        avg_day_f_m=mdf["avg_day_f"].mean()
        tmax_mean=mdf["tmax_f"].mean()
        tmin_mean=mdf["tmin_f"].mean()
        sunny_days_avg=mdf["sunny_day"].sum()
        day_length_avg=mdf["day_length_hrs"].mean()
        monthly_data.append((m,avg_day_f_m,sunny_days_avg,day_length_avg,tmax_mean,tmin_mean))
    monthly_df=pd.DataFrame(monthly_data,columns=["month","avg_day_f","sunny_day","day_length_hrs","tmax_mean","tmin_mean"])
    return monthly_df

def month_name(m): return ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][m-1]

def fetch_current_forecast_data(lat,lon):
    print(f"\n[FETCH] Current forecast for coordinates ({lat}, {lon})")
    url="https://api.open-meteo.com/v1/forecast"
    params={"latitude":lat,"longitude":lon,
    "daily":"weathercode,sunrise,sunset,temperature_2m_max,temperature_2m_min",
    "timezone":"UTC","forecast_days":"16",
    "apikey":OPEN_METEO_API_KEY}
    
    max_retries = 5
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = base_delay * (2 ** attempt)
                print(f"[FETCH] Retry attempt {attempt + 1}/{max_retries}, waiting {delay} seconds...")
                time.sleep(delay)
            
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            print(f"[SUCCESS] Retrieved forecast data with {len(data['daily']['time'])} days")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit error
                if attempt < max_retries - 1:
                    print(f"[FETCH] Rate limited (429), trying Visual Crossing...")
                    try:
                        vc_data = fetch_current_visual_crossing(lat, lon)
                        print(f"[VISUAL-CROSSING] Successfully retrieved fallback data")
                        return vc_data
                    except Exception as vc_error:
                        print(f"[VISUAL-CROSSING] Failed: {vc_error}")
                        print(f"[FETCH] Waiting {base_delay*(2**attempt)} seconds before retry...")
                        time.sleep(base_delay*(2**attempt))
                        continue
            print(f"[ERROR] Failed to fetch forecast data: {e}")
            raise
        except Exception as e:
            print(f"[ERROR] Failed to fetch forecast data: {e}")
            raise

def fetch_current_visual_crossing(lat, lon):
    print(f"\n[VISUAL-CROSSING] Fetching current weather for coordinates ({lat}, {lon})")
    base = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
    location = f"{lat},{lon}"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"{urllib.parse.quote(location)}/{today}/{today}"
    params = {
        "unitGroup": "metric",
        "include": "days,current",
        "elements": "datetime,tempmax,tempmin,icon,sunrise,sunset,temp",
        "contentType": "json",
        "key": VISUAL_CROSSING_API_KEY,
    }
    url = base + path + "?" + urllib.parse.urlencode(params, safe=",")
    
    try:
        print(f"[VISUAL-CROSSING] Making API request to {url}")
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        # Convert Visual Crossing format to match Open-Meteo format
        current_weather = {
            "temperature": data.get("currentConditions", {}).get("temp", 0),
            "time": data.get("currentConditions", {}).get("datetime", "")
        }
        
        daily_data = {
            "time": [],
            "temperature_2m_max": [],
            "temperature_2m_min": [],
            "weathercode": [],
            "sunrise": [],
            "sunset": []
        }
        
        # Map Visual Crossing weather icons to Open-Meteo weather codes
        icon_to_code = {
            "clear-day": 0,
            "partly-cloudy-day": 1,
            "cloudy": 2,
            "rain": 3,
            "snow": 4,
            "fog": 5,
            "wind": 6,
            "thunder-rain": 7
        }
        
        for day in data.get("days", []):
            daily_data["time"].append(day["datetime"])
            daily_data["temperature_2m_max"].append(day["tempmax"])
            daily_data["temperature_2m_min"].append(day["tempmin"])
            daily_data["weathercode"].append(icon_to_code.get(day["icon"], 0))
            # Format sunrise/sunset times to match Open-Meteo format
            sunrise = day.get("sunrise", "")
            sunset = day.get("sunset", "")
            if sunrise and sunset:
                daily_data["sunrise"].append(f"{day['datetime']}T{sunrise}")
                daily_data["sunset"].append(f"{day['datetime']}T{sunset}")
            else:
                daily_data["sunrise"].append("")
                daily_data["sunset"].append("")
        
        print(f"[VISUAL-CROSSING] Successfully retrieved data")
        return {
            "current_weather": current_weather,
            "daily": daily_data
        }
    except Exception as e:
        print(f"[VISUAL-CROSSING] Error: {e}")
        raise

def fetch_current(lat,lon):
    print(f"\n[FETCH] Current weather for coordinates ({lat}, {lon})")
    url="https://api.open-meteo.com/v1/forecast"
    params={"latitude":lat,"longitude":lon,"current_weather":"true",
    "daily":"weathercode,sunrise,sunset,temperature_2m_max,temperature_2m_min",
    "forecast_days":"16","timezone":"UTC",
    "apikey":OPEN_METEO_API_KEY}
    
    max_retries = 5
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            # Add a small delay between requests to avoid rate limiting
            if attempt > 0:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"[FETCH] Retry attempt {attempt + 1}/{max_retries}, waiting {delay} seconds...")
                time.sleep(delay)
            
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            if "current_weather" in data:
                print(f"[SUCCESS] Retrieved current weather: {data['current_weather']['temperature']}°C")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit error
                if attempt < max_retries - 1:
                    print(f"[FETCH] Rate limited (429), trying Visual Crossing...")
                    try:
                        vc_data = fetch_current_visual_crossing(lat, lon)
                        print(f"[VISUAL-CROSSING] Successfully retrieved fallback data")
                        return vc_data
                    except Exception as vc_error:
                        print(f"[VISUAL-CROSSING] Failed: {vc_error}")
                        print(f"[FETCH] Waiting {base_delay*(2**attempt)} seconds before retry...")
                        time.sleep(base_delay*(2**attempt))
                        continue
            print(f"[ERROR] Failed to fetch current weather: {e}")
            raise
        except Exception as e:
            print(f"[ERROR] Failed to fetch current weather: {e}")
            raise

def compute_daytime_avg_temp(tmax_f,tmin_f):
    daytime_low_f=tmax_f-(tmax_f-tmin_f)/4.0
    return (tmax_f+daytime_low_f)/2.0

def compute_niceness(temp_f,sunny_days,day_length_hrs):
    if temp_f<50 or temp_f>105: temp_score=0.0
    elif 50<=temp_f<70: temp_score=(temp_f-50)/20.0*0.5
    elif 70<=temp_f<75: temp_score=0.5+((temp_f-70)/5.0)*0.5
    elif 75<=temp_f<=85: temp_score=1.0
    elif 85<temp_f<=90: temp_score=1.0-((temp_f-85)/5.0)*0.5
    else: temp_score=0.5-((temp_f-90)/15.0)*0.5
    sunny_score=max(0.0,min(sunny_days/30.0,1.0))
    day_length_score=max(0.0,min(day_length_hrs/24.0,1.0))
    sun_day_score=(sunny_score+day_length_score)/2.0
    return 0.5*temp_score+0.5*sun_day_score

def compute_city_niceness(tmax_f,tmin_f,sunny_days,day_length_hrs):
    daytime_avg_f=compute_daytime_avg_temp(tmax_f,tmin_f)
    return compute_niceness(daytime_avg_f,sunny_days,day_length_hrs)

def load_forecast_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE,'rb') as f:
                cache = pickle.load(f)
                print(f"\n[CACHE] Loaded forecast cache with {len(cache)} cities")
                for city, data in cache.items():
                    if 'time' in data:
                        age = datetime.now(timezone.utc) - data['time']
                        print(f"[CACHE] {city}: {age.total_seconds()/3600:.1f} hours old")
                        if 'niceness' in data:
                            print(f"[CACHE] {city}: Niceness score {data['niceness']:.2f}")
                return cache
        except (pickle.UnpicklingError, EOFError, AttributeError) as e:
            print(f"\n[CACHE] Error loading cache file: {e}")
            print("[CACHE] Cache file appears to be corrupted. Deleting and starting fresh.")
            try:
                os.remove(CACHE_FILE)
            except Exception as e:
                print(f"[CACHE] Error deleting corrupted cache file: {e}")
    print("\n[CACHE] No forecast cache found or cache was corrupted")
    return {}

def save_forecast_cache(cache):
    with open(CACHE_FILE,'wb') as f:
        pickle.dump(cache,f)

def is_forecast_fresh(city,cache,hours=24):
    now=datetime.now(timezone.utc)
    if city not in cache or 'time' not in cache[city]: 
        print(f"[CACHE] {city}: No cached data")
        return False
    last_fetch=cache[city]['time']
    age = now-last_fetch
    is_fresh = age < timedelta(hours=hours)
    print(f"[CACHE] {city}: Data is {age.total_seconds()/3600:.1f} hours old, {'fresh' if is_fresh else 'stale'}")
    return is_fresh

class LoadingDialog(QDialog):
    def __init__(self,max_cities):
        super().__init__()
        self.setWindowTitle("Loading Weather Data...")
        layout=QVBoxLayout()
        self.label_fetch=QLabel("Fetching historical data...")
        self.pb_fetch=QProgressBar();self.pb_fetch.setMaximum(max_cities);self.pb_fetch.setValue(0)
        self.label_process=QLabel("Processing monthly data...")
        self.pb_process=QProgressBar();self.pb_process.setMaximum(max_cities);self.pb_process.setValue(0)
        self.label_current=QLabel("Fetching current & forecast data...")
        self.pb_current=QProgressBar();self.pb_current.setMaximum(max_cities);self.pb_current.setValue(0)
        layout.addWidget(self.label_fetch);layout.addWidget(self.pb_fetch)
        layout.addWidget(self.label_process);layout.addWidget(self.pb_process)
        layout.addWidget(self.label_current);layout.addWidget(self.pb_current)
        self.setLayout(layout);self.resize(400,200)
    def update_fetch(self,v): self.pb_fetch.setValue(v)
    def update_process(self,v): self.pb_process.setValue(v)
    def update_current(self,v): self.pb_current.setValue(v)

class NumericTableWidgetItem(QTableWidgetItem):
    def __init__(self,value):
        if pd.isna(value):
            self.numeric_val=float('nan')
            display_value="N/A"
        else:
            self.numeric_val=float(value)
            display_value=f"{self.numeric_val:.4f}"
        super().__init__(display_value)
    def __lt__(self,other):
        if isinstance(other,NumericTableWidgetItem):
            return self.numeric_val<other.numeric_val
        return super().__lt__(other)

def is_nice_strict(avg_temp,sunny_days,day_length):
    return (avg_temp>70) and (sunny_days>12) and (day_length>10)

def is_nice_light(avg_temp,sunny_days,day_length):
    return (avg_temp>60) and (sunny_days>10) and (day_length>10)

def highlight_cell(item,avg_temp,sunny_days,day_length):
    item.setBackground(QBrush(QColor(255,255,255)))
    item.setForeground(QBrush(Qt.GlobalColor.black))
    if pd.isna(avg_temp) or pd.isna(sunny_days) or pd.isna(day_length): return
    if avg_temp>90:
        item.setBackground(QBrush(QColor(255,0,0)))
        item.setForeground(QBrush(QColor(255,255,255)))
    elif avg_temp<50:
        item.setBackground(QBrush(QColor(0,0,255)))
        item.setForeground(QBrush(QColor(255,255,255)))
    elif is_nice_strict(avg_temp,sunny_days,day_length):
        item.setBackground(QBrush(QColor(255,255,0)))  # Pure yellow
        item.setForeground(QBrush(QColor(0,0,0)))
    elif is_nice_light(avg_temp,sunny_days,day_length):
        item.setBackground(QBrush(QColor(255,255,153)))  # Light yellow
        item.setForeground(QBrush(QColor(0,0,0)))

class WeatherApp(QWidget):
    def __init__(self,current_data_list,monthly_dict,all_city_data,forecast_cache):
        super().__init__()
        self.setWindowTitle("Weather Overview")
        self.current_data_list=current_data_list
        self.monthly_dict=monthly_dict
        self.all_city_data=all_city_data
        self.forecast_cache=forecast_cache
        self.current_detail_city=None
        self.default_city_detail="New York"
        self.pinned_city="New York"
        self.recent_cities=[]
        font = QFont()
        font.setPointSize(12)
        self.setFont(font)
        layout = QVBoxLayout()
        
        # Add Map Button at the top
        map_button_layout = QHBoxLayout()
        self.map_button = QPushButton("Show 3D Globe")
        self.map_button.setFont(QFont("", 14, QFont.Weight.Bold))
        self.map_button.setStyleSheet("padding: 10px;")
        self.map_button.clicked.connect(self.create_plotly_globe)
        map_button_layout.addStretch()
        map_button_layout.addWidget(self.map_button)
        map_button_layout.addStretch()
        layout.addLayout(map_button_layout)

        self.tab_widget = QTabWidget()
        
        # Current Tab
        self.current_tab=QWidget()
        current_layout=QVBoxLayout()
        self.current_table=self.create_current_table(self.current_data_list,self.monthly_dict)
        current_layout.addWidget(self.current_table)
        self.current_tab.setLayout(current_layout)
        self.last_sorted_column_current=None
        self.last_sort_order_current=Qt.SortOrder.DescendingOrder
        self.last_sorted_column_monthly=None
        self.last_sort_order_monthly=Qt.SortOrder.DescendingOrder
        self.current_table.horizontalHeader().sectionClicked.connect(self.on_current_header_clicked)
        self.current_table.horizontalHeader().sectionDoubleClicked.connect(self.on_current_header_double_clicked)
        self.current_table.cellDoubleClicked.connect(self.on_current_table_double_click)
        self.current_table.sortItems(0,Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_current=0
        self.last_sort_order_current=Qt.SortOrder.DescendingOrder

        # Monthly Tab
        self.monthly_tab=QWidget()
        monthly_layout=QVBoxLayout()
        self.monthly_table=self.create_monthly_table(self.monthly_dict)
        monthly_layout.addWidget(self.monthly_table)
        self.monthly_tab.setLayout(monthly_layout)
        self.monthly_table.horizontalHeader().sectionClicked.connect(self.on_monthly_header_clicked)
        self.monthly_table.horizontalHeader().sectionDoubleClicked.connect(self.on_monthly_header_double_clicked)
        self.monthly_table.cellDoubleClicked.connect(self.on_monthly_table_double_click)

        # Detail Tab
        self.detail_tab=QWidget()
        self.detail_layout=QVBoxLayout()
        self.detail_layout.setSpacing(20)
        self.detail_label=QLabel("Select a city in the other tabs to view details.")
        detail_font=QFont();detail_font.setPointSize(16);detail_font.setBold(True)
        self.detail_label.setFont(detail_font)
        self.detail_layout.addWidget(self.detail_label)
        self.remove_city_button=QPushButton("Remove City");self.remove_city_button.setEnabled(False)
        self.remove_city_button.setFont(QFont("",14,QFont.Weight.Bold))
        self.remove_city_button.setStyleSheet("padding: 10px;")
        self.remove_city_button.clicked.connect(self.remove_current_city)
        detail_container=QWidget();detail_container.setLayout(self.detail_layout)
        detail_scroll=QScrollArea();detail_scroll.setWidget(detail_container)
        detail_scroll.setWidgetResizable(True)
        detail_main_layout=QVBoxLayout()
        detail_main_layout.addWidget(detail_scroll)
        self.detail_tab.setLayout(detail_main_layout)

        # Itinerary Tab
        self.itinerary_tab=QWidget()
        itinerary_layout=QVBoxLayout()
        self.itinerary_info_label=QLabel("")
        itinerary_info_layout=QHBoxLayout()
        itinerary_info_layout.addStretch();itinerary_info_layout.addWidget(self.itinerary_info_label)
        itinerary_layout.addLayout(itinerary_info_layout)
        self.itinerary_table=QTableWidget()
        self.itinerary_table.setColumnCount(11)
        itinerary_headers=["Month"]+[f"Rank {i}" for i in range(1,11)]
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

        self.tab_widget.addTab(self.current_tab,"Current Weather")
        self.tab_widget.addTab(self.monthly_tab,"Monthly Calendar")
        self.tab_widget.addTab(self.detail_tab,"City Detail")
        self.tab_widget.addTab(self.itinerary_tab,"Itinerary")

        layout.addWidget(self.tab_widget)
        
        # Add city section
        add_city_layout = QHBoxLayout()
        
        # Country dropdown
        self.country_combo = QComboBox()
        self.country_combo.addItem("Select Country", None)
        for country in sorted(CITIES_BY_COUNTRY.keys()):
            self.country_combo.addItem(country, country)
        self.country_combo.currentIndexChanged.connect(self.on_country_selected)
        
        # City dropdown
        self.city_combo = QComboBox()
        self.city_combo.setEnabled(False)
        
        # Add buttons
        self.add_city_button = QPushButton("Add City")
        self.add_city_button.clicked.connect(self.add_city)
        self.add_country_button = QPushButton("Add Country")
        self.add_country_button.clicked.connect(self.add_country)
        self.add_country_button.setEnabled(False)
        
        # Add widgets to layout
        add_city_layout.addWidget(QLabel("Country:"))
        add_city_layout.addWidget(self.country_combo)
        add_city_layout.addWidget(QLabel("City:"))
        add_city_layout.addWidget(self.city_combo)
        add_city_layout.addWidget(self.add_city_button)
        add_city_layout.addWidget(self.add_country_button)
        
        layout.addLayout(add_city_layout)
        self.setLayout(layout)
        self.resize(1600,900)
        self.current_table.cellClicked.connect(self.on_current_table_click)
        self.monthly_table.cellClicked.connect(self.on_monthly_table_click)
        self.tab_widget.setCurrentIndex(0)
        self.show_city_detail(self.default_city_detail, switch_tab=False)
        self.pref_min_temp=60
        self.pref_max_temp=80
        self.pref_temp_weight=0.5
        self.use_preferences=False
        self.create_preferences_tab()
        self.tab_widget.addTab(self.preferences_tab,"Preferences")
        self.update_all_niceness_and_refresh()

        # Add after the CITY_COUNTRY dictionary
        self.CITIES_BY_COUNTRY = {}
        for city, country in CITY_COUNTRY.items():
            country_name = country.split(", ")[-1]
            if country_name not in self.CITIES_BY_COUNTRY:
                self.CITIES_BY_COUNTRY[country_name] = []
            self.CITIES_BY_COUNTRY[country_name].append(city)

        # Add to the WeatherApp class, after the add_city_layout definition
        self.create_add_city_layout()

    def set_itinerary_label(self,text):
        self.itinerary_info_label.setText(text)

    def remove_current_city(self):
        if not self.current_detail_city: return
        city=self.current_detail_city
        if city in self.all_city_data: del self.all_city_data[city]
        if city in self.monthly_dict: del self.monthly_dict[city]
        self.current_data_list=[c for c in self.current_data_list if c["city"]!=city]
        if city in self.forecast_cache: del self.forecast_cache[city]
        if self.pinned_city==city: self.pinned_city=None
        self.recent_cities=[c for c in self.recent_cities if c!=city]
        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()
        if self.recent_cities:
            self.show_city_detail(self.recent_cities[-1])
        elif self.pinned_city:
            self.show_city_detail(self.pinned_city)
        else:
            for i in reversed(range(self.detail_layout.count())):
                w=self.detail_layout.itemAt(i).widget()
                if w and w not in [self.detail_label,self.remove_city_button]:
                    w.setParent(None)
            self.detail_label.setText("City removed. Select another city.")
            self.remove_city_button.setEnabled(False)
        self.current_detail_city=None

    def refresh_itinerary_tab(self):
        monthly_data={c:mdf.set_index("month") for c,mdf in self.monthly_dict.items()}
        top_cities_by_month={}
        for month in range(1,13):
            city_scores=[]
            for city,mdf in monthly_data.items():
                if month in mdf.index:
                    nic=mdf.at[month,"niceness"]
                    if not pd.isna(nic): city_scores.append((city,nic))
            city_scores.sort(key=lambda x:x[1],reverse=True)
            top_cities_by_month[month]=city_scores[:10]
        self.itinerary_table.clear()
        self.itinerary_table.setColumnCount(11)
        itinerary_headers=["Month"]+[f"Rank {i}" for i in range(1,11)]
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
            for i,mon in enumerate(range(1,13)):
                month_item=QTableWidgetItem(month_name(mon))
                self.itinerary_table.setItem(i,0,month_item)
                for col_clear in range(1,11):
                    self.itinerary_table.setItem(i,col_clear,None)
                for j,(city,nic_val) in enumerate(top_cities_by_month[mon],start=1):
                    row_m=monthly_data[city].loc[mon]
                    if row_m is not None:
                        avg_f=row_m["avg_day_f"];sunny=row_m["sunny_day"];hrs=row_m["day_length_hrs"]
                    else:
                        avg_f,sunny,hrs=float('nan'),float('nan'),float('nan')
                    city_str=f"{CITY_COUNTRY.get(city,city)} ({nic_val:.2f})"
                    city_item=QTableWidgetItem(city_str)
                    city_item.setData(Qt.ItemDataRole.UserRole,city)
                    city_item.setForeground(QBrush(Qt.GlobalColor.blue))
                    fnt=city_item.font();fnt.setUnderline(True);city_item.setFont(fnt)
                    highlight_cell(city_item,avg_f,sunny,hrs)
                    self.itinerary_table.setItem(i,j,city_item)
        else:
            self.itinerary_table.setRowCount(1)
            self.itinerary_table.setColumnCount(1)
            self.itinerary_table.setHorizontalHeaderLabels(["Itinerary"])
            no_data_item=QTableWidgetItem("No itinerary data available.")
            self.itinerary_table.setItem(0,0,no_data_item)
        self.itinerary_table.cellClicked.connect(self.on_itinerary_table_click)

    def abbreviated_monthly_text(self,tmax,tmin,sunny):
        tmax_str="N/A" if pd.isna(tmax) else f"{tmax:.0f}"
        tmin_str="N/A" if pd.isna(tmin) else f"{tmin:.0f}"
        sunny_str="N/A" if pd.isna(sunny) else f"{int(sunny)}/30"
        return f"High: {tmax_str}F Low: {tmin_str}F Sun: {sunny_str}"

    def create_current_table(self,data,monthly_dict):
        headers=["Niceness","City","Temp (H/DL/L)","Sunny Next 16 Days","Sunny Next 30 Days","Day Length"]
        table=QTableWidget()
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
        now=datetime.now(timezone.utc)
        next_month=(now.month%12)+1
        for i,row in enumerate(data):
            tmax_f=row.get("tmax_f",float('nan'))
            tmin_f=row.get("tmin_f",float('nan'))
            daytime_low=(tmax_f+tmin_f)/2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else float('nan')
            if pd.isna(tmax_f) or pd.isna(tmin_f):
                triple_str="N/A";triple_val=float('nan')
            else:
                triple_str=f"{tmax_f:.0f}F/{daytime_low:.0f}F/{tmin_f:.0f}F"
                triple_val=(tmax_f+daytime_low+tmin_f)/3.0
            nice_val=row["niceness"]
            nice_str=f"{nice_val:.2f}" if not pd.isna(nice_val) else "N/A"
            nice_item=NumericTableWidgetItem(nice_val);nice_item.setText(nice_str)
            city_text=CITY_COUNTRY.get(row["city"],row["city"])
            city_item=QTableWidgetItem(city_text)
            city_item.setData(Qt.ItemDataRole.UserRole,row["city"])
            city_item.setForeground(QBrush(Qt.GlobalColor.blue))
            fnt=city_item.font();fnt.setUnderline(True);city_item.setFont(fnt)
            forecast_sunny_count=row.get("forecast_sunny_count",0)
            forecast_item=NumericTableWidgetItem(forecast_sunny_count);forecast_item.setText(str(forecast_sunny_count))
            s_val=row["next_month_sunny_days"]
            s_display=f"{s_val:.0f}" if not pd.isna(s_val) else "N/A"
            next_30_item=NumericTableWidgetItem(s_val);next_30_item.setText(s_display)
            dl_val=row.get("est_next_month_day_length",float('nan'))
            dl_display=f"{dl_val:.0f}" if not pd.isna(dl_val) else "N/A"
            dl_item=NumericTableWidgetItem(dl_val);dl_item.setText(dl_display)
            triple_item=NumericTableWidgetItem(triple_val);triple_item.setText(triple_str)
            table.setItem(i,0,nice_item)
            table.setItem(i,1,city_item)
            table.setItem(i,2,triple_item)
            table.setItem(i,3,forecast_item)
            table.setItem(i,4,next_30_item)
            table.setItem(i,5,dl_item)
            mdf=monthly_dict.get(row["city"],pd.DataFrame())
            row_m=mdf[mdf["month"]==next_month]
            if not row_m.empty:
                avg_f=row_m["avg_day_f"].iloc[0]
                sunny_v=row_m["sunny_day"].iloc[0]
                hrs=row_m["day_length_hrs"].iloc[0]
            else:
                avg_f,sunny_v,hrs=float('nan'),float('nan'),float('nan')
            for c in range(table.columnCount()):
                it=table.item(i,c)
                if it: highlight_cell(it,avg_f,sunny_v,hrs)
        table.setSortingEnabled(True)
        return table

    def create_monthly_table(self,data):
        headers=["City"]+[month_name(m) for m in range(1,13)]
        table=QTableWidget()
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
        city_list=list(data.keys())
        for i,city in enumerate(city_list):
            monthly_df=data[city]
            city_item=QTableWidgetItem(CITY_COUNTRY.get(city,city))
            city_item.setData(Qt.ItemDataRole.UserRole,city)
            city_item.setForeground(QBrush(Qt.GlobalColor.blue))
            fnt=city_item.font();fnt.setUnderline(True);city_item.setFont(fnt)
            table.setItem(i,0,city_item)
            for col,m in enumerate(range(1,13),start=1):
                row_m=monthly_df[monthly_df["month"]==m]
                if row_m.empty:
                    item=NumericTableWidgetItem(float('nan'));item.setText("N/A")
                    table.setItem(i,col,item)
                else:
                    tmax=row_m["tmax_mean"].iloc[0]
                    tmin=row_m["tmin_mean"].iloc[0]
                    sunny=row_m["sunny_day"].iloc[0]
                    avg_f=row_m["avg_day_f"].iloc[0]
                    hrs=row_m["day_length_hrs"].iloc[0]
                    niceness=row_m["niceness"].iloc[0]
                    txt=self.abbreviated_monthly_text(tmax,tmin,sunny)
                    sort_val=niceness
                    item=NumericTableWidgetItem(sort_val);item.setText(txt)
                    highlight_cell(item,avg_f,sunny,hrs)
                    fitem=item.font();fitem.setBold(False);item.setFont(fitem)
                    table.setItem(i,col,item)
        table.setSortingEnabled(True)
        return table

    @pyqtSlot(int)
    def on_current_header_clicked(self,col):
        self.current_table.sortItems(col,Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_current=col
        self.last_sort_order_current=Qt.SortOrder.DescendingOrder

    @pyqtSlot(int)
    def on_current_header_double_clicked(self,col):
        if self.last_sorted_column_current==col and self.last_sort_order_current==Qt.SortOrder.DescendingOrder:
            self.current_table.sortItems(col,Qt.SortOrder.AscendingOrder)
            self.last_sort_order_current=Qt.SortOrder.AscendingOrder
        else:
            if self.last_sorted_column_current==col and self.last_sort_order_current==Qt.SortOrder.AscendingOrder:
                self.current_table.sortItems(col,Qt.SortOrder.DescendingOrder)
                self.last_sort_order_current=Qt.SortOrder.DescendingOrder
            else:
                self.current_table.sortItems(col,Qt.SortOrder.AscendingOrder)
                self.last_sorted_column_current=col
                self.last_sort_order_current=Qt.SortOrder.AscendingOrder

    @pyqtSlot(int)
    def on_monthly_header_clicked(self,col):
        self.monthly_table.sortItems(col,Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_monthly=col
        self.last_sort_order_monthly=Qt.SortOrder.DescendingOrder

    @pyqtSlot(int)
    def on_monthly_header_double_clicked(self,col):
        if self.last_sorted_column_monthly==col and self.last_sort_order_monthly==Qt.SortOrder.DescendingOrder:
            self.monthly_table.sortItems(col,Qt.SortOrder.AscendingOrder)
            self.last_sort_order_monthly=Qt.SortOrder.AscendingOrder
        else:
            if self.last_sorted_column_monthly==col and self.last_sort_order_monthly==Qt.SortOrder.AscendingOrder:
                self.monthly_table.sortItems(col,Qt.SortOrder.DescendingOrder)
                self.last_sort_order_monthly=Qt.SortOrder.DescendingOrder
            else:
                self.monthly_table.sortItems(col,Qt.SortOrder.AscendingOrder)
                self.last_sorted_column_monthly=col
                self.last_sort_order_monthly=Qt.SortOrder.AscendingOrder

    @pyqtSlot(int,int)
    def on_monthly_table_double_click(self,row,column):
        city_item=self.monthly_table.item(row,0)
        if city_item:
            city_key=city_item.data(Qt.ItemDataRole.UserRole)
            if city_key in self.monthly_dict:
                self.show_city_detail(city_key)

    @pyqtSlot(int,int)
    def on_current_table_double_click(self,row,column):
        city_item=self.current_table.item(row,1)
        if city_item:
            city_key=city_item.data(Qt.ItemDataRole.UserRole)
            if city_key in self.monthly_dict:
                self.show_city_detail(city_key)

    @pyqtSlot(int,int)
    def on_current_table_click(self,row,column):
        if column==1:
            city_item=self.current_table.item(row,column)
            if city_item is not None:
                city_key=city_item.data(Qt.ItemDataRole.UserRole)
                if city_key in self.monthly_dict:
                    self.show_city_detail(city_key)
                else:
                    self.update_detail_tab("<b>No monthly data found for this city.</b>",enable_remove=False)

    @pyqtSlot(int,int)
    def on_monthly_table_click(self,row,column):
        if column==0:
            city_item=self.monthly_table.item(row,column)
            if city_item is not None:
                city_key=city_item.data(Qt.ItemDataRole.UserRole)
                if city_key in self.monthly_dict:
                    self.show_city_detail(city_key)
                else:
                    self.update_detail_tab("<b>No monthly data found for this city.</b>",enable_remove=False)

    @pyqtSlot(int,int)
    def on_itinerary_table_click(self,row,column):
        if column>0:
            item=self.itinerary_table.item(row,column)
            if item is not None:
                city_key=item.data(Qt.ItemDataRole.UserRole)
                if city_key and city_key in self.monthly_dict:
                    self.show_city_detail(city_key)
                else:
                    self.update_detail_tab("<b>No monthly data found for this city.</b>",enable_remove=False)

    def update_detail_tab(self,text,enable_remove=False):
        for i in reversed(range(self.detail_layout.count())):
            widget=self.detail_layout.itemAt(i).widget()
            if widget and widget not in [self.detail_label,self.remove_city_button]:
                widget.setParent(None)
        self.detail_label.setText(text)
        self.remove_city_button.setEnabled(enable_remove)

    def show_city_detail(self, city, switch_tab=True):
        """
        Display city detail information
        Args:
            city: City name to show details for
            switch_tab: Whether to switch to the detail tab (default: True)
        """
        self.current_detail_city=city
        self.remove_city_button.setEnabled(True)
        self.detail_label.setText("")
        if city in self.recent_cities: self.recent_cities.remove(city)
        self.recent_cities.append(city)
        for i in reversed(range(self.detail_layout.count())):
            widget=self.detail_layout.itemAt(i).widget()
            if widget and widget not in [self.detail_label,self.remove_city_button]:
                widget.setParent(None)
        cities_to_show=[city]
        if self.pinned_city and self.pinned_city!=city: cities_to_show.append(self.pinned_city)
        hbox=QHBoxLayout()
        for idx,detail_city in enumerate(cities_to_show):
            city_widget=self.create_city_detail_widget(detail_city,is_main=(detail_city==city))
            hbox.addWidget(city_widget)
        container=QWidget();container.setLayout(hbox)
        self.detail_layout.addWidget(container)
        if switch_tab:
            self.tab_widget.setCurrentIndex(2)

    def create_city_detail_widget(self,city,is_main=False):
        cur_data=next((x for x in self.current_data_list if x["city"]==city),None)
        def fmt_or_na(val,fmt="{:.0f}"):
            if pd.isna(val): return "N/A"
            return fmt.format(val)
        now=datetime.now(timezone.utc)
        next_month=(now.month%12)+1
        mdf=self.monthly_dict.get(city,pd.DataFrame())
        row_m=mdf[mdf["month"]==next_month]
        summary_box=QGroupBox()
        fnt=summary_box.font();fnt.setBold(True);summary_box.setFont(fnt)
        summary_layout=QVBoxLayout()
        title_label=QLabel(city);title_font=QFont();title_font.setPointSize(60);title_font.setBold(True)
        title_label.setFont(title_font)
        summary_layout.addWidget(title_label)
        if cur_data is not None:
            tmax_str=fmt_or_na(cur_data.get("tmax_f",float('nan')))
            tmin_str=fmt_or_na(cur_data.get("tmin_f",float('nan')))
            sunny_val=cur_data.get("next_month_sunny_days",float('nan'))
            sunny_str=fmt_or_na(sunny_val)
            if sunny_str!="N/A": sunny_str=f"{sunny_str}/30"
            dl_val=row_m["day_length_hrs"].iloc[0] if not row_m.empty else cur_data.get("est_next_month_day_length",12.0)
            dl_str=fmt_or_na(dl_val)
            nic_val=cur_data["niceness"]
            nic_str="N/A" if pd.isna(nic_val) else f"{nic_val:.2f}"
            data_font=QFont();data_font.setPointSize(30)
            data_layout=QVBoxLayout()
            temp_label=QLabel(f"High/Low: {tmax_str}F / {tmin_str}F");temp_label.setFont(data_font);data_layout.addWidget(temp_label)
            sunny_label=QLabel(f"Expected Next 30 Sunny Days: {sunny_str}");sunny_label.setFont(data_font);data_layout.addWidget(sunny_label)
            length_label=QLabel(f"Next Month Day Length: {dl_str} hours");length_label.setFont(data_font);data_layout.addWidget(length_label)
            nice_label=QLabel(f"Today's Niceness: {nic_str}");nice_label.setFont(data_font);data_layout.addWidget(nice_label)
            summary_layout.addLayout(data_layout)
        else:
            data_label=QLabel("<b>No current data found for this city.</b>")
            summary_layout.addWidget(data_label)
        pin_button=QPushButton("Pin City" if self.pinned_city!=city else "Unpin City")
        pin_button.setFont(QFont("",12))
        def toggle_pin():
            if self.pinned_city==city: self.pinned_city=None
            else: self.pinned_city=city
            self.show_city_detail(city)
        pin_button.clicked.connect(toggle_pin)
        summary_layout.addWidget(pin_button)
        if city==self.current_detail_city:
            remove_button=QPushButton("Remove City");remove_button.setFont(QFont("",12))
            remove_button.clicked.connect(self.remove_current_city)
            summary_layout.addWidget(remove_button)
        summary_box.setLayout(summary_layout)
        monthly_box=QGroupBox()
        fnt=monthly_box.font();fnt.setBold(True);monthly_box.setFont(fnt)
        monthly_box_layout=QVBoxLayout()
        if city in self.monthly_dict:
            mdf=self.monthly_dict[city]
            if mdf.empty:
                no_data_label=QLabel("No monthly data available.")
                monthly_box_layout.addWidget(no_data_label)
            else:
                current_month=datetime.now(timezone.utc).month
                row_m=mdf[mdf["month"]==current_month]
                if not row_m.empty:
                    avg_f=row_m["avg_day_f"].iloc[0]
                    sunny=row_m["sunny_day"].iloc[0]
                    hrs=row_m["day_length_hrs"].iloc[0]
                    tmax=row_m["tmax_mean"].iloc[0]
                    tmin=row_m["tmin_mean"].iloc[0]
                    month_container=QWidget()
                    month_layout=QVBoxLayout();month_container.setLayout(month_layout)
                    month_label=QLabel(month_name(current_month))
                    month_font=QFont();month_font.setPointSize(30);month_font.setBold(True)
                    month_label.setFont(month_font)
                    month_layout.addWidget(month_label)
                    data_font=QFont();data_font.setPointSize(30)
                    temp_label=QLabel(f"High/Low: {tmax:.0f}F / {tmin:.0f}F");temp_label.setFont(data_font);month_layout.addWidget(temp_label)
                    sunny_label=QLabel(f"Average Sunny Days: {sunny:.0f}/30");sunny_label.setFont(data_font);month_layout.addWidget(sunny_label)
                    length_label=QLabel(f"Day Length: {hrs:.1f} hours");length_label.setFont(data_font);month_layout.addWidget(length_label)
                    base_style="QWidget { border: 2px solid black; padding: 10px; %s cursor: pointer; } QLabel {border:none;%s}"
                    if is_nice_strict(avg_f,sunny,hrs): month_container.setStyleSheet(base_style%("background-color:#FFFF00;",""))  # Pure yellow
                    elif is_nice_light(avg_f,sunny,hrs): month_container.setStyleSheet(base_style%("background-color:#FFFF99;",""))  # Light yellow
                    elif avg_f>90: month_container.setStyleSheet(base_style%("background-color:#FF0000;","color:white;"))
                    elif avg_f<50: month_container.setStyleSheet(base_style%("background-color:#0000FF;","color:white;"))
                    else: month_container.setStyleSheet(base_style%("background-color:white;",""))
                    
                    # Add click handling for the month container
                    month_container.mousePressEvent = lambda e, c=city, m=current_month: self.on_month_container_click(c, m)
                    month_container.setCursor(Qt.CursorShape.PointingHandCursor)
                    
                    monthly_box_layout.addWidget(month_container)
                else:
                    no_data_label=QLabel("No data available for current month.")
                    monthly_box_layout.addWidget(no_data_label)
        else:
            no_data_label=QLabel("No monthly data found for this city.")
            monthly_box_layout.addWidget(no_data_label)
        monthly_box.setLayout(monthly_box_layout)
        city_vlayout=QVBoxLayout()
        city_vlayout.addWidget(summary_box)
        city_vlayout.addWidget(monthly_box)
        city_widget=QWidget();city_widget.setLayout(city_vlayout)
        summary_box.setStyleSheet("QGroupBox { border:none;}")
        monthly_box.setStyleSheet("QGroupBox { border:none;}")
        city_widget.setStyleSheet("QWidget#mainContainer { border:3px solid #e0f7ff; border-radius:5px; background-color:white; padding:10px;} QGroupBox { background-color:white;} QWidget { background-color:white; border:none;}")  
        city_widget.setObjectName("mainContainer")
        return city_widget

    def on_month_container_click(self, city, month):
        """Handle click on month container in city detail view"""
        self.tab_widget.setCurrentIndex(1)  # Switch to Monthly Calendar tab
        
        # Find the row for the city
        for row in range(self.monthly_table.rowCount()):
            city_item = self.monthly_table.item(row, 0)
            if city_item and city_item.data(Qt.ItemDataRole.UserRole) == city:
                # Select the row and scroll to it
                self.monthly_table.selectRow(row)
                self.monthly_table.scrollToItem(city_item)
                
                # Scroll horizontally to the month column (month + 1 because first column is city name)
                month_item = self.monthly_table.item(row, month)
                if month_item:
                    self.monthly_table.scrollToItem(month_item)
                break

    def create_add_city_layout(self):
        add_city_layout = QHBoxLayout()
        
        # Country dropdown
        self.country_combo = QComboBox()
        self.country_combo.addItem("Select Country", None)
        for country in sorted(self.CITIES_BY_COUNTRY.keys()):
            self.country_combo.addItem(country, country)
        self.country_combo.currentIndexChanged.connect(self.on_country_selected)
        
        # City dropdown
        self.city_combo = QComboBox()
        self.city_combo.setEnabled(False)
        
        # Add buttons
        self.add_city_button = QPushButton("Add City")
        self.add_city_button.clicked.connect(self.add_city)
        self.add_country_button = QPushButton("Add Country")
        self.add_country_button.clicked.connect(self.add_country)
        self.add_country_button.setEnabled(False)
        
        # Add widgets to layout
        add_city_layout.addWidget(QLabel("Country:"))
        add_city_layout.addWidget(self.country_combo)
        add_city_layout.addWidget(QLabel("City:"))
        add_city_layout.addWidget(self.city_combo)
        add_city_layout.addWidget(self.add_city_button)
        add_city_layout.addWidget(self.add_country_button)
        
        return add_city_layout

    def on_country_selected(self, index):
        country = self.country_combo.currentData()
        self.city_combo.clear()
        self.city_combo.setEnabled(False)
        self.add_country_button.setEnabled(False)
        
        if country:
            self.city_combo.addItem("Select City", None)
            for city in sorted(self.CITIES_BY_COUNTRY[country]):
                self.city_combo.addItem(city, city)
            self.city_combo.setEnabled(True)
            self.add_country_button.setEnabled(True)

    def add_country(self):
        country = self.country_combo.currentData()
        if not country:
            return
        
        print(f"\n[ADD] Starting to add all cities in {country}...")
        conn = sqlite3.connect(DATABASE)
        try:
            cities = CITIES_BY_COUNTRY[country]
            total_cities = len(cities)
            print(f"[ADD] Found {total_cities} cities to process")
            
            for i, city in enumerate(cities, 1):
                print(f"\n[ADD] Processing city {i}/{total_cities}: {city}")
                self._add_single_city(city)
                
            print(f"\n[ADD] Finished processing all cities in {country}")
            QMessageBox.information(self, "Success", f"Added all cities in {country}!")
        finally:
            conn.close()

    def add_city(self):
        city_name = self.city_combo.currentData()
        if not city_name:
            return
        
        self._add_single_city(city_name)
        QMessageBox.information(self, "Success", f"City {city_name} added successfully!")
        self.show_city_detail(city_name)

    def _add_single_city(self,city_name):
        print(f"\n[ADD] Adding single city: {city_name}")
        if city_name in ALL_CITIES:
            lat,lon=ALL_CITIES[city_name]
            print(f"[ADD] Using coordinates from ALL_CITIES: ({lat}, {lon})")
        else:
            if city_name in ZIP_CITIES:
                if ZIP_CITIES[city_name] is None:
                    try:
                        url=f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1"
                        r=requests.get(url);r.raise_for_status()
                        data=r.json()
                        if data.get("results"):
                            result=data["results"][0]
                            lat,lon=result["latitude"],result["longitude"]
                            ZIP_CITIES[city_name]=(lat,lon)
                        else: return
                    except: return
                else:
                    lat,lon=ZIP_CITIES[city_name]
            else:
                city_key=next((k for k in CITY_COORDS.keys() if k.lower()==city_name.lower()),None)
                if city_key:
                    city_name=city_key
                    lat,lon=CITY_COORDS[city_key]
                else:
                    return
        if city_name in self.all_city_data: return
        conn=sqlite3.connect(DATABASE)
        try:
            if not have_data_for_city(conn,city_name):
                hist_json=fetch_historical(lat,lon,START_DATE,END_DATE)
                if "daily" in hist_json:
                    df=process_daily_data(hist_json["daily"])
                    store_data(conn,city_name,df)
                    self.all_city_data[city_name]=df
                else:
                    QMessageBox.warning(self,"Error",f"No historical data for {city_name}")
                    return
            else:
                df=load_data_from_db(conn,city_name)
                self.all_city_data[city_name]=df
        finally:
            conn.close()
        mdf=monthly_aggregates(self.all_city_data[city_name])
        mdf["niceness"]=mdf.apply(lambda r: compute_city_niceness(r["tmax_mean"],r["tmin_mean"],r["sunny_day"],r["day_length_hrs"]),axis=1)
        self.monthly_dict[city_name]=mdf
        today=datetime.now(timezone.utc)
        next_month=(today.month%12)+1
        target_month_row=mdf[mdf["month"]==next_month]
        historical_sunny_avg=15.0 if target_month_row.empty else target_month_row["sunny_day"].iloc[0]
        if city_name in self.forecast_cache:
            fore_json=self.forecast_cache[city_name]['fore_json']
            cur_json=self.forecast_cache[city_name]['cur_json']
        else:
            try:
                fore_json=fetch_current_forecast_data(lat,lon)
                cur_json=fetch_current(lat,lon)
                self.forecast_cache[city_name]={'fore_json':fore_json,'cur_json':cur_json,'time':datetime.now(timezone.utc)}
            except:
                fore_json={}
                cur_json={}
        current_temp_f=float('nan');tmax_f=float('nan');tmin_f=float('nan');forecast_sunny_count=0;forecast_days=0
        if "current_weather" in cur_json:
            current_temp_c=cur_json["current_weather"]["temperature"]
            current_temp_f=c_to_f(current_temp_c)
        if "daily" in fore_json and "temperature_2m_max" in fore_json["daily"]:
            forecast_df=process_forecast_daily_data(fore_json["daily"])
            combined_df=pd.concat([self.all_city_data[city_name],forecast_df],ignore_index=True)
            self.all_city_data[city_name]=combined_df
            daily_dates=pd.to_datetime(fore_json["daily"]["time"])
            daily_tmax=fore_json["daily"]["temperature_2m_max"]
            daily_tmin=fore_json["daily"]["temperature_2m_min"]
            daily_codes=fore_json["daily"]["weathercode"]
            forecast_sunny_count=sum(1 for c in daily_codes if c in SUNNY_CODES)
            forecast_days=len(daily_codes)
            today_str=today.strftime("%Y-%m-%d")
            idx_today=None
            for i2,d in enumerate(daily_dates):
                if d.strftime("%Y-%m-%d")==today_str:
                    idx_today=i2;break
            if idx_today is not None:
                tmax_f=c_to_f(daily_tmax[idx_today])
                tmin_f=c_to_f(daily_tmin[idx_today])
            else:
                if len(daily_tmax)>0:
                    tmax_f=c_to_f(daily_tmax[0])
                    tmin_f=c_to_f(daily_tmin[0])
            sunny_fraction_hist=historical_sunny_avg/30.0
            if forecast_days<30:
                remainder=30-forecast_days
                remainder_sunny=remainder*sunny_fraction_hist
                next_month_sunny_days=forecast_sunny_count+remainder_sunny
            else:
                next_month_sunny_days=forecast_sunny_count
        else:
            next_month_sunny_days=historical_sunny_avg
        row_m=mdf[mdf["month"]==next_month]
        est_next_month_day_length=row_m["day_length_hrs"].iloc[0] if not row_m.empty else 12.0
        ref_temp=(tmax_f+tmin_f)/2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
        niceness=compute_niceness(ref_temp,next_month_sunny_days,est_next_month_day_length)
        new_city_current={"city":city_name,"current_temp_f":current_temp_f,"next_month_sunny_days":next_month_sunny_days,"est_next_month_day_length":est_next_month_day_length,"niceness":niceness,"tmax_f":tmax_f,"tmin_f":tmin_f,"forecast_sunny_count":forecast_sunny_count,"forecast_days":forecast_days}
        self.current_data_list.append(new_city_current)
        save_forecast_cache(self.forecast_cache)
        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()
        QMessageBox.information(self,"Success",f"City {city_name} added successfully!")
        self.show_city_detail(city_name)

    def refresh_current_table(self):
        new_table=self.create_current_table(self.current_data_list,self.monthly_dict)
        self.current_table.horizontalHeader().sectionClicked.disconnect()
        self.current_table.horizontalHeader().sectionDoubleClicked.disconnect()
        self.current_table.cellDoubleClicked.disconnect()
        self.tab_widget.widget(0).layout().replaceWidget(self.current_table,new_table)
        self.current_table.deleteLater()
        self.current_table=new_table
        self.current_table.horizontalHeader().sectionClicked.connect(self.on_current_header_clicked)
        self.current_table.horizontalHeader().sectionDoubleClicked.connect(self.on_current_header_double_clicked)
        self.current_table.cellClicked.connect(self.on_current_table_click)
        self.current_table.cellDoubleClicked.connect(self.on_current_table_double_click)
        self.current_table.sortItems(0,Qt.SortOrder.DescendingOrder)
        self.last_sorted_column_current=0
        self.last_sort_order_current=Qt.SortOrder.DescendingOrder

    def refresh_monthly_table(self):
        new_table=self.create_monthly_table(self.monthly_dict)
        self.monthly_table.horizontalHeader().sectionClicked.disconnect()
        self.monthly_table.horizontalHeader().sectionDoubleClicked.disconnect()
        self.monthly_table.cellDoubleClicked.disconnect()
        self.monthly_table.cellClicked.disconnect()
        self.tab_widget.widget(1).layout().replaceWidget(self.monthly_table,new_table)
        self.monthly_table.deleteLater()
        self.monthly_table=new_table
        self.monthly_table.horizontalHeader().sectionClicked.connect(self.on_monthly_header_clicked)
        self.monthly_table.horizontalHeader().sectionDoubleClicked.connect(self.on_monthly_header_double_clicked)
        self.monthly_table.cellClicked.connect(self.on_monthly_table_click)
        self.monthly_table.cellDoubleClicked.connect(self.on_monthly_table_double_click)

    def create_preferences_tab(self):
        self.preferences_tab=QWidget()
        layout=QFormLayout()
        self.min_temp_spin=QSpinBox();self.min_temp_spin.setRange(0,150);self.min_temp_spin.setValue(self.pref_min_temp)
        layout.addRow("Min Ideal Temp (°F):",self.min_temp_spin)
        self.max_temp_spin=QSpinBox();self.max_temp_spin.setRange(0,150);self.max_temp_spin.setValue(self.pref_max_temp)
        layout.addRow("Max Ideal Temp (°F):",self.max_temp_spin)
        self.temp_weight_spin=QDoubleSpinBox();self.temp_weight_spin.setRange(0.0,1.0);self.temp_weight_spin.setSingleStep(0.05);self.temp_weight_spin.setValue(self.pref_temp_weight)
        layout.addRow("Temperature Weight (0.0 - 1.0):",self.temp_weight_spin)
        apply_button=QPushButton("Apply");apply_button.clicked.connect(self.on_apply_preferences)
        layout.addRow(apply_button)
        self.preferences_tab.setLayout(layout)

    def on_apply_preferences(self):
        self.pref_min_temp=self.min_temp_spin.value()
        self.pref_max_temp=self.max_temp_spin.value()
        self.pref_temp_weight=self.temp_weight_spin.value()
        self.use_preferences=True
        self.update_all_niceness_and_refresh()

    def update_all_niceness_and_refresh(self):
        for row in self.current_data_list:
            tmax_f=row["tmax_f"]
            tmin_f=row["tmin_f"]
            sunny_days=row["next_month_sunny_days"]
            day_length=row["est_next_month_day_length"]
            row["niceness"]=self.compute_adjusted_niceness(tmax_f,tmin_f,sunny_days,day_length) if self.use_preferences else compute_city_niceness(tmax_f,tmin_f,sunny_days,day_length)
        for city,mdf in self.monthly_dict.items():
            def calc_niceness(r):
                if self.use_preferences:
                    return self.compute_adjusted_niceness(
                        r["tmax_mean"],
                        r["tmin_mean"],
                        r["sunny_day"],
                        r["day_length_hrs"]
                    )
                else:
                    return compute_city_niceness(
                        r["tmax_mean"],
                        r["tmin_mean"],
                        r["sunny_day"],
                        r["day_length_hrs"]
                    )
            mdf["niceness"] = mdf.apply(calc_niceness, axis=1)
        self.refresh_current_table()
        self.refresh_monthly_table()
        self.refresh_itinerary_tab()
        if self.current_detail_city: self.show_city_detail(self.current_detail_city)

    def compute_adjusted_niceness(self,tmax_f,tmin_f,sunny_days,day_length_hrs):
        if self.pref_min_temp>self.pref_max_temp:
            self.pref_min_temp,self.pref_max_temp=self.pref_max_temp,self.pref_min_temp
        daytime_avg_f=compute_daytime_avg_temp(tmax_f,tmin_f)
        if daytime_avg_f<50 or daytime_avg_f>105: temp_score=0.0
        elif 50<=daytime_avg_f<self.pref_min_temp: temp_score=(daytime_avg_f-50)/float(self.pref_min_temp-50)
        elif self.pref_min_temp<=daytime_avg_f<=self.pref_max_temp: temp_score=1.0
        else:
            temp_score=1.0-(daytime_avg_f-self.pref_max_temp)/float(105-self.pref_max_temp)
        temp_score=max(0.0,min(temp_score,1.0))
        sunny_score=max(0.0,min(sunny_days/30.0,1.0))
        day_length_score=max(0.0,min(day_length_hrs/24.0,1.0))
        sun_day_score=(sunny_score+day_length_score)/2.0
        niceness=(self.pref_temp_weight*temp_score)+((1.0-self.pref_temp_weight)*sun_day_score)
        return max(0.0,min(niceness,1.0))

    def latlon_to_cartesian(self, lat, lon, radius=6371):
        """Convert (lat, lon) in degrees to 3D (x, y, z)."""
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        x = radius * math.cos(lat_rad) * math.cos(lon_rad)
        y = radius * math.cos(lat_rad) * math.sin(lon_rad)
        z = radius * math.sin(lat_rad)
        return x, y, z

    def niceness_to_color(self, niceness):
        """Convert niceness [0,1] to a gradient from Blue (0) to Yellow (1)."""
        n = max(0.0, min(1.0, niceness))
        r = int(0 + n * 255)
        g = int(0 + n * 255)
        b = int(255 - n * 255)
        return f"rgb({r},{g},{b})"

    def build_earth_surface(self):
        """Create a sphere 'surface' for Earth with better styling."""
        lats = np.linspace(-90, 90, 30)
        lons = np.linspace(-180, 180, 60)
        x_vals, y_vals, z_vals = [], [], []
        for lat in lats:
            row_x, row_y, row_z = [], [], []
            for lon in lons:
                x, y, z = self.latlon_to_cartesian(lat, lon)
                row_x.append(x)
                row_y.append(y)
                row_z.append(z)
            x_vals.append(row_x)
            y_vals.append(row_y)
            z_vals.append(row_z)
        return go.Surface(
            x=x_vals,
            y=y_vals,
            z=z_vals,
            colorscale=[[0, 'rgb(255,255,255)'], [1, 'rgb(255,255,255)']],
            opacity=0.0,
            showscale=False,
            hoverinfo='skip'
        )

    def get_top_cities_by_niceness(self, n=None):
        """Get cities sorted by niceness score (optionally top N)."""
        # Sort cities by niceness score, handling NaN values
        sorted_cities = sorted(
            self.current_data_list,
            key=lambda x: float('-inf') if pd.isna(x.get('niceness')) else x.get('niceness', 0),
            reverse=True
        )
        if n is None:
            return sorted_cities
        return sorted_cities[:n]

    def _resolve_globe_coords(self, city_name):
        """Resolve coordinates for a city using local maps, DB hints, then geocoding fallback."""
        if city_name in CITY_COORDS:
            return CITY_COORDS[city_name]

        z = ZIP_CITIES.get(city_name)
        if isinstance(z, tuple) and len(z) == 2:
            return z

        if city_name in ALL_CITIES:
            return ALL_CITIES[city_name]

        # Try matching by place-name prefix in ALL_CITIES, e.g. "Paris, France" -> "Paris, FR".
        place = city_name.split(",")[0].strip()
        if place:
            prefix = f"{place}, "
            for k, coords in ALL_CITIES.items():
                if k.startswith(prefix):
                    return coords

        # Last resort: geocode and cache for this session.
        try:
            url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1"
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            data = r.json()
            if data.get("results"):
                result = data["results"][0]
                coords = (result["latitude"], result["longitude"])
                ZIP_CITIES[city_name] = coords
                return coords
        except Exception:
            pass
        return None

    def create_rotating_globe_figure(self):
        """Build an interactive 3D globe figure with improved styling and better city handling."""
        earth_surface = self.build_earth_surface()
        city_scatter = []

        # Plot all loaded cities by niceness rank.
        top_cities = self.get_top_cities_by_niceness(n=None)
        print(f"\n[GLOBE] Displaying {len(top_cities)} cities by niceness score")

        # Add city markers with enhanced hover info
        plotted = 0
        for rank, row in enumerate(top_cities, start=1):
            city_name = row["city"]
            coords = self._resolve_globe_coords(city_name)
            if not coords:
                continue

            lat, lon = coords
            niceness_val = row.get("niceness", 0.0)
            color_str = self.niceness_to_color(niceness_val)
            
            # Get weather details for hover text
            tmax_f = row.get("tmax_f", float('nan'))
            tmin_f = row.get("tmin_f", float('nan'))
            sunny_days = row.get("next_month_sunny_days", float('nan'))
            day_length = row.get("est_next_month_day_length", float('nan'))
            
            # Format hover text with city rank
            hover_text = (
                f"<b>Rank {rank}: {CITY_COUNTRY.get(city_name, city_name)}</b><br>"
                f"High/Low: {tmax_f:.1f}°F/{tmin_f:.1f}°F<br>"
                f"Sunny Days: {sunny_days:.1f}/30<br>"
                f"Day Length: {day_length:.1f}h<br>"
                f"Niceness: {niceness_val:.2f}"
            )

            x, y, z = self.latlon_to_cartesian(lat, lon)
            
            # Adjust marker size based on niceness score
            marker_size = 4 + (niceness_val * 2)  # Size ranges from 4 to 6
            
            city_scatter.append(
                go.Scatter3d(
                    x=[x], y=[y], z=[z],
                    mode='markers+text',
                    text=[f"{rank}. {city_name}"],  # Show rank in label
                    textposition='top center',
                    textfont=dict(color='black', size=10),
                    marker=dict(
                        size=marker_size,
                        color=color_str,
                        line=dict(color='black', width=0.5)
                    ),
                    hoverinfo='text',
                    hovertext=[hover_text],
                    name=city_name
                )
            )
            plotted += 1
        print(f"[GLOBE] Plotted {plotted} cities with resolved coordinates")

        # Add coastlines with improved error handling and visualization
        coastline_traces = []
        try:
            coastlines = gpd.read_file("ne_110m_coastline.shp")
            for idx, row in coastlines.iterrows():
                if row.geometry.geom_type == "LineString":
                    coords = list(row.geometry.coords)
                    xs, ys, zs = [], [], []
                    for lon, lat in coords:
                        x, y, z = self.latlon_to_cartesian(lat, lon)
                        xs.append(x)
                        ys.append(y)
                        zs.append(z)
                    coastline_traces.append(
                        go.Scatter3d(
                            x=xs, y=ys, z=zs,
                            mode="lines",
                            line=dict(color="rgba(0,0,0,0.8)", width=1),
                            showlegend=False,
                            hoverinfo='skip'
                        )
                    )
                elif row.geometry.geom_type == "MultiLineString":
                    for line in row.geometry:
                        coords = list(line.coords)
                        xs, ys, zs = [], [], []
                        for lon, lat in coords:
                            x, y, z = self.latlon_to_cartesian(lat, lon)
                            xs.append(x)
                            ys.append(y)
                            zs.append(z)
                        coastline_traces.append(
                            go.Scatter3d(
                                x=xs, y=ys, z=zs,
                                mode="lines",
                                line=dict(color="rgba(0,0,0,0.8)", width=1),
                                showlegend=False,
                                hoverinfo='skip'
                            )
                        )
        except Exception as e:
            print(f"Could not load coastlines: {e}")
            QMessageBox.warning(
                self,
                "Warning",
                f"Could not load coastline data: {str(e)}\n"
                "The globe will be displayed without coastlines."
            )

        fig = go.Figure(data=[earth_surface] + coastline_traces + city_scatter)

        fig.update_layout(
            title=dict(
                text='Top 100 Cities by Weather Niceness (Blue→Yellow = Bad→Good)',
                font=dict(color='black', size=20),
                y=0.95
            ),
            paper_bgcolor='white',
            plot_bgcolor='white',
            scene=dict(
                xaxis=dict(showbackground=False, visible=False),
                yaxis=dict(showbackground=False, visible=False),
                zaxis=dict(showbackground=False, visible=False),
                aspectmode='data',
                camera=dict(
                    up=dict(x=0, y=0, z=1),
                    center=dict(x=0, y=0, z=0),
                    eye=dict(x=1.5, y=1.5, z=1.5)
                ),
                bgcolor='white'
            ),
            margin=dict(l=0, r=0, t=30, b=0),
            showlegend=False,
            hoverlabel=dict(
                bgcolor='rgba(255,255,255,0.9)',
                font=dict(color='black', size=14),
                bordercolor='black'
            )
        )

        return fig

    def create_plotly_globe(self):
        """Display the rotating 3D globe in a browser with improved error handling."""
        try:
            fig = self.create_rotating_globe_figure()
            html_file = "weather_globe.html"
            
            # Write the HTML file with a simpler template
            plot_html = fig.to_html(
                full_html=True,
                include_plotlyjs=True,
                config={'displayModeBar': True}
            )
            
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(plot_html)
            
            # Open in browser
            import webbrowser
            webbrowser.open(f"file://{os.path.abspath(html_file)}")
            
        except Exception as e:
            QMessageBox.warning(
                self,
                "Error",
                f"Could not create globe visualization: {str(e)}\n\n"
                "Please make sure you have all required packages installed:\n"
                "- plotly\n"
                "- geopandas (optional, for coastlines)"
            )

def main():
    app=QApplication(sys.argv)
    app.setStyle("Fusion")
    palette=QPalette()
    palette.setColor(QPalette.ColorRole.Window,QColor(255,255,255))
    palette.setColor(QPalette.ColorRole.WindowText,QColor(0,0,0))
    palette.setColor(QPalette.ColorRole.Base,QColor(255,255,255))
    palette.setColor(QPalette.ColorRole.AlternateBase,QColor(245,245,245))
    palette.setColor(QPalette.ColorRole.Text,QColor(0,0,0))
    palette.setColor(QPalette.ColorRole.PlaceholderText,QColor(128,128,128))
    palette.setColor(QPalette.ColorRole.Button,QColor(240,240,240))
    palette.setColor(QPalette.ColorRole.ButtonText,QColor(0,0,0))
    palette.setColor(QPalette.ColorRole.Highlight,QColor(200,200,200))
    palette.setColor(QPalette.ColorRole.HighlightedText,QColor(0,0,0))
    palette.setColor(QPalette.ColorRole.Light,QColor(255,255,255))
    palette.setColor(QPalette.ColorRole.Midlight,QColor(240,240,240))
    palette.setColor(QPalette.ColorRole.Mid,QColor(200,200,200))
    palette.setColor(QPalette.ColorRole.Dark,QColor(160,160,160))
    palette.setColor(QPalette.ColorRole.Shadow,QColor(105,105,105))
    palette.setColor(QPalette.ColorRole.Link,QColor(0,0,255))
    app.setPalette(palette)
    app.setStyleSheet("""
        QWidget {
            background-color:white;
            color:black;
        }
        * {
            color:black !important;
            background-color:white !important;
        }
        QTableWidget {
            alternate-background-color:#f5f5f5;
        }
        QHeaderView::section {
            background-color:#e0e0e0;
            color:black !important;
        }
        QPushButton {
            background-color:#f0f0f0;
            border:1px solid #c0c0c0;
        }
        QLineEdit {
            border:1px solid #c0c0c0;
        }
        QLabel {
            color:black !important;
        }
    """)
    init_db()
    forecast_cache=load_forecast_cache()
    all_coords = CITY_COORDS.copy()
    city_list=list(all_coords.items())
    loading=LoadingDialog(len(city_list))
    loading.show()
    conn=sqlite3.connect(DATABASE)
    
    # Reduce number of workers to avoid rate limiting
    max_workers = 3  # Reduced from 8
    
    def fetch_city_data(city,latlon):
        local_conn=sqlite3.connect(DATABASE)
        try:
            if not have_data_for_city(local_conn,city):
                lat,lon=latlon
                hist_json=fetch_historical(lat,lon,START_DATE,END_DATE)
                if "daily" in hist_json:
                    df=process_daily_data(hist_json["daily"])
                    store_data(local_conn,city,df)
            df=load_data_from_db(local_conn,city)
            return city,df
        finally:
            local_conn.close()
    all_city_data={}
    done_count=0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures={executor.submit(fetch_city_data,c,l):c for c,l in city_list}
        for fut in as_completed(futures):
            city_name,df=fut.result()
            all_city_data[city_name]=df
            done_count+=1
            loading.update_fetch(done_count)
    forecast_cache=load_forecast_cache()
    done_count=0
    monthly_dict={}
    with ThreadPoolExecutor(max_workers=8) as executor:
        def monthly_task(c):
            mdf=monthly_aggregates(all_city_data[c])
            mdf["niceness"]=mdf.apply(lambda r:compute_city_niceness(r["tmax_mean"],r["tmin_mean"],r["sunny_day"],r["day_length_hrs"]),axis=1)
            return c,mdf
        futures={executor.submit(monthly_task,c):c for c in all_city_data}
        for fut in as_completed(futures):
            city_name,mdf=fut.result()
            monthly_dict[city_name]=mdf
            done_count+=1
            loading.update_process(done_count)
    def fetch_current_data(city, latlon):
        print(f"\n[FETCH] Processing current data for {city}")
        lat, lon = latlon
        local_df = all_city_data[city]
        now = datetime.now(timezone.utc)
        
        # Check if we have fresh cached data including niceness calculation
        if city in forecast_cache and is_forecast_fresh(city, forecast_cache):
            print(f"[CACHE] Using cached data for {city}")
            cached_data = forecast_cache[city]
            if all(key in cached_data for key in ['niceness', 'current_temp_f', 'next_month_sunny_days', 
                                                'est_next_month_day_length', 'tmax_f', 'tmin_f', 
                                                'forecast_sunny_count', 'forecast_days']):
                print(f"[CACHE] Using cached niceness score and calculations for {city}")
                return {
                    "city": city,
                    "current_temp_f": cached_data['current_temp_f'],
                    "next_month_sunny_days": cached_data['next_month_sunny_days'],
                    "est_next_month_day_length": cached_data['est_next_month_day_length'],
                    "niceness": cached_data['niceness'],
                    "tmax_f": cached_data['tmax_f'],
                    "tmin_f": cached_data['tmin_f'],
                    "forecast_sunny_count": cached_data['forecast_sunny_count'],
                    "forecast_days": cached_data['forecast_days']
                }
        
        print(f"[FETCH] Fetching fresh data for {city}")
        try:
            fore_json = fetch_current_forecast_data(lat, lon)
            cur_json = fetch_current(lat, lon)
            
            current_temp_f = float('nan')
            est_next_month_day_length = 12.0
            tmax_f = float('nan')
            tmin_f = float('nan')
            today = datetime.now(timezone.utc)
            next_month = (today.month % 12) + 1
            mdf = monthly_dict[city]
            target_month_row = mdf[mdf["month"] == next_month]
            historical_sunny_avg = 15.0 if target_month_row.empty else target_month_row["sunny_day"].iloc[0]
            
            if "current_weather" in cur_json:
                current_temp_c = cur_json["current_weather"]["temperature"]
                current_temp_f = c_to_f(current_temp_c)
            
            forecast_sunny_count = 0
            forecast_days = 0
            next_month_sunny_days = historical_sunny_avg
            
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
                
                # Calculate next month's sunny days more accurately
                if forecast_days < 30:
                    # For remaining days, use historical average proportion
                    remaining_days = 30 - forecast_days
                    historical_sunny_proportion = historical_sunny_avg / 30.0
                    estimated_remaining_sunny = remaining_days * historical_sunny_proportion
                    next_month_sunny_days = min(30, forecast_sunny_count + estimated_remaining_sunny)
                else:
                    # If we have more than 30 days of forecast, just use the first 30 days
                    next_month_sunny_days = min(30, forecast_sunny_count)
            
            row_m = mdf[mdf["month"] == next_month]
            if not row_m.empty:
                est_next_month_day_length = row_m["day_length_hrs"].iloc[0]
            
            ref_temp = (tmax_f + tmin_f) / 2 if not pd.isna(tmax_f) and not pd.isna(tmin_f) else current_temp_f
            niceness = compute_niceness(ref_temp, next_month_sunny_days, est_next_month_day_length)
            
            # Store all calculated data in cache
            forecast_cache[city] = {
                'fore_json': fore_json,
                'cur_json': cur_json,
                'time': now,
                'niceness': niceness,
                'current_temp_f': current_temp_f,
                'next_month_sunny_days': next_month_sunny_days,
                'est_next_month_day_length': est_next_month_day_length,
                'tmax_f': tmax_f,
                'tmin_f': tmin_f,
                'forecast_sunny_count': forecast_sunny_count,
                'forecast_days': forecast_days
            }
            
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
        except Exception as e:
            print(f"[ERROR] Failed to fetch fresh data for {city}: {e}")
            raise

    current_data_list=[]
    done_count=0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures={executor.submit(fetch_current_data,c,all_coords[c]):c for c in all_coords}
        for fut in as_completed(futures):
            row=fut.result()
            current_data_list.append(row)
            done_count+=1
            loading.update_current(done_count)
    save_forecast_cache(forecast_cache)
    loading.close()
    window=WeatherApp(current_data_list,monthly_dict,all_city_data,forecast_cache)
    now=datetime.now(timezone.utc)
    forecast_until="N/A"
    for c in forecast_cache:
        fore_json=forecast_cache[c]['fore_json']
        if "daily" in fore_json and "time" in fore_json["daily"]:
            times=pd.to_datetime(fore_json["daily"]["time"])
            forecast_until=times.max().strftime("%Y-%m-%d")
            break
    window.set_itinerary_label(f"Updated as of: {now.strftime('%Y-%m-%d %H:%M UTC')}   Forecast until: {forecast_until}")
    window.show()
    sys.exit(app.exec())

if __name__=="__main__":
    main()
