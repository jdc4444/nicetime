#!/usr/bin/env python3
"""
Open-Meteo backfill — replaces Visual Crossing with local Open-Meteo API.
Uses the meteo dashboard's niceness scoring engine (6-parameter band scoring).

Reads the city catalog, fetches 16-day forecast from a local Open-Meteo instance,
and generates the same JS data files the nicetime frontend expects:
  - forecast_current_data.js   (FORECAST_CURRENT, FORECAST_CURRENT_COORD, FORECAST_CURRENT_META)
  - forecast_14day_data.js     (FORECAST_14DAY, FORECAST_14DAY_COORD, FORECAST_14DAY_META)
  - estimated_monthly_data.js  (estimatedMonthlyData — derived from catalog monthly data)

Usage:
  python3 run_openmeteo_backfill.py                          # default: localhost:8090
  python3 run_openmeteo_backfill.py --api-url http://host:8080
  python3 run_openmeteo_backfill.py --max-cities 10          # test run
"""
import argparse
import json
import math
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

BASE_DIR = str(Path(__file__).resolve().parent)
DEFAULT_CATALOG = f"{BASE_DIR}/all_city_data.json"
DEFAULT_OUTPUT_DIR = str((Path(BASE_DIR).parent / "docs").resolve())

# Open-Meteo forecast variables — includes all 6 meteo scoring params
FORECAST_PARAMS = {
    "daily": ",".join([
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "precipitation_probability_max",
        "weathercode",
        "sunrise",
        "sunset",
        "wind_speed_10m_max",
        "sunshine_duration",
        "cloud_cover_mean",
        "relative_humidity_2m_mean",
    ]),
    "models": "ecmwf_ifs025",
    "timezone": "auto",
    "forecast_days": 16,
    "past_days": 0,
}


# ══════════════════════════════════════════════════════════════════════
# Meteo Dashboard Niceness Scoring Engine (ported from niceness.ts)
# ══════════════════════════════════════════════════════════════════════
# 6 parameters with ideal/acceptable bands, importance weights, and boosts.
# scoreValue maps any value to 0-100 based on where it falls relative to bands.
# Overall niceness = weighted average of per-param scores.

NICE_PARAMS = [
    {
        "key": "temperature",
        "band": {"idealLo": 22.2, "idealHi": 27.2, "acceptLo": 14.4, "acceptHi": 28.9},
        "weight": 59, "boost": 3,
    },
    {
        "key": "precipitation",
        "band": {"idealLo": 0, "idealHi": 0, "acceptLo": 0, "acceptHi": 3},
        "weight": 60, "boost": 2,
    },
    {
        "key": "wind",
        "band": {"idealLo": 0, "idealHi": 15, "acceptLo": 0, "acceptHi": 35},
        "weight": 10, "boost": 1,
    },
    {
        "key": "cloud",
        "band": {"idealLo": 5, "idealHi": 10, "acceptLo": 0, "acceptHi": 43},
        "weight": 60, "boost": 1,
    },
    {
        "key": "humidity",
        "band": {"idealLo": 30, "idealHi": 83, "acceptLo": 10, "acceptHi": 80},
        "weight": 27, "boost": 1,
    },
    {
        "key": "sunshine",
        "band": {"idealLo": 9, "idealHi": 16, "acceptLo": 9, "acceptHi": 20},
        "weight": 40, "boost": 3,
    },
]

# Pre-compute total absolute weight
_TOTAL_ABS_WEIGHT = sum(abs(p["weight"] * p["boost"]) for p in NICE_PARAMS)


def score_value(value: float, band: dict) -> float:
    """Score a single value against ideal/acceptable bands. Returns 0-100.
    Exact port of niceness.ts scoreValue()."""
    ideal_lo = band["idealLo"]
    ideal_hi = band["idealHi"]
    accept_lo = band["acceptLo"]
    accept_hi = band["acceptHi"]

    # Inside ideal zone → 100
    if ideal_lo <= value <= ideal_hi:
        return 100.0

    # Between acceptable and ideal (low side) → 50-100
    if value < ideal_lo and value >= accept_lo:
        rng = ideal_lo - accept_lo
        return 50.0 + 50.0 * (value - accept_lo) / rng if rng > 0 else 50.0

    # Between ideal and acceptable (high side) → 50-100
    if value > ideal_hi and value <= accept_hi:
        rng = accept_hi - ideal_hi
        return 50.0 + 50.0 * (accept_hi - value) / rng if rng > 0 else 50.0

    # Below acceptable → 0-50 (quadratic falloff)
    if value < accept_lo:
        dist = accept_lo - value
        rng = (ideal_lo - accept_lo) or 1
        return max(0.0, 50.0 * (1 - (dist / rng) ** 2))

    # Above acceptable → 0-50 (quadratic falloff)
    if value > accept_hi:
        dist = value - accept_hi
        rng = (accept_hi - ideal_hi) or 1
        return max(0.0, 50.0 * (1 - (dist / rng) ** 2))

    return 0.0


def blended_temperature_score(
    temp_mean_c: float | None,
    tmax_c: float | None,
    tmin_c: float | None,
    band: dict,
) -> float | None:
    """Blend temperature scores for mean/high/low using the existing band scorer.

    The intended mix is 50% mean, 25% high, 25% low. If one or more values are
    missing, the available weights are normalized so missing data does not
    artificially depress the score.
    """
    weighted_scores: list[tuple[float, float]] = []
    if temp_mean_c is not None:
        weighted_scores.append((0.5, score_value(temp_mean_c, band)))
    if tmax_c is not None:
        weighted_scores.append((0.25, score_value(tmax_c, band)))
    if tmin_c is not None:
        weighted_scores.append((0.25, score_value(tmin_c, band)))

    if not weighted_scores:
        return None

    total_weight = sum(weight for weight, _ in weighted_scores)
    if total_weight <= 0:
        return None

    return sum(weight * score for weight, score in weighted_scores) / total_weight


def compute_day_niceness(
    temp_mean_c: float | None,
    precip_mm: float | None,
    wind_kmh: float | None,
    cloud_pct: float | None,
    humidity_pct: float | None,
    sunshine_hrs: float | None,
    tmax_c: float | None = None,
    tmin_c: float | None = None,
) -> float | None:
    """Compute niceness score for a single day using meteo dashboard engine.
    Temperature is scored as a blend of mean/high/low using the same band
    function as the other parameters.
    Returns 0-100 or None if insufficient data."""
    values = {
        "temperature": temp_mean_c,
        "precipitation": precip_mm,
        "wind": wind_kmh,
        "cloud": cloud_pct,
        "humidity": humidity_pct,
        "sunshine": sunshine_hrs,
    }

    if _TOTAL_ABS_WEIGHT == 0:
        return None

    total = 0.0
    has_any = False
    for p in NICE_PARAMS:
        v = values.get(p["key"])
        if p["key"] == "temperature":
            s = blended_temperature_score(temp_mean_c, tmax_c, tmin_c, p["band"])
        else:
            if v is None:
                continue
            s = score_value(v, p["band"])
        if s is None:
            continue
        ew = p["weight"] * p["boost"]
        total += s * (ew / _TOTAL_ABS_WEIGHT)
        has_any = True

    if not has_any:
        return None

    return round(max(0.0, min(100.0, total)), 1)


# ══════════════════════════════════════════════════════════════════════


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_city_key(s: Any) -> str:
    raw = unicodedata.normalize("NFD", str(s or ""))
    raw = "".join(ch for ch in raw if unicodedata.category(ch) != "Mn")
    raw = raw.lower()
    return re.sub(r"[^a-z0-9]+", "", raw)


def to_num(v: Any) -> float | None:
    try:
        n = float(v)
        if n != n:
            return None
        return n
    except Exception:
        return None


def c_to_f(c: float | None) -> float | None:
    if c is None:
        return None
    return round((c * 9.0 / 5.0) + 32.0, 1)


# WMO weather code -> Visual Crossing-style icon mapping
WMO_TO_ICON: dict[int, str] = {
    0: "clear-day", 1: "clear-day", 2: "partly-cloudy-day", 3: "cloudy",
    45: "fog", 48: "fog",
    51: "rain", 53: "rain", 55: "rain", 56: "rain", 57: "rain",
    61: "rain", 63: "rain", 65: "rain",
    66: "sleet", 67: "sleet",
    71: "snow", 73: "snow", 75: "snow", 77: "snow",
    80: "showers-day", 81: "showers-day", 82: "showers-day",
    85: "snow", 86: "snow",
    95: "thunder-rain", 96: "thunder-rain", 99: "thunder-rain",
}


def wmo_to_icon(code: int | None) -> str:
    if code is None:
        return "cloudy"
    return WMO_TO_ICON.get(code, "cloudy")


def compute_sunny_bad_days(icons: list[str], precip_probs: list[float | None], precip_sums: list[float | None]) -> tuple[float | None, float | None]:
    if not icons:
        return None, None
    good_icons = {"clear-day", "clear-night", "partly-cloudy-day", "partly-cloudy-night"}
    bad_icons = {"rain", "snow", "fog", "thunder-rain", "showers-day", "showers-night", "sleet", "thunderstorm"}
    good = bad = neutral = 0
    for i, icon in enumerate(icons):
        pp = precip_probs[i] if i < len(precip_probs) else None
        ps = precip_sums[i] if i < len(precip_sums) else None
        if icon in good_icons:
            good += 1
        elif icon in bad_icons:
            bad += 1
        elif (pp is not None and pp >= 60.0) or (ps is not None and ps >= 1.0):
            bad += 1
        else:
            neutral += 1
    n = max(1, len(icons))
    scale = 30.0 / n
    return round((good + 0.5 * neutral) * scale, 1), round(bad * scale, 1)


def sunrise_sunset_to_hours(sunrise_str: str | None, sunset_str: str | None) -> float | None:
    if not sunrise_str or not sunset_str:
        return None
    try:
        sr = datetime.fromisoformat(sunrise_str)
        ss = datetime.fromisoformat(sunset_str)
        hrs = (ss - sr).total_seconds() / 3600.0
        if hrs < 0:
            hrs += 24.0
        if 0 <= hrs <= 24:
            return round(hrs, 2)
    except Exception:
        pass
    return None


def load_catalog(path: str) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    out = []
    seen = set()
    for row in data:
        city = str(row.get("city", "")).strip()
        country = str(row.get("country", "")).strip()
        if not city or not country:
            continue
        key = (city, country)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "city": city,
            "country": country,
            "lat": float(row.get("lat", 0.0)),
            "lng": float(row.get("lng", 0.0)),
            "continent": str(row.get("continent", "Unknown")).strip() or "Unknown",
            "monthly": row.get("monthly", {}),
        })
    return out


def fetch_forecast(api_url: str, lat: float, lon: float, session: requests.Session) -> dict[str, Any]:
    params = {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        **FORECAST_PARAMS,
    }
    resp = session.get(f"{api_url}/v1/forecast", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def process_city_forecast(city_info: dict[str, Any], api_data: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Transform Open-Meteo response into JS data structures with meteo niceness scores."""
    daily = api_data.get("daily", {})
    dates = daily.get("time", [])
    tmax_list = daily.get("temperature_2m_max", [])
    tmin_list = daily.get("temperature_2m_min", [])
    precip_list = daily.get("precipitation_sum", [])
    precip_prob_list = daily.get("precipitation_probability_max", [])
    wmo_codes = daily.get("weathercode", [])
    sunrise_list = daily.get("sunrise", [])
    sunset_list = daily.get("sunset", [])
    wind_list = daily.get("wind_speed_10m_max", [])
    sunshine_list = daily.get("sunshine_duration", [])  # in seconds
    cloud_list = daily.get("cloud_cover_mean", [])
    humidity_list = daily.get("relative_humidity_2m_mean", [])

    n = len(dates)
    icons = [wmo_to_icon(wmo_codes[i] if i < len(wmo_codes) else None) for i in range(n)]

    # Compute per-day niceness scores using meteo engine
    niceness_scores: list[float | None] = []
    for i in range(n):
        tmax = to_num(tmax_list[i]) if i < len(tmax_list) else None
        tmin = to_num(tmin_list[i]) if i < len(tmin_list) else None
        temp_mean = (tmax + tmin) / 2 if tmax is not None and tmin is not None else None
        precip = to_num(precip_list[i]) if i < len(precip_list) else None
        wind = to_num(wind_list[i]) if i < len(wind_list) else None
        cloud = to_num(cloud_list[i]) if i < len(cloud_list) else None
        humidity = to_num(humidity_list[i]) if i < len(humidity_list) else None
        sunshine_sec = to_num(sunshine_list[i]) if i < len(sunshine_list) else None
        sunshine_hrs = sunshine_sec / 3600.0 if sunshine_sec is not None else None

        score = compute_day_niceness(temp_mean, precip, wind, cloud, humidity, sunshine_hrs, tmax_c=tmax, tmin_c=tmin)
        niceness_scores.append(score)

    # Compute summary stats
    highs_c = [to_num(tmax_list[i]) for i in range(min(n, len(tmax_list)))]
    lows_c = [to_num(tmin_list[i]) for i in range(min(n, len(tmin_list)))]
    highs_valid = [x for x in highs_c if x is not None]
    lows_valid = [x for x in lows_c if x is not None]

    day_temp_f = c_to_f(sum(highs_valid) / len(highs_valid)) if highs_valid else None
    night_temp_f = c_to_f(sum(lows_valid) / len(lows_valid)) if lows_valid else None

    day_lengths = []
    for i in range(n):
        sr = sunrise_list[i] if i < len(sunrise_list) else None
        ss = sunset_list[i] if i < len(sunset_list) else None
        dl = sunrise_sunset_to_hours(sr, ss)
        if dl is not None:
            day_lengths.append(dl)
    day_length_hrs = round(sum(day_lengths) / len(day_lengths), 2) if day_lengths else None

    precip_probs = [to_num(precip_prob_list[i]) if i < len(precip_prob_list) else None for i in range(n)]
    precip_sums = [to_num(precip_list[i]) if i < len(precip_list) else None for i in range(n)]
    sunny_days_30, bad_days_30 = compute_sunny_bad_days(icons, precip_probs, precip_sums)

    # Aggregate niceness: avg score, really nice days (>=70), perfect days (>=90)
    valid_scores = [s for s in niceness_scores if s is not None]
    avg_nice_score = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else None
    if valid_scores:
        scale = 30.0 / max(1, len(valid_scores))
        really_nice_days_30 = round(sum(1 for s in valid_scores if s >= 70) * scale, 1)
        perfect_days_30 = round(sum(1 for s in valid_scores if s >= 90) * scale, 1)
    else:
        really_nice_days_30 = None
        perfect_days_30 = None

    # Average the 6 scoring variables for client-side re-scoring
    def avg_valid(lst: list, length: int) -> float | None:
        vals = [to_num(lst[i]) for i in range(min(length, len(lst)))]
        valid = [v for v in vals if v is not None]
        return round(sum(valid) / len(valid), 1) if valid else None

    # Mean temp for scoring, plus high/low for extremes penalty
    avg_temp_mean_c = None
    if highs_valid and lows_valid:
        pairs = [(h, l) for h, l in zip(highs_valid, lows_valid)]
        avg_temp_mean_c = round(sum((h + l) / 2 for h, l in pairs) / len(pairs), 1)
    avg_temp_high_c = round(sum(highs_valid) / len(highs_valid), 1) if highs_valid else None
    avg_temp_low_c = round(sum(lows_valid) / len(lows_valid), 1) if lows_valid else None

    avg_precip_mm = avg_valid(precip_list, n)
    avg_wind_kmh = avg_valid(wind_list, n)
    avg_cloud_pct = avg_valid(cloud_list, n)
    avg_humidity_pct = avg_valid(humidity_list, n)
    sunshine_hrs_list = [to_num(sunshine_list[i]) for i in range(min(n, len(sunshine_list)))]
    sunshine_valid = [v / 3600.0 for v in sunshine_hrs_list if v is not None]
    avg_sunshine_hrs = round(sum(sunshine_valid) / len(sunshine_valid), 1) if sunshine_valid else None

    now_iso = utcnow_iso()
    summary = {
        "cache_key": f"{city_info['city']}, {city_info['country']}",
        "city": city_info["city"],
        "country": city_info["country"],
        "day_temp_f": day_temp_f,
        "night_temp_f": night_temp_f,
        "sunny_days_30": sunny_days_30,
        "bad_days_30": bad_days_30,
        "really_nice_days_30": really_nice_days_30,
        "perfect_days_30": perfect_days_30,
        "avg_nice_score": avg_nice_score,
        "day_length_hrs": day_length_hrs,
        # 6 meteo scoring params (averages over forecast window, for client-side re-scoring)
        "temp_mean_c": avg_temp_mean_c,
        "temp_high_c": avg_temp_high_c,
        "temp_low_c": avg_temp_low_c,
        "precip_mm": avg_precip_mm,
        "wind_kmh": avg_wind_kmh,
        "cloud_pct": avg_cloud_pct,
        "humidity_pct": avg_humidity_pct,
        "sunshine_hrs": avg_sunshine_hrs,
        "forecast_days": n,
        "fetched_at": now_iso,
    }

    # Build daily rows
    daily_rows = []
    for i in range(n):
        hi_f = c_to_f(to_num(tmax_list[i]) if i < len(tmax_list) else None)
        lo_f = c_to_f(to_num(tmin_list[i]) if i < len(tmin_list) else None)
        daily_rows.append({
            "date": dates[i] if i < len(dates) else "",
            "high_f": hi_f,
            "low_f": lo_f,
            "precip_pct": to_num(precip_prob_list[i]) if i < len(precip_prob_list) else None,
            "icon": icons[i],
            "niceness": niceness_scores[i],
            "updated_at": now_iso,
        })

    return summary, daily_rows


def estimate_day_length_hours(lat: float, month: int) -> float:
    day_of_year = (month - 1) * 30.4 + 15
    decl = 23.45 * math.sin(math.radians((360 / 365) * (day_of_year - 81)))
    lat_rad = math.radians(lat)
    decl_rad = math.radians(decl)
    cos_ha = -math.tan(lat_rad) * math.tan(decl_rad)
    cos_ha = max(-1, min(1, cos_ha))
    if cos_ha <= -1:
        return 24.0
    if cos_ha >= 1:
        return 0.0
    ha = math.degrees(math.acos(cos_ha))
    return round(ha * 2 / 15, 2)


def generate_estimated_monthly(catalog: list[dict[str, Any]]) -> str:
    """Generate estimatedMonthlyData JS from catalog monthly data, scored with meteo engine."""
    result: dict[str, dict[str, Any]] = {}
    now_iso = utcnow_iso()

    for city_info in catalog:
        city = city_info["city"]
        country = city_info["country"]
        lat = city_info["lat"]
        monthly = city_info.get("monthly", {})
        key = normalize_city_key(f"{city}{country}")
        if not key or not monthly:
            continue

        months_data: dict[str, Any] = {}
        for m_str, m_data in monthly.items():
            m_int = int(m_str)
            avg_temp_f = to_num(m_data.get("avg_temp_f"))
            avg_tmax_f = to_num(m_data.get("avg_tmax_f"))
            avg_tmin_f = to_num(m_data.get("avg_tmin_f"))
            rain_pct = to_num(m_data.get("rain_pct")) or 0
            sunny_days = to_num(m_data.get("sunny_days")) or 15
            bad_days = to_num(m_data.get("bad_days")) or 0
            day_len = to_num(m_data.get("day_length_hrs")) or estimate_day_length_hours(lat, m_int)

            # Compute niceness using meteo engine — mean temp + extremes penalty
            if avg_tmax_f is not None and avg_tmin_f is not None:
                temp_mean_c = ((avg_tmax_f + avg_tmin_f) / 2 - 32) * 5 / 9
                tmax_c = (avg_tmax_f - 32) * 5 / 9
                tmin_c = (avg_tmin_f - 32) * 5 / 9
            elif avg_temp_f is not None:
                temp_mean_c = (avg_temp_f - 32) * 5 / 9
                tmax_c = None
                tmin_c = None
            else:
                temp_mean_c = None
                tmax_c = None
                tmin_c = None

            # Estimate precip mm from rain_pct (rough: 30% rain ~ 2mm avg)
            precip_mm = rain_pct * 0.067 if rain_pct is not None else None
            # Estimate cloud cover from sunny/bad days (30 sunny = 10%, 0 sunny = 80%)
            cloud_pct = max(0, min(100, 80 - (sunny_days / 30) * 70)) if sunny_days is not None else None
            # No wind/humidity data in catalog — use neutral defaults
            wind_kmh = 12.0  # neutral value within ideal band
            humidity_pct = 55.0  # neutral value within ideal band
            sunshine_hrs = day_len * (sunny_days / 30) if day_len and sunny_days else day_len

            nice = compute_day_niceness(temp_mean_c, precip_mm, wind_kmh, cloud_pct, humidity_pct, sunshine_hrs, tmax_c=tmax_c, tmin_c=tmin_c)
            if nice is None:
                nice = to_num(m_data.get("niceness")) or 50.0

            # Estimate really nice / perfect days from niceness score
            if avg_tmax_f is not None and avg_tmin_f is not None:
                dry_frac = max(0, (30 - bad_days)) / 30
                # Really nice: score >= 70 equivalent
                nice_frac = max(0, min(1, (nice - 40) / 60)) if nice else 0
                really_nice = round(nice_frac * dry_frac * 30, 1)
                # Perfect: score >= 90 equivalent
                perfect_frac = max(0, min(1, (nice - 70) / 30)) if nice else 0
                perfect = round(perfect_frac * dry_frac * 30 * 0.5, 1)
            else:
                really_nice = 0.0
                perfect = 0.0

            months_data[m_str] = {
                "sunny_days_30": round(sunny_days, 1),
                "bad_days_30": round(bad_days, 1),
                "really_nice_days_30": really_nice,
                "perfect_days_30": perfect,
                "avg_nice_score": nice,
                "day_length_hrs": round(day_len, 2),
                "fetched_at": now_iso,
                "rows": 30,
            }

        if months_data:
            result[key] = months_data

    return f"window.estimatedMonthlyData = {json.dumps(result, ensure_ascii=False)};\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Open-Meteo forecast backfill for nicetime frontend.")
    ap.add_argument("--api-url", default="http://localhost:8090", help="Open-Meteo API base URL")
    ap.add_argument("--catalog", default=DEFAULT_CATALOG, help="City catalog JSON path")
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for JS files")
    ap.add_argument("--max-cities", type=int, default=0, help="Limit cities (0 = all)")
    ap.add_argument("--continent", default="", help="Filter by continent")
    ap.add_argument("--country", default="", help="Filter by country")
    ap.add_argument("--batch-size", type=int, default=50, help="Cities per progress update")
    ap.add_argument("--delay", type=float, default=0.05, help="Delay between API calls (seconds)")
    ap.add_argument("--skip-monthly", action="store_true", help="Skip estimated monthly generation")
    ap.add_argument("--skip-forecast", action="store_true", help="Skip forecast fetch")
    args = ap.parse_args()

    print(f"Loading catalog: {args.catalog}")
    catalog = load_catalog(args.catalog)
    print(f"Loaded {len(catalog)} cities")

    if args.continent:
        cont = {c.strip().lower() for c in args.continent.split(",")}
        catalog = [c for c in catalog if c["continent"].lower() in cont]
        print(f"After continent filter: {len(catalog)} cities")
    if args.country:
        ctry = {c.strip().lower() for c in args.country.split(",")}
        catalog = [c for c in catalog if c["country"].lower() in ctry]
        print(f"After country filter: {len(catalog)} cities")
    if args.max_cities > 0:
        catalog = catalog[:args.max_cities]
        print(f"Limited to {len(catalog)} cities")

    if not catalog:
        print("No cities selected.", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Generate estimated monthly data (from catalog, no API needed) ──
    if not args.skip_monthly:
        print("\n-- Generating estimated monthly data (meteo scoring) --")
        monthly_js = generate_estimated_monthly(catalog)
        monthly_path = output_dir / "estimated_monthly_data.js"
        monthly_path.write_text(monthly_js, encoding="utf-8")
        print(f"Wrote {monthly_path} ({len(monthly_js):,} bytes)")

    # ── Fetch forecasts from Open-Meteo ──
    if not args.skip_forecast:
        print(f"\n-- Fetching forecasts from {args.api_url} (meteo scoring) --")
        today = date.today()
        fc_start = today.isoformat()
        fc_end = (today + timedelta(days=15)).isoformat()

        session = requests.Session()

        try:
            test_resp = session.get(f"{args.api_url}/v1/forecast", params={
                "latitude": 0, "longitude": 0,
                "daily": "temperature_2m_max",
                "timezone": "auto",
                "forecast_days": 1,
            }, timeout=10)
            test_resp.raise_for_status()
            print(f"API connection OK: {args.api_url}")
        except Exception as e:
            print(f"ERROR: Cannot reach Open-Meteo API at {args.api_url}: {e}", file=sys.stderr)
            return 2

        src: dict[str, dict[str, Any]] = {}
        by_coord: dict[str, dict[str, Any]] = {}
        daily_src: dict[str, list[dict[str, Any]]] = {}
        daily_by_coord: dict[str, list[dict[str, Any]]] = {}

        done = 0
        ok = 0
        err = 0
        errors: list[str] = []
        started = time.time()

        for city_info in catalog:
            done += 1
            city = city_info["city"]
            country = city_info["country"]
            lat = city_info["lat"]
            lng = city_info["lng"]
            label = f"{city}, {country}"

            try:
                api_data = fetch_forecast(args.api_url, lat, lng, session)
                summary, daily_rows = process_city_forecast(city_info, api_data)

                key_cc = normalize_city_key(f"{city}{country}")
                key_city = normalize_city_key(f"{city}, {country}")
                for k in (key_cc, key_city):
                    if k:
                        src[k] = summary
                        daily_src[k] = daily_rows

                if lat is not None and lng is not None:
                    for r in (4, 3, 2):
                        ck = f"{float(round(lat, r))}|{float(round(lng, r))}"
                        by_coord[ck] = summary
                        daily_by_coord[ck] = daily_rows

                ok += 1
            except Exception as e:
                err += 1
                errors.append(f"{label}: {e}")
                if err <= 5:
                    print(f"  ERROR {label}: {e}")

            if done % args.batch_size == 0 or done == len(catalog):
                elapsed = time.time() - started
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(catalog) - done) / rate if rate > 0 else 0
                print(f"  [{done}/{len(catalog)}] ok={ok} err={err} "
                      f"rate={rate:.1f}/s ETA={int(eta)}s")

            if args.delay > 0:
                time.sleep(args.delay)

        meta = {
            "generated_at": utcnow_iso(),
            "run_date_local": today.isoformat(),
            "window_start": fc_start,
            "window_end": fc_end,
            "expected_cities": len(catalog),
            "cities_with_rows": ok,
            "cities_fresh_today": ok,
            "source": "open-meteo",
            "scoring": "meteo-niceness-v1",
            "api_url": args.api_url,
        }

        fc_text = (
            f"window.FORECAST_CURRENT = {json.dumps(src, ensure_ascii=False)};\n"
            f"window.FORECAST_CURRENT_COORD = {json.dumps(by_coord, ensure_ascii=False)};\n"
            f"window.FORECAST_CURRENT_META = {json.dumps(meta, ensure_ascii=False)};\n"
        )
        fc_path = output_dir / "forecast_current_data.js"
        fc_path.write_text(fc_text, encoding="utf-8")
        print(f"\nWrote {fc_path} ({len(fc_text):,} bytes)")

        fc14_text = (
            f"window.FORECAST_14DAY = {json.dumps(daily_src, ensure_ascii=False)};\n"
            f"window.FORECAST_14DAY_COORD = {json.dumps(daily_by_coord, ensure_ascii=False)};\n"
            f"window.FORECAST_14DAY_META = {json.dumps(meta, ensure_ascii=False)};\n"
        )
        fc14_path = output_dir / "forecast_14day_data.js"
        fc14_path.write_text(fc14_text, encoding="utf-8")
        print(f"Wrote {fc14_path} ({len(fc14_text):,} bytes)")

        elapsed = time.time() - started
        print(f"\n-- Done: {ok}/{len(catalog)} cities in {elapsed:.0f}s ({err} errors) --")

        if errors:
            print(f"\nFirst {min(10, len(errors))} errors:")
            for e in errors[:10]:
                print(f"  {e}")

    print("\nAll JS data files updated successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
