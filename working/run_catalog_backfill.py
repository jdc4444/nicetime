#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

BASE_DIR = str(Path(__file__).resolve().parent)
DEFAULT_DB = f"{BASE_DIR}/weather_data_v2.db"
DEFAULT_CATALOG = f"{BASE_DIR}/all_city_data.json"
DEFAULT_API_LOG = f"{BASE_DIR}/sync_api_calls.ndjson"
DEFAULT_SYNC_LOG = f"{BASE_DIR}/sync_runs.log"
DEFAULT_KEY_FILE = f"{BASE_DIR}/.visualcrossing_key"
DEFAULT_STATUS_FILE = f"{BASE_DIR}/backfill_status.json"

VC_URL = (
    "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
    "{lat},{lon}/{start}/{end}?unitGroup=metric&include=days{include_current}"
    "&key={key}&contentType=json"
)

# Explicit disambiguation map for ambiguous city names that collide globally.
# Key: (catalog city, catalog country) -> DB/API city key
CITY_COUNTRY_DISAMBIGUATION: dict[tuple[str, str], str] = {
    ("Hong Kong", "China (Hong Kong SAR)"): "Hong Kong, China",
    ("Macau", "China (Macau SAR)"): "Macau, China",
    ("George Town", "Cayman Islands"): "George Town, Cayman Islands",
    ("Granada", "Nicaragua"): "Granada, Nicaragua",
    ("Hamilton", "Bermuda"): "Hamilton, Bermuda",
    ("San Juan", "United States"): "San Juan, United States",
    ("Alexandria", "United States"): "Alexandria, United States",
    ("Georgetown, TX", "United States"): "Georgetown, TX",
}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def append_sync_log(sync_log_file: str, msg: str) -> None:
    line = f"[{utc_ts()}] {msg}"
    print(line, flush=True)
    with open(sync_log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def format_duration(sec: float) -> str:
    sec = int(max(0, sec))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def write_status_file(path: str, payload: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    Path(tmp).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def read_vc_key(key_file: str) -> str:
    env_key = os.environ.get("VISUAL_CROSSING_API_KEY", "").strip()
    if env_key:
        return env_key
    p = Path(key_file)
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return ""


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def ensure_support_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_city_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            city TEXT,
            stage TEXT,
            status TEXT,
            message TEXT,
            ts TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS city_coords (
            city TEXT PRIMARY KEY,
            lat REAL,
            lon REAL
        )
        """
    )
    conn.commit()


def insert_city_log(conn: sqlite3.Connection, run_id: str, city: str, stage: str, status: str, message: str) -> None:
    conn.execute(
        "INSERT INTO sync_city_log(run_id, city, stage, status, message, ts) VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, city, stage, status, message, utcnow_iso()),
    )


def append_api_log(api_log_path: str, row: dict[str, Any]) -> None:
    with open(api_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_counts_map(conn: sqlite3.Connection, table: str, start_date: str, end_date: str) -> dict[str, int]:
    rows = conn.execute(
        f"""
        SELECT city, COUNT(*) AS cnt
        FROM {table}
        WHERE date >= ? AND date <= ?
        GROUP BY city
        """,
        (start_date, end_date),
    ).fetchall()
    return {r["city"]: int(r["cnt"]) for r in rows}


def upsert_weather_row(conn: sqlite3.Connection, table: str, city: str, d: dict[str, Any], source: str) -> None:
    now = utcnow_iso()
    conn.execute(
        f"""
        INSERT INTO {table} (
            city, date, tmax_c, tmin_c, tavg_c, sunrise, sunset,
            precip_mm, precip_prob_pct, solarradiation_wm2, solarenergy_mj_m2,
            uvindex, moonphase, conditions_text, icon, description_text,
            source_provider, stations_text, severerisk, data_source, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(city, date) DO UPDATE SET
            tmax_c=excluded.tmax_c,
            tmin_c=excluded.tmin_c,
            tavg_c=excluded.tavg_c,
            sunrise=excluded.sunrise,
            sunset=excluded.sunset,
            precip_mm=excluded.precip_mm,
            precip_prob_pct=excluded.precip_prob_pct,
            solarradiation_wm2=excluded.solarradiation_wm2,
            solarenergy_mj_m2=excluded.solarenergy_mj_m2,
            uvindex=excluded.uvindex,
            moonphase=excluded.moonphase,
            conditions_text=excluded.conditions_text,
            icon=excluded.icon,
            description_text=excluded.description_text,
            source_provider=excluded.source_provider,
            stations_text=excluded.stations_text,
            severerisk=excluded.severerisk,
            data_source=excluded.data_source,
            updated_at=excluded.updated_at
        """,
        (
            city,
            str(d.get("datetime", "")),
            d.get("tempmax"),
            d.get("tempmin"),
            d.get("temp"),
            d.get("sunrise"),
            d.get("sunset"),
            d.get("precip"),
            d.get("precipprob"),
            d.get("solarradiation"),
            d.get("solarenergy"),
            d.get("uvindex"),
            d.get("moonphase"),
            d.get("conditions"),
            d.get("icon"),
            d.get("description"),
            "visualcrossing",
            ",".join(d.get("stations", [])) if isinstance(d.get("stations"), list) else d.get("stations"),
            d.get("severerisk"),
            source,
            now,
        ),
    )


class RateGate:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self.last_call = 0.0

    def wait(self) -> None:
        if self.min_interval_sec <= 0:
            return
        now = time.time()
        delay = self.min_interval_sec - (now - self.last_call)
        if delay > 0:
            time.sleep(delay)
        self.last_call = time.time()


def fetch_vc(
    key: str,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    include_current: bool,
    gate: RateGate,
    attempts: int,
) -> tuple[dict[str, Any], str, int]:
    include = ",current" if include_current else ""
    url = VC_URL.format(
        lat=lat,
        lon=lon,
        start=start_date,
        end=end_date,
        include_current=include,
        key=key,
    )
    last_err: Exception | None = None
    for i in range(1, max(1, attempts) + 1):
        gate.wait()
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_sec = float(retry_after) if retry_after and retry_after.isdigit() else min(60.0, 2.0 * i)
                time.sleep(sleep_sec)
                r.raise_for_status()
            if 500 <= r.status_code <= 599:
                r.raise_for_status()
            r.raise_for_status()
            return r.json(), url, r.status_code
        except Exception as e:
            last_err = e
            if i >= attempts:
                break
            time.sleep(min(30.0, 1.5 * i))
    assert last_err is not None
    raise last_err


def db_city_key(city: str, country: str) -> str:
    city_s = str(city or "").strip()
    country_s = str(country or "").strip()
    if not city_s:
        return ""
    mapped = CITY_COUNTRY_DISAMBIGUATION.get((city_s, country_s))
    if mapped:
        return mapped
    return city_s


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
        out.append(
            {
                "city": city,
                "country": country,
                "db_city": db_city_key(city, country),
                "continent": str(row.get("continent", "Unknown")).strip() or "Unknown",
                "lat": float(row.get("lat", 0.0)),
                "lng": float(row.get("lng", 0.0)),
            }
        )
    return out


def parse_csv_values(value: str) -> set[str]:
    if not value:
        return set()
    return {x.strip().lower() for x in str(value).split(",") if x.strip()}


def filter_catalog(catalog: list[dict[str, Any]], continents: set[str], countries: set[str]) -> list[dict[str, Any]]:
    out = []
    for row in catalog:
        cont_ok = True
        country_ok = True
        if continents:
            cont_ok = str(row.get("continent", "")).strip().lower() in continents
        if countries:
            country_ok = str(row.get("country", "")).strip().lower() in countries
        if cont_ok and country_ok:
            out.append(row)
    return out


def acquire_lock(lock_path: str) -> int:
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(lock_path, flags, 0o644)
    os.write(fd, f"{os.getpid()} {utcnow_iso()}\n".encode("utf-8"))
    return fd


def release_lock(fd: int, lock_path: str) -> None:
    try:
        os.close(fd)
    finally:
        if os.path.exists(lock_path):
            os.unlink(lock_path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Resumable Visual Crossing backfill runner for catalog cities.")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--catalog", default=DEFAULT_CATALOG)
    ap.add_argument("--api-log", default=DEFAULT_API_LOG)
    ap.add_argument("--sync-log", default=DEFAULT_SYNC_LOG)
    ap.add_argument("--key-file", default=DEFAULT_KEY_FILE)
    ap.add_argument("--mode", choices=["both", "estimated", "forecast"], default="both")
    ap.add_argument("--continent", default="", help="Filter to one or more continents (comma-separated)")
    ap.add_argument("--country", default="", help="Filter to one or more countries (comma-separated)")
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", action="store_false", dest="resume")
    ap.add_argument("--max-cities", type=int, default=0, help="Limit number of catalog cities for test runs")
    ap.add_argument("--min-interval-sec", type=float, default=0.15, help="Minimum delay between VC requests")
    ap.add_argument("--attempts", type=int, default=4)
    ap.add_argument("--status-file", default=DEFAULT_STATUS_FILE, help="Live JSON status output path")
    ap.add_argument("--live", action="store_true", default=True, help="Print per-city live updates in terminal")
    ap.add_argument("--quiet-live", action="store_false", dest="live", help="Disable per-city live output")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    key = read_vc_key(args.key_file)
    if not key:
        print("Missing Visual Crossing key. Set VISUAL_CROSSING_API_KEY or create .visualcrossing_key.", file=sys.stderr)
        return 2

    today = date.today()
    est_start = today.isoformat()
    est_end = (today + timedelta(days=364)).isoformat()
    fc_start = today.isoformat()
    fc_end = (today + timedelta(days=15)).isoformat()

    catalog = load_catalog(args.catalog)
    continent_filter = parse_csv_values(args.continent)
    country_filter = parse_csv_values(args.country)
    if continent_filter or country_filter:
        catalog = filter_catalog(catalog, continent_filter, country_filter)
    if args.max_cities > 0:
        catalog = catalog[: args.max_cities]
    if not catalog:
        print("No cities selected after filters.", file=sys.stderr)
        return 4

    lock_path = str(Path(args.db).with_suffix(".sync.lock"))
    lock_fd = -1
    try:
        lock_fd = acquire_lock(lock_path)
    except FileExistsError:
        print(f"Lock exists: {lock_path}. Another sync may be running.", file=sys.stderr)
        return 3

    run_id = str(uuid.uuid4())
    gate = RateGate(args.min_interval_sec)
    started = utcnow_iso()
    started_ts = time.time()

    conn = db_connect(args.db)
    ensure_support_tables(conn)
    conn.execute(
        "INSERT OR REPLACE INTO sync_runs(run_id, started_at, status, total_cities, notes) VALUES(?,?,?,?,?)",
        (
            run_id,
            started,
            "running",
            len(catalog),
            (
                f"script=run_catalog_backfill.py mode={args.mode} resume={args.resume} "
                f"continent={args.continent or '*'} country={args.country or '*'} "
                f"window_est={est_start}..{est_end} window_fc={fc_start}..{fc_end}"
            ),
        ),
    )
    conn.commit()

    append_sync_log(args.sync_log, f"Backfill run_id={run_id}")
    append_sync_log(args.sync_log, f"DB: {args.db}")
    append_sync_log(args.sync_log, f"Catalog: {args.catalog}")
    append_sync_log(
        args.sync_log,
        f"Filters: continent={args.continent or '*'} country={args.country or '*'}",
    )
    append_sync_log(args.sync_log, f"Mode={args.mode} Resume={args.resume} Cities={len(catalog)}")
    append_sync_log(args.sync_log, f"Live status file: {args.status_file}")

    try:
        est_counts = build_counts_map(conn, "daily_data_estimated", est_start, est_end)
        fc_counts = build_counts_map(conn, "daily_data_forecast", fc_start, fc_end)

        est_complete = 0
        fc_complete = 0
        to_pull_est = 0
        to_pull_fc = 0
        for c in catalog:
            city = c["db_city"]
            e_ok = est_counts.get(city, 0) >= 365
            f_ok = fc_counts.get(city, 0) >= 14
            if e_ok:
                est_complete += 1
            if f_ok:
                fc_complete += 1
            if args.mode in {"both", "estimated"} and (not args.resume or not e_ok):
                to_pull_est += 1
            if args.mode in {"both", "forecast"} and (not args.resume or not f_ok):
                to_pull_fc += 1

        append_sync_log(
            args.sync_log,
            "Before sync: "
            f"estimated complete={est_complete}, missing={len(catalog)-est_complete}; "
            f"forecast complete={fc_complete}, missing={len(catalog)-fc_complete}; "
            f"to_pull_est={to_pull_est}, to_pull_fc={to_pull_fc}",
        )
        write_status_file(
            args.status_file,
            {
                "run_id": run_id,
                "state": "running",
                "started_at": started,
                "db": args.db,
                "catalog": args.catalog,
                "filters": {"continent": args.continent or "*", "country": args.country or "*"},
                "mode": args.mode,
                "resume": args.resume,
                "total_cities": len(catalog),
                "done": 0,
                "ok": 0,
                "err": 0,
                "est_complete_before": est_complete,
                "fc_complete_before": fc_complete,
                "to_pull_est": to_pull_est,
                "to_pull_fc": to_pull_fc,
                "current_city": None,
                "last_message": "starting",
                "updated_at": utcnow_iso(),
            },
        )

        if args.dry_run:
            append_sync_log(args.sync_log, "Dry run only. No API calls made.")
            write_status_file(
                args.status_file,
                {
                    "run_id": run_id,
                    "state": "dry_run",
                    "finished_at": utcnow_iso(),
                    "total_cities": len(catalog),
                    "done": 0,
                    "ok": 0,
                    "err": 0,
                    "last_message": "dry run complete",
                    "updated_at": utcnow_iso(),
                },
            )
            conn.execute(
                "UPDATE sync_runs SET status='dry_run', finished_at=?, historical_complete=?, historical_missing=?, forecast_fresh=?, forecast_stale=?, historical_updated=0, forecast_updated=0, errors=0 WHERE run_id=?",
                (
                    utcnow_iso(),
                    est_complete,
                    len(catalog) - est_complete,
                    fc_complete,
                    len(catalog) - fc_complete,
                    run_id,
                ),
            )
            conn.commit()
            return 0

        done = 0
        ok = 0
        err = 0
        est_updated_cities = 0
        fc_updated_cities = 0

        for c in catalog:
            city = c["db_city"]
            label = f"{c['city']}, {c['country']}"
            lat = c["lat"]
            lon = c["lng"]
            done += 1
            city_action = []

            e_ok = est_counts.get(city, 0) >= 365
            f_ok = fc_counts.get(city, 0) >= 14
            pull_est = args.mode in {"both", "estimated"} and (not args.resume or not e_ok)
            pull_fc = args.mode in {"both", "forecast"} and (not args.resume or not f_ok)
            if args.mode in {"both", "estimated"}:
                city_action.append("est:fetch" if pull_est else "est:skip")
            if args.mode in {"both", "forecast"}:
                city_action.append("fc:fetch" if pull_fc else "fc:skip")

            if not pull_est and not pull_fc:
                if args.mode in {"both", "estimated"}:
                    insert_city_log(conn, run_id, city, "estimated", "complete", "already complete")
                if args.mode in {"both", "forecast"}:
                    insert_city_log(conn, run_id, city, "forecast", "complete", "already complete")
                conn.commit()
                ok += 1
                elapsed = time.time() - started_ts
                rate = done / elapsed if elapsed > 0 else 0.0
                rem = len(catalog) - done
                eta = rem / rate if rate > 0 else 0
                msg = (
                    f"[{done}/{len(catalog)}] {city} ({', '.join(city_action)}) "
                    f"ok={ok} err={err} elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
                )
                if args.live:
                    print(msg, flush=True)
                write_status_file(
                    args.status_file,
                    {
                        "run_id": run_id,
                        "state": "running",
                        "mode": args.mode,
                        "done": done,
                        "total_cities": len(catalog),
                        "ok": ok,
                        "err": err,
                        "elapsed_sec": round(elapsed, 1),
                        "eta_sec": round(eta, 1),
                        "current_city": label,
                        "db_city": city,
                        "current_action": city_action,
                        "last_message": msg,
                        "updated_at": utcnow_iso(),
                    },
                )
                if done % 50 == 0 or done == len(catalog):
                    append_sync_log(args.sync_log, f"Progress {done}/{len(catalog)} ok={ok} err={err} (skip)")
                continue

            try:
                conn.execute("INSERT OR IGNORE INTO city_coords(city, lat, lon) VALUES(?,?,?)", (city, lat, lon))

                if pull_est:
                    payload, url, status_code = fetch_vc(
                        key, lat, lon, est_start, est_end, include_current=False, gate=gate, attempts=args.attempts
                    )
                    days = payload.get("days", []) or []
                    for d in days:
                        upsert_weather_row(conn, "daily_data_estimated", city, d, "estimated")
                        upsert_weather_row(conn, "daily_data", city, d, "estimated")
                    insert_city_log(conn, run_id, city, "estimated", "updated", f"rows={len(days)}")
                    append_api_log(
                        args.api_log,
                        {
                            "city": city,
                            "catalog_city": c["city"],
                            "catalog_country": c["country"],
                            "kind": "estimated_window",
                            "provider": "visualcrossing",
                            "status_code": status_code,
                            "ok": True,
                            "url": url.replace(key, "***"),
                            "lat": lat,
                            "lon": lon,
                            "records": len(days),
                            "start_date": est_start,
                            "end_date": est_end,
                            "sample_tmax_c": (days[0].get("tempmax") if days else None),
                            "sample_tmin_c": (days[0].get("tempmin") if days else None),
                            "sample_precip_mm": (days[0].get("precip") if days else None),
                            "sample_precip_prob_pct": (days[0].get("precipprob") if days else None),
                            "sample_solarradiation_wm2": (days[0].get("solarradiation") if days else None),
                            "ts": utcnow_iso(),
                        },
                    )
                    est_counts[city] = len(days)
                    est_updated_cities += 1
                elif args.mode in {"both", "estimated"}:
                    insert_city_log(conn, run_id, city, "estimated", "complete", "already complete")

                if pull_fc:
                    payload, url, status_code = fetch_vc(
                        key, lat, lon, fc_start, fc_end, include_current=True, gate=gate, attempts=args.attempts
                    )
                    days = payload.get("days", []) or []
                    cur = payload.get("currentConditions", {}) or {}
                    for d in days:
                        upsert_weather_row(conn, "daily_data_forecast", city, d, "forecast")
                        upsert_weather_row(conn, "daily_data", city, d, "forecast")
                    insert_city_log(conn, run_id, city, "forecast", "updated", f"rows={len(days)}")
                    append_api_log(
                        args.api_log,
                        {
                            "city": city,
                            "catalog_city": c["city"],
                            "catalog_country": c["country"],
                            "kind": "forecast_bundle",
                            "provider": "visualcrossing",
                            "status_code": status_code,
                            "ok": True,
                            "url": url.replace(key, "***"),
                            "lat": lat,
                            "lon": lon,
                            "records": len(days),
                            "start_date": fc_start,
                            "end_date": fc_end,
                            "current_temp_c": cur.get("temp"),
                            "sample_tmax_c": (days[0].get("tempmax") if days else None),
                            "sample_tmin_c": (days[0].get("tempmin") if days else None),
                            "sample_precip_mm": (days[0].get("precip") if days else None),
                            "sample_precip_prob_pct": (days[0].get("precipprob") if days else None),
                            "sample_solarradiation_wm2": (days[0].get("solarradiation") if days else None),
                            "ts": utcnow_iso(),
                        },
                    )
                    fc_counts[city] = len(days)
                    fc_updated_cities += 1
                elif args.mode in {"both", "forecast"}:
                    insert_city_log(conn, run_id, city, "forecast", "complete", "already complete")

                conn.commit()
                ok += 1
                elapsed = time.time() - started_ts
                rate = done / elapsed if elapsed > 0 else 0.0
                rem = len(catalog) - done
                eta = rem / rate if rate > 0 else 0
                msg = (
                    f"[{done}/{len(catalog)}] {city} ({', '.join(city_action)}) "
                    f"ok={ok} err={err} elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
                )
                if args.live:
                    print(msg, flush=True)
                write_status_file(
                    args.status_file,
                    {
                        "run_id": run_id,
                        "state": "running",
                        "mode": args.mode,
                        "done": done,
                        "total_cities": len(catalog),
                        "ok": ok,
                        "err": err,
                        "elapsed_sec": round(elapsed, 1),
                        "eta_sec": round(eta, 1),
                        "current_city": label,
                        "db_city": city,
                        "current_action": city_action,
                        "last_message": msg,
                        "updated_at": utcnow_iso(),
                    },
                )
            except Exception as e:
                conn.rollback()
                msg = str(e)
                if args.mode in {"both", "estimated"}:
                    insert_city_log(conn, run_id, city, "estimated", "error", msg)
                if args.mode in {"both", "forecast"}:
                    insert_city_log(conn, run_id, city, "forecast", "error", msg)
                conn.commit()
                err += 1
                elapsed = time.time() - started_ts
                rate = done / elapsed if elapsed > 0 else 0.0
                rem = len(catalog) - done
                eta = rem / rate if rate > 0 else 0
                line = (
                    f"[{done}/{len(catalog)}] {city} ({', '.join(city_action)}) ERROR: {msg} "
                    f"ok={ok} err={err} elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
                )
                if args.live:
                    print(line, flush=True)
                write_status_file(
                    args.status_file,
                    {
                        "run_id": run_id,
                        "state": "running",
                        "mode": args.mode,
                        "done": done,
                        "total_cities": len(catalog),
                        "ok": ok,
                        "err": err,
                        "elapsed_sec": round(elapsed, 1),
                        "eta_sec": round(eta, 1),
                        "current_city": label,
                        "db_city": city,
                        "current_action": city_action,
                        "last_message": line,
                        "last_error": msg,
                        "updated_at": utcnow_iso(),
                    },
                )

            if done % 25 == 0 or done == len(catalog):
                append_sync_log(args.sync_log, f"Progress {done}/{len(catalog)} ok={ok} err={err}")

        est_complete_after = 0
        fc_complete_after = 0
        for c in catalog:
            city = c["db_city"]
            if est_counts.get(city, 0) >= 365:
                est_complete_after += 1
            if fc_counts.get(city, 0) >= 14:
                fc_complete_after += 1

        append_sync_log(
            args.sync_log,
            "After sync: "
            f"estimated complete={est_complete_after}, missing={len(catalog)-est_complete_after}; "
            f"forecast complete={fc_complete_after}, missing={len(catalog)-fc_complete_after}",
        )
        write_status_file(
            args.status_file,
            {
                "run_id": run_id,
                "state": "done" if err == 0 else "partial",
                "finished_at": utcnow_iso(),
                "mode": args.mode,
                "done": len(catalog),
                "total_cities": len(catalog),
                "ok": ok,
                "err": err,
                "historical_complete": est_complete_after,
                "historical_missing": len(catalog) - est_complete_after,
                "forecast_complete": fc_complete_after,
                "forecast_missing": len(catalog) - fc_complete_after,
                "historical_updated": est_updated_cities,
                "forecast_updated": fc_updated_cities,
                "updated_at": utcnow_iso(),
            },
        )

        conn.execute(
            """
            UPDATE sync_runs
            SET finished_at=?,
                status=?,
                historical_complete=?,
                historical_missing=?,
                forecast_fresh=?,
                forecast_stale=?,
                historical_updated=?,
                forecast_updated=?,
                errors=?
            WHERE run_id=?
            """,
            (
                utcnow_iso(),
                "ok" if err == 0 else "partial",
                est_complete_after,
                len(catalog) - est_complete_after,
                fc_complete_after,
                len(catalog) - fc_complete_after,
                est_updated_cities,
                fc_updated_cities,
                err,
                run_id,
            ),
        )
        conn.commit()
        return 0 if err == 0 else 1
    finally:
        conn.close()
        if lock_fd >= 0:
            release_lock(lock_fd, lock_path)


if __name__ == "__main__":
    raise SystemExit(main())
