#!/usr/bin/env python3
import argparse
import json
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_DEFAULT = "/Users/jos/Desktop/Archive/weather_data_v2.db"
CATALOG_DEFAULT = "/Users/jos/Desktop/Archive/all_city_data.json"
REPORT_DEFAULT = "/Users/jos/Desktop/Archive/reconcile_catalog_report.json"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def base_city_name(s: str) -> str:
    s = str(s or "").strip()
    if "," in s:
        s = s.split(",", 1)[0].strip()
    s = re.sub(r"\([^)]*\)", " ", s)
    return " ".join(s.split())


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]


def build_upsert_sql(table: str, cols: list[str]) -> str:
    insert_cols = ",".join(cols)
    placeholders = ",".join(["?"] + [f"s.{c}" for c in cols if c != "city"])
    update_cols = [c for c in cols if c not in {"city", "date"}]
    update_set = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
    return (
        f"INSERT INTO {table} ({insert_cols}) "
        f"SELECT {placeholders} FROM {table} s WHERE s.city=? "
        f"ON CONFLICT(city,date) DO UPDATE SET {update_set}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile catalog city names from existing DB city keys using strict coordinate/name matching.")
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--catalog", default=CATALOG_DEFAULT)
    ap.add_argument("--report", default=REPORT_DEFAULT)
    ap.add_argument("--max-distance", type=float, default=0.20, help="Max lat/lon manhattan distance for base-name matches")
    ap.add_argument("--exact-distance", type=float, default=0.03, help="Auto-accept if within this distance regardless of name")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    catalog = json.loads(Path(args.catalog).read_text(encoding="utf-8"))
    with db_connect(args.db) as conn:
        est_cols = table_columns(conn, "daily_data_estimated")
        fc_cols = table_columns(conn, "daily_data_forecast")
        best_cols = table_columns(conn, "daily_data")
        upsert_est = build_upsert_sql("daily_data_estimated", est_cols)
        upsert_fc = build_upsert_sql("daily_data_forecast", fc_cols)
        upsert_best = build_upsert_sql("daily_data", best_cols)

        est_counts = {r["city"]: int(r["cnt"]) for r in conn.execute("SELECT city, COUNT(*) cnt FROM daily_data_estimated GROUP BY city").fetchall()}
        fc_counts = {r["city"]: int(r["cnt"]) for r in conn.execute("SELECT city, COUNT(*) cnt FROM daily_data_forecast GROUP BY city").fetchall()}

        coords = [
            {
                "city": r["city"],
                "lat": float(r["lat"] or 0.0),
                "lon": float(r["lon"] or 0.0),
                "base": norm_text(base_city_name(r["city"])),
            }
            for r in conn.execute("SELECT city, lat, lon FROM city_coords").fetchall()
        ]

        candidates = []
        for r in coords:
            city = r["city"]
            ec = est_counts.get(city, 0)
            fc = fc_counts.get(city, 0)
            if ec >= 365 and fc >= 14:
                r2 = dict(r)
                r2["est"] = ec
                r2["fc"] = fc
                candidates.append(r2)

        total = len(catalog)
        already_complete = 0
        mapped = 0
        unmapped = 0
        copied_est = 0
        copied_fc = 0
        copied_best = 0
        report_rows: list[dict[str, Any]] = []

        for row in catalog:
            city = str(row.get("city", "")).strip()
            country = str(row.get("country", "")).strip()
            lat = float(row.get("lat", 0.0))
            lon = float(row.get("lng", 0.0))
            if not city:
                continue

            ec = est_counts.get(city, 0)
            fc = fc_counts.get(city, 0)
            if ec >= 365 and fc >= 14:
                already_complete += 1
                continue

            cbase = norm_text(base_city_name(city))
            best = None
            best_score = -10**9
            for cand in candidates:
                dist = abs(lat - cand["lat"]) + abs(lon - cand["lon"])
                if dist > args.max_distance:
                    continue
                name_match = cand["base"] == cbase and cbase != ""
                if not name_match and dist > args.exact_distance:
                    continue

                score = 0.0
                score += (1000.0 - dist * 1000.0)
                if name_match:
                    score += 200.0
                if score > best_score:
                    best_score = score
                    best = cand

            if not best:
                unmapped += 1
                report_rows.append(
                    {
                        "city": city,
                        "country": country,
                        "status": "unmapped",
                        "reason": "no safe candidate",
                    }
                )
                continue

            src_city = best["city"]
            mapped += 1
            if args.apply:
                cur = conn.execute(upsert_est, (city, src_city))
                est_rows = int(cur.rowcount or 0)
                cur = conn.execute(upsert_fc, (city, src_city))
                fc_rows = int(cur.rowcount or 0)
                cur = conn.execute(upsert_best, (city, src_city))
                best_rows = int(cur.rowcount or 0)
                copied_est += max(0, est_rows)
                copied_fc += max(0, fc_rows)
                copied_best += max(0, best_rows)
                conn.execute("INSERT OR REPLACE INTO city_coords(city, lat, lon) VALUES(?,?,?)", (city, lat, lon))

            report_rows.append(
                {
                    "city": city,
                    "country": country,
                    "status": "mapped",
                    "source_city": src_city,
                    "distance": round(abs(lat - best["lat"]) + abs(lon - best["lon"]), 6),
                    "source_est_rows": best["est"],
                    "source_fc_rows": best["fc"],
                }
            )

        if args.apply:
            conn.commit()

        est_counts_after = {r["city"]: int(r["cnt"]) for r in conn.execute("SELECT city, COUNT(*) cnt FROM daily_data_estimated GROUP BY city").fetchall()}
        fc_counts_after = {r["city"]: int(r["cnt"]) for r in conn.execute("SELECT city, COUNT(*) cnt FROM daily_data_forecast GROUP BY city").fetchall()}
        complete_after = 0
        for row in catalog:
            city = str(row.get("city", "")).strip()
            if est_counts_after.get(city, 0) >= 365 and fc_counts_after.get(city, 0) >= 14:
                complete_after += 1

    report = {
        "generated_at": utcnow_iso(),
        "db": args.db,
        "catalog": args.catalog,
        "apply": args.apply,
        "total_catalog": total,
        "already_complete_before": already_complete,
        "mapped": mapped,
        "unmapped": unmapped,
        "copied_rows_estimated": copied_est,
        "copied_rows_forecast": copied_fc,
        "copied_rows_best": copied_best,
        "complete_after": complete_after,
        "missing_after": total - complete_after,
        "rows": report_rows,
    }
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({k: v for k, v in report.items() if k != "rows"}, ensure_ascii=False, indent=2))
    print(f"Report written: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

