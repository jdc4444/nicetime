#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import threading
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote_plus, urlparse

import requests

BASE_DIR = str(Path(__file__).resolve().parent)
DB_DEFAULT = f"{BASE_DIR}/weather_data_v2.db"
CATALOG_DEFAULT = f"{BASE_DIR}/all_city_data.json"
API_LOG_PATH = f"{BASE_DIR}/sync_api_calls.ndjson"
KEY_FILE = f"{BASE_DIR}/.visualcrossing_key"

VC_URL = (
    "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
    "{lat},{lon}/{start}/{end}?unitGroup=metric&include=days{include_current}"
    "&key={key}&contentType=json"
)

HTML = """<!doctype html>
<html>
<head>
<meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>City Weather Manager</title>
<style>
:root {
  --bg:#f6f7fb; --card:#fff; --ink:#1f2937; --muted:#6b7280; --line:#d1d5db;
  --green:#16a34a; --yellow:#ca8a04; --red:#dc2626; --accent:#2563eb;
}
* { box-sizing:border-box; }
body { margin:0; font-family: ui-sans-serif,-apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--ink); }
.top {
  position:sticky; top:0; z-index:2; background:linear-gradient(180deg,#e8eefc,#f6f7fb);
  border-bottom:1px solid var(--line); padding:12px 14px;
}
.h1 { font-size:24px; font-weight:800; }
.sub { color:var(--muted); font-size:13px; margin-top:2px; }
.controls { display:flex; gap:8px; flex-wrap:wrap; margin-top:10px; }
button, select, input {
  border:1px solid var(--line); border-radius:8px; background:#fff; color:var(--ink); padding:8px 10px; font-size:13px;
}
button.primary { background:var(--accent); color:white; border-color:var(--accent); }
.layout { display:grid; grid-template-columns: 380px 1fr; gap:10px; padding:10px; }
@media (max-width:1000px){ .layout { grid-template-columns: 1fr; } }
.panel { background:var(--card); border:1px solid var(--line); border-radius:12px; }
.panel h3 { margin:0; padding:10px 12px; border-bottom:1px solid var(--line); font-size:15px; }
.treewrap { max-height: calc(100vh - 210px); overflow:auto; padding:8px 10px 12px; }
.detailwrap { padding:10px; }
.node { margin:3px 0; }
.continent { font-weight:700; cursor:pointer; padding:5px; border-radius:7px; }
.country { margin-left:12px; font-weight:600; cursor:pointer; padding:4px; border-radius:7px; }
.city { margin-left:24px; cursor:pointer; padding:4px; border-radius:7px; display:flex; justify-content:space-between; align-items:center; }
.city:hover, .country:hover, .continent:hover { background:#f3f4f6; }
.city.active { background:#e8efff; border:1px solid #c5d5ff; }
.badges { display:flex; gap:6px; align-items:center; }
.dot { width:10px; height:10px; border-radius:999px; display:inline-block; }
.dot.green { background:var(--green); }
.dot.yellow { background:var(--yellow); }
.dot.red { background:var(--red); }
.legend { display:flex; gap:14px; align-items:center; font-size:12px; color:var(--muted); margin:6px 0 4px; }
.kpis { display:grid; grid-template-columns: repeat(4,minmax(0,1fr)); gap:8px; margin-bottom:10px; }
@media (max-width:1100px){ .kpis { grid-template-columns: repeat(2,minmax(0,1fr)); } }
.kpi { border:1px solid var(--line); border-radius:10px; padding:8px; background:#fafbff; }
.kpi .l { color:var(--muted); font-size:11px; text-transform:uppercase; }
.kpi .v { font-size:22px; font-weight:800; }
table { width:100%; border-collapse:collapse; font-size:12px; }
th,td { border-bottom:1px solid var(--line); text-align:left; padding:6px; vertical-align:top; }
th { color:var(--muted); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.grid2 { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
@media (max-width:1000px){ .grid2 { grid-template-columns:1fr; } }
.callout { border:1px solid var(--line); border-radius:10px; padding:8px; margin-bottom:8px; background:#fbfdff; }
.small { font-size:12px; color:var(--muted); }
</style>
</head>
<body>
  <div class='top'>
    <div class='h1'>City Weather Manager</div>
    <div class='sub' id='meta'>Loading…</div>
    <div class='controls'>
      <input id='filter' placeholder='Filter city/country/continent' style='min-width:280px'/>
      <select id='cityscope'>
        <option value='catalog'>Catalog Cities</option>
        <option value='all'>Catalog + DB Cities</option>
        <option value='db'>DB Cities Only</option>
      </select>
      <select id='kind'>
        <option value='both'>Pull forecast + estimated</option>
        <option value='forecast'>Pull forecast only</option>
        <option value='estimated'>Pull estimated only</option>
      </select>
      <button class='primary' onclick='refreshScope("all", "")'>Pull All</button>
      <button onclick='refreshScope("continent", getSelectedContinent())'>Pull Continent</button>
      <button onclick='refreshScope("country", getSelectedCountry())'>Pull Country</button>
      <button onclick='refreshScope("city", getSelectedCityName())'>Pull City</button>
    </div>
    <div class='small' id='jobstatus'></div>
  </div>

  <div class='layout'>
    <div class='panel'>
      <h3>Cities (Continent > Country > City)</h3>
      <div class='treewrap'>
        <div class='legend'>
          <span><span class='dot green'></span> complete</span>
          <span><span class='dot yellow'></span> missing</span>
          <span><span class='dot red'></span> failed</span>
          <span>left dot = estimated, right dot = forecast</span>
        </div>
        <div id='tree'></div>
      </div>
    </div>

    <div class='panel'>
      <h3>City Detail</h3>
      <div class='detailwrap'>
        <div class='kpis'>
          <div class='kpi'><div class='l'>Total Cities</div><div class='v' id='k_total'>-</div></div>
          <div class='kpi'><div class='l'>Both Complete</div><div class='v' id='k_both'>-</div></div>
          <div class='kpi'><div class='l'>Missing Any</div><div class='v' id='k_missing'>-</div></div>
          <div class='kpi'><div class='l'>Failed Any</div><div class='v' id='k_failed'>-</div></div>
        </div>

        <div class='callout' id='citymeta'>Select a city…</div>

        <div class='grid2'>
          <div>
            <h4>Estimated / Expected Data</h4>
            <div class='small' id='est_summary'></div>
            <table><thead><tr>
              <th>Date</th><th>TmaxC</th><th>TminC</th><th>Precip</th><th>Solar</th><th>Updated</th>
            </tr></thead><tbody id='est_rows'></tbody></table>
          </div>
          <div>
            <h4>Forecast Data</h4>
            <div class='small' id='fc_summary'></div>
            <table><thead><tr>
              <th>Date</th><th>TmaxC</th><th>TminC</th><th>Precip%</th><th>Solar</th><th>Updated</th>
            </tr></thead><tbody id='fc_rows'></tbody></table>
          </div>
        </div>

        <h4 style='margin-top:12px'>Latest Forecast Pull</h4>
        <div id='latest_call' class='small mono'></div>

        <h4 style='margin-top:12px'>Missing / Failed Snapshot</h4>
        <table><thead><tr>
          <th>City</th><th>Country</th><th>Estimated</th><th>Forecast</th><th>Last Error</th>
        </tr></thead><tbody id='missing_rows'></tbody></table>
      </div>
    </div>
  </div>

<script>
let treeData = [];
let cityIndex = {};
let selectedCityId = null;
let selectedContinent = '';
let selectedCountry = '';
let lastJobId = null;

function esc(s){ return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
function dot(c){ return `<span class='dot ${c}'></span>`; }

function getSelectedCityName(){
  if(!selectedCityId || !cityIndex[selectedCityId]) return '';
  return cityIndex[selectedCityId].refresh_city || cityIndex[selectedCityId].city;
}
function getSelectedContinent(){ return selectedContinent || ''; }
function getSelectedCountry(){ return selectedCountry || ''; }

function renderTree(){
  const q = (document.getElementById('filter').value || '').toLowerCase().trim();
  let html = '';
  for(const cont of treeData){
    const contText = `${cont.name}`.toLowerCase();
    let contMatch = !q || contText.includes(q);
    let countryHtml = '';
    for(const ctry of cont.countries){
      const countryText = `${ctry.name}`.toLowerCase();
      let countryMatch = !q || contMatch || countryText.includes(q);
      let cityHtml = '';
      for(const city of ctry.cities){
        const cityText = `${city.city} ${ctry.name} ${cont.name}`.toLowerCase();
        if(q && !cityText.includes(q) && !countryMatch) continue;
        countryMatch = true;
        const active = city.id === selectedCityId ? 'active' : '';
        cityHtml += `<div class='city ${active}' onclick='selectCity("${city.id}")'>
          <span>${esc(city.city)} <span class='small'>${city.source_type === 'db' ? '[db]' : ''}</span></span>
          <span class='badges' title='estimated / forecast'>${dot(city.estimated_status)}${dot(city.forecast_status)}</span>
        </div>`;
      }
      if(!countryMatch && !cityHtml) continue;
      countryHtml += `<div class='node'>
        <div class='country' onclick='selectedContinent="${esc(cont.name)}";selectedCountry="${esc(ctry.name)}";'>${esc(ctry.name)} <span class='small'>(${ctry.cities.length})</span></div>
        ${cityHtml}
      </div>`;
    }
    if(!contMatch && !countryHtml) continue;
    html += `<div class='node'>
      <div class='continent' onclick='selectedContinent="${esc(cont.name)}"; selectedCountry="";'>${esc(cont.name)} <span class='small'>(${cont.countries.length} countries)</span></div>
      ${countryHtml}
    </div>`;
  }
  document.getElementById('tree').innerHTML = html || '<div class="small">No matches.</div>';
}

async function loadTree(){
  const scope = document.getElementById('cityscope').value || 'catalog';
  const r = await fetch('/api/tree?scope=' + encodeURIComponent(scope));
  const j = await r.json();
  treeData = j.continents || [];
  cityIndex = j.city_index || {};
  document.getElementById('meta').textContent = `DB: ${j.db_path} • scope=${j.scope || 'catalog'} • generated ${j.generated_at}`;
  document.getElementById('k_total').textContent = j.stats.total;
  document.getElementById('k_both').textContent = j.stats.both_complete;
  document.getElementById('k_missing').textContent = j.stats.missing_any;
  document.getElementById('k_failed').textContent = j.stats.failed_any;
  if(!selectedCityId){
    const first = Object.keys(cityIndex)[0];
    if(first) selectedCityId = first;
  }else if(!cityIndex[selectedCityId]){
    selectedCityId = Object.keys(cityIndex)[0] || null;
  }
  renderTree();
  renderMissing(j.missing_rows || []);
  if(selectedCityId) await loadCity(selectedCityId);
}

function renderMissing(rows){
  const html = rows.slice(0,200).map(r => `<tr>
    <td>${esc(r.city)}</td><td>${esc(r.country)}</td>
    <td>${dot(r.estimated_status)} ${esc(r.estimated_note || '')}</td>
    <td>${dot(r.forecast_status)} ${esc(r.forecast_note || '')}</td>
    <td class='mono'>${esc(r.last_error || '')}</td>
  </tr>`).join('');
  document.getElementById('missing_rows').innerHTML = html || '<tr><td colspan="5">No missing/failed cities.</td></tr>';
}

async function loadCity(id){
  const r = await fetch('/api/city?id=' + encodeURIComponent(id));
  const j = await r.json();
  const c = j.city || {};
  document.getElementById('citymeta').innerHTML = `<b>${esc(c.city || '')}, ${esc(c.country || '')}</b> • ${esc(c.continent || '')}<br/>`+
    `<span class='small'>source: ${esc(c.source_type || '')} • db key: ${esc(c.db_city || c.city || '')} • lat/lon: ${esc(c.lat)} , ${esc(c.lng)} • statuses: ${dot(j.estimated.status)} estimated, ${dot(j.forecast.status)} forecast</span>`;

  document.getElementById('est_summary').textContent = j.estimated.summary || 'No estimated data';
  document.getElementById('fc_summary').textContent = j.forecast.summary || 'No forecast data';

  const erows = (j.estimated.rows || []).map(r => `<tr>
    <td>${esc(r.date)}</td><td>${esc(r.tmax_c)}</td><td>${esc(r.tmin_c)}</td><td>${esc(r.precip_mm)}</td><td>${esc(r.solarradiation_wm2)}</td><td class='mono'>${esc(r.updated_at)}</td>
  </tr>`).join('');
  document.getElementById('est_rows').innerHTML = erows || '<tr><td colspan="6">No rows</td></tr>';

  const frows = (j.forecast.rows || []).map(r => `<tr>
    <td>${esc(r.date)}</td><td>${esc(r.tmax_c)}</td><td>${esc(r.tmin_c)}</td><td>${esc(r.precip_prob_pct)}</td><td>${esc(r.solarradiation_wm2)}</td><td class='mono'>${esc(r.updated_at)}</td>
  </tr>`).join('');
  document.getElementById('fc_rows').innerHTML = frows || '<tr><td colspan="6">No rows</td></tr>';

  document.getElementById('latest_call').textContent = j.latest_call_text || 'No API call found for this city.';
}

function selectCity(id){
  selectedCityId = id;
  renderTree();
  loadCity(id);
}

async function refreshScope(scope, name){
  const kind = document.getElementById('kind').value;
  const payload = { scope, name, kind };
  const r = await fetch('/api/refresh', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(payload)});
  const j = await r.json();
  if(!j.ok){
    document.getElementById('jobstatus').textContent = `Refresh failed: ${j.error || 'unknown'}`;
    return;
  }
  lastJobId = j.job_id;
  document.getElementById('jobstatus').textContent = `Job ${j.job_id} started for ${j.city_count} cities…`;
}

async function pollJob(){
  if(!lastJobId) return;
  const r = await fetch('/api/job?id=' + encodeURIComponent(lastJobId));
  const j = await r.json();
  if(!j.job) return;
  const x = j.job;
  document.getElementById('jobstatus').textContent = `Job ${x.id}: ${x.state} • ${x.done}/${x.total} • ok=${x.ok} err=${x.err} • ${x.stage}`;
  if(x.state === 'done' || x.state === 'error'){
    await loadTree();
    lastJobId = null;
  }
}

document.getElementById('filter').addEventListener('input', renderTree);
document.getElementById('cityscope').addEventListener('change', () => { selectedCityId = null; loadTree(); });

loadTree();
setInterval(loadTree, 10000);
setInterval(pollJob, 2000);
</script>
</body>
</html>
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_city_id(city: str, country: str) -> str:
    return f"{city}|{country}"


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


def get_vc_key() -> str:
    key = os.environ.get("VISUAL_CROSSING_API_KEY", "").strip()
    if key:
        return key
    p = Path(KEY_FILE)
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return ""


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


class AppState:
    def __init__(self, db_path: str, catalog_path: str, api_log_path: str):
        self.db_path = db_path
        self.catalog_path = catalog_path
        self.api_log_path = api_log_path
        self.jobs: dict[str, dict[str, Any]] = {}
        self.jobs_lock = threading.Lock()
        self.catalog = self._load_catalog()
        self.catalog_by_id = {normalize_city_id(c["city"], c["country"]): c for c in self.catalog}
        self.city_to_db = self._build_city_resolution()
        self.api_cache_mtime = 0.0
        self.api_cache_rows: list[dict[str, Any]] = []

    def _load_catalog(self) -> list[dict[str, Any]]:
        data = json.loads(Path(self.catalog_path).read_text(encoding="utf-8"))
        out = []
        for row in data:
            city = str(row.get("city", "")).strip()
            country = str(row.get("country", "")).strip()
            if not city or not country:
                continue
            out.append(
                {
                    "city": city,
                    "country": country,
                    "continent": str(row.get("continent", "Unknown")).strip() or "Unknown",
                    "lat": float(row.get("lat", 0.0)),
                    "lng": float(row.get("lng", 0.0)),
                    "zip": str(row.get("zip", "")),
                }
            )
        return out

    def _load_api_rows(self) -> list[dict[str, Any]]:
        p = Path(self.api_log_path)
        if not p.exists():
            return []
        mtime = p.stat().st_mtime
        if mtime == self.api_cache_mtime:
            return self.api_cache_rows
        rows = []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
        self.api_cache_mtime = mtime
        self.api_cache_rows = rows
        return rows

    def _latest_api_call_for_city(self, city: str) -> Optional[dict[str, Any]]:
        rows = self._load_api_rows()
        for row in reversed(rows):
            if row.get("city") == city and row.get("kind") == "forecast_bundle":
                return row
        return None

    def _bucket_key(self, lat: float, lon: float) -> tuple[int, int]:
        return (int(round(lat * 10.0)), int(round(lon * 10.0)))

    def _build_city_resolution(self) -> dict[str, str]:
        with db_connect(self.db_path) as conn:
            db_rows = [
                {"city": r["city"], "lat": float(r["lat"] or 0.0), "lon": float(r["lon"] or 0.0)}
                for r in conn.execute("SELECT city, lat, lon FROM city_coords").fetchall()
            ]
            est_cities = {r["city"] for r in conn.execute("SELECT DISTINCT city FROM daily_data_estimated").fetchall()}
            fc_cities = {r["city"] for r in conn.execute("SELECT DISTINCT city FROM daily_data_forecast").fetchall()}

        db_known = est_cities | fc_cities
        for row in db_rows:
            row["base"] = norm_text(base_city_name(row["city"]))
            row["city_norm"] = norm_text(row["city"])
            row["has_data"] = row["city"] in db_known

        by_exact: dict[str, dict[str, Any]] = {r["city"]: r for r in db_rows}
        by_base: dict[str, list[dict[str, Any]]] = {}
        by_bucket: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for r in db_rows:
            by_base.setdefault(r["base"], []).append(r)
            by_bucket.setdefault(self._bucket_key(r["lat"], r["lon"]), []).append(r)

        out: dict[str, str] = {}
        for c in self.catalog:
            cid = normalize_city_id(c["city"], c["country"])
            city = c["city"]
            lat = float(c.get("lat", 0.0))
            lon = float(c.get("lng", 0.0))
            cbase = norm_text(base_city_name(city))

            exact = by_exact.get(city)
            if exact and exact["has_data"]:
                out[cid] = city
                continue

            candidates: list[dict[str, Any]] = []
            bk = self._bucket_key(lat, lon)
            for dlat in (-1, 0, 1):
                for dlon in (-1, 0, 1):
                    candidates.extend(by_bucket.get((bk[0] + dlat, bk[1] + dlon), []))
            candidates.extend(by_base.get(cbase, []))

            best_row = None
            best_score = -10**9
            for r in candidates:
                if not r["has_data"]:
                    continue
                dist = abs(lat - r["lat"]) + abs(lon - r["lon"])
                score = -dist * 300.0
                if r["base"] == cbase:
                    score += 120.0
                if r["city_norm"] == norm_text(city):
                    score += 220.0
                if score > best_score:
                    best_score = score
                    best_row = r

            if best_row is not None and best_score > -45:
                out[cid] = best_row["city"]
            else:
                out[cid] = city

        return out

    def _status_from_counts(self, count: int, expected: int, last_error: Optional[str]) -> str:
        if count >= expected:
            return "green"
        if last_error:
            return "red"
        return "yellow"

    def _get_counts(self):
        with db_connect(self.db_path) as conn:
            est = {
                r["city"]: dict(r)
                for r in conn.execute(
                    """
                    SELECT city, COUNT(*) cnt, MIN(date) min_date, MAX(date) max_date, MAX(updated_at) updated_at
                    FROM daily_data_estimated
                    GROUP BY city
                    """
                ).fetchall()
            }
            fc = {
                r["city"]: dict(r)
                for r in conn.execute(
                    """
                    SELECT city, COUNT(*) cnt, MIN(date) min_date, MAX(date) max_date, MAX(updated_at) updated_at
                    FROM daily_data_forecast
                    GROUP BY city
                    """
                ).fetchall()
            }
            errs = {}
            for r in conn.execute(
                """
                SELECT city, stage, message, ts
                FROM sync_city_log
                WHERE status='error'
                ORDER BY ts DESC
                """
            ).fetchall():
                k = (r["city"], r["stage"])
                if k not in errs:
                    errs[k] = dict(r)
            coords = {r["city"]: (float(r["lat"] or 0.0), float(r["lon"] or 0.0)) for r in conn.execute("SELECT city, lat, lon FROM city_coords")}
        return est, fc, errs, coords

    def _db_city_label_meta(self, db_city: str) -> tuple[str, str]:
        if "," in db_city:
            parts = [p.strip() for p in db_city.split(",", 1)]
            if len(parts) == 2 and parts[1]:
                return parts[0], parts[1]
        return db_city, "Unknown"

    def build_tree_payload(self, scope: str = "catalog"):
        est, fc, errs, coords = self._get_counts()
        scope = (scope or "catalog").strip().lower()
        if scope not in {"catalog", "db", "all"}:
            scope = "catalog"

        continents: dict[str, dict[str, Any]] = {}
        city_index: dict[str, dict[str, Any]] = {}
        missing_rows = []

        both_complete = 0
        missing_any = 0
        failed_any = 0

        catalog_db_to_meta: dict[str, dict[str, Any]] = {}
        for c in self.catalog:
            city = c["city"]
            country = c["country"]
            cont = c["continent"]
            cid = normalize_city_id(city, country)
            db_city = self.city_to_db.get(cid, city)
            catalog_db_to_meta.setdefault(db_city, {"country": country, "continent": cont})

            if scope == "db":
                continue

            est_count = int(est.get(db_city, {}).get("cnt", 0))
            fc_count = int(fc.get(db_city, {}).get("cnt", 0))
            est_err = errs.get((db_city, "estimated")) or errs.get((city, "estimated"))
            fc_err = errs.get((db_city, "forecast")) or errs.get((city, "forecast"))

            est_status = self._status_from_counts(est_count, 365, est_err["message"] if est_err else None)
            fc_status = self._status_from_counts(fc_count, 14, fc_err["message"] if fc_err else None)
            is_complete = est_count >= 365 and fc_count >= 14

            if is_complete:
                both_complete += 1
            else:
                missing_any += 1
                missing_rows.append(
                    {
                        "id": cid,
                        "city": city,
                        "country": country,
                        "estimated_status": est_status,
                        "forecast_status": fc_status,
                        "estimated_note": f"rows={est_count}",
                        "forecast_note": f"rows={fc_count}",
                        "last_error": (est_err or fc_err or {}).get("message", ""),
                    }
                )
            if est_status == "red" or fc_status == "red":
                failed_any += 1

            cont_node = continents.setdefault(cont, {"name": cont, "countries": {}})
            ctry_node = cont_node["countries"].setdefault(country, {"name": country, "cities": []})
            city_obj = {
                "id": cid,
                "city": city,
                "country": country,
                "continent": cont,
                "lat": c["lat"],
                "lng": c["lng"],
                "db_city": db_city,
                "source_type": "catalog",
                "refresh_city": city,
                "estimated_status": est_status,
                "forecast_status": fc_status,
            }
            ctry_node["cities"].append(city_obj)
            city_index[cid] = city_obj

        if scope in {"db", "all"}:
            all_db_cities = sorted(set(est.keys()) | set(fc.keys()) | {k[0] for k in errs.keys()})
            for db_city in all_db_cities:
                if scope == "all" and db_city in catalog_db_to_meta:
                    continue
                est_count = int(est.get(db_city, {}).get("cnt", 0))
                fc_count = int(fc.get(db_city, {}).get("cnt", 0))
                est_err = errs.get((db_city, "estimated"))
                fc_err = errs.get((db_city, "forecast"))
                est_status = self._status_from_counts(est_count, 365, est_err["message"] if est_err else None)
                fc_status = self._status_from_counts(fc_count, 14, fc_err["message"] if fc_err else None)
                is_complete = est_count >= 365 and fc_count >= 14

                if is_complete:
                    both_complete += 1
                else:
                    missing_any += 1
                    city_label, inferred_country = self._db_city_label_meta(db_city)
                    country = catalog_db_to_meta.get(db_city, {}).get("country", inferred_country)
                    missing_rows.append(
                        {
                            "id": f"db::{db_city}",
                            "city": city_label,
                            "country": country,
                            "estimated_status": est_status,
                            "forecast_status": fc_status,
                            "estimated_note": f"rows={est_count}",
                            "forecast_note": f"rows={fc_count}",
                            "last_error": (est_err or fc_err or {}).get("message", ""),
                        }
                    )
                if est_status == "red" or fc_status == "red":
                    failed_any += 1

                city_label, inferred_country = self._db_city_label_meta(db_city)
                country = catalog_db_to_meta.get(db_city, {}).get("country", inferred_country)
                cont = catalog_db_to_meta.get(db_city, {}).get("continent", "DB (Unmapped)")
                lat, lon = coords.get(db_city, (0.0, 0.0))
                cid = f"db::{db_city}"
                cont_node = continents.setdefault(cont, {"name": cont, "countries": {}})
                ctry_node = cont_node["countries"].setdefault(country, {"name": country, "cities": []})
                city_obj = {
                    "id": cid,
                    "city": city_label,
                    "country": country,
                    "continent": cont,
                    "lat": lat,
                    "lng": lon,
                    "db_city": db_city,
                    "source_type": "db",
                    "refresh_city": "",
                    "estimated_status": est_status,
                    "forecast_status": fc_status,
                }
                ctry_node["cities"].append(city_obj)
                city_index[cid] = city_obj

        cont_list = []
        for cont_name in sorted(continents.keys()):
            cnode = continents[cont_name]
            countries = []
            for country_name in sorted(cnode["countries"].keys()):
                x = cnode["countries"][country_name]
                x["cities"] = sorted(x["cities"], key=lambda z: z["city"].lower())
                countries.append(x)
            cont_list.append({"name": cont_name, "countries": countries})

        missing_rows = sorted(missing_rows, key=lambda r: (r["country"], r["city"]))
        return {
            "generated_at": utcnow_iso(),
            "db_path": self.db_path,
            "scope": scope,
            "continents": cont_list,
            "city_index": city_index,
            "stats": {
                "total": len(city_index),
                "both_complete": both_complete,
                "missing_any": missing_any,
                "failed_any": failed_any,
            },
            "missing_rows": missing_rows,
        }

    def city_detail(self, city_id: str):
        if city_id.startswith("db::"):
            db_city = city_id[4:]
            city_name, country_name = self._db_city_label_meta(db_city)
            city_obj = {
                "city": city_name,
                "country": country_name,
                "continent": "DB (Unmapped)",
                "lat": 0.0,
                "lng": 0.0,
                "source_type": "db",
                "db_city": db_city,
            }
            city = city_name
        else:
            city_obj = self.catalog_by_id.get(city_id)
            if not city_obj:
                return {"error": "city not found"}
            city = city_obj["city"]
            db_city = self.city_to_db.get(city_id, city)
            city_obj = dict(city_obj)
            city_obj["source_type"] = "catalog"
            city_obj["db_city"] = db_city

        with db_connect(self.db_path) as conn:
            coords_row = conn.execute("SELECT lat, lon FROM city_coords WHERE city=? LIMIT 1", (db_city,)).fetchone()
            if coords_row:
                city_obj["lat"] = float(coords_row["lat"] or city_obj.get("lat", 0.0))
                city_obj["lng"] = float(coords_row["lon"] or city_obj.get("lng", 0.0))
            if city_obj.get("continent") == "DB (Unmapped)":
                meta = next((c for c in self.catalog if self.city_to_db.get(normalize_city_id(c["city"], c["country"]), c["city"]) == db_city), None)
                if meta:
                    city_obj["continent"] = meta["continent"]
                    city_obj["country"] = meta["country"]

            est_rows = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT date, tmax_c, tmin_c, precip_mm, solarradiation_wm2, updated_at
                    FROM daily_data_estimated
                    WHERE city=?
                    ORDER BY date DESC
                    """,
                    (db_city,),
                ).fetchall()
            ]
            fc_rows = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT date, tmax_c, tmin_c, precip_prob_pct, solarradiation_wm2, updated_at
                    FROM daily_data_forecast
                    WHERE city=?
                    ORDER BY date DESC
                    """,
                    (db_city,),
                ).fetchall()
            ]

            est_sum = conn.execute(
                "SELECT COUNT(*) cnt, MIN(date) min_date, MAX(date) max_date, MAX(updated_at) updated_at FROM daily_data_estimated WHERE city=?",
                (db_city,),
            ).fetchone()
            fc_sum = conn.execute(
                "SELECT COUNT(*) cnt, MIN(date) min_date, MAX(date) max_date, MAX(updated_at) updated_at FROM daily_data_forecast WHERE city=?",
                (db_city,),
            ).fetchone()

            est_err = conn.execute(
                "SELECT message FROM sync_city_log WHERE city=? AND stage='estimated' AND status='error' ORDER BY ts DESC LIMIT 1",
                (db_city,),
            ).fetchone()
            fc_err = conn.execute(
                "SELECT message FROM sync_city_log WHERE city=? AND stage='forecast' AND status='error' ORDER BY ts DESC LIMIT 1",
                (db_city,),
            ).fetchone()

        est_count = int(est_sum["cnt"] or 0)
        fc_count = int(fc_sum["cnt"] or 0)
        est_status = self._status_from_counts(est_count, 365, est_err["message"] if est_err else None)
        fc_status = self._status_from_counts(fc_count, 14, fc_err["message"] if fc_err else None)

        est_summary = (
            f"rows={est_count} • range={est_sum['min_date'] or '-'}..{est_sum['max_date'] or '-'}"
            f" • updated={est_sum['updated_at'] or '-'}"
        )
        fc_summary = (
            f"rows={fc_count} • range={fc_sum['min_date'] or '-'}..{fc_sum['max_date'] or '-'}"
            f" • updated={fc_sum['updated_at'] or '-'}"
        )

        latest_call = self._latest_api_call_for_city(db_city) or self._latest_api_call_for_city(city)
        latest_call_text = ""
        if latest_call:
            latest_call_text = json.dumps(latest_call, ensure_ascii=False)

        return {
            "generated_at": utcnow_iso(),
            "city": city_obj,
            "estimated": {"status": est_status, "summary": est_summary, "rows": est_rows},
            "forecast": {"status": fc_status, "summary": fc_summary, "rows": fc_rows},
            "latest_call_text": latest_call_text,
        }

    def start_refresh(self, scope: str, name: str, kind: str):
        scope = (scope or "").strip().lower()
        kind = (kind or "both").strip().lower()
        if scope not in {"all", "continent", "country", "city"}:
            return {"ok": False, "error": "invalid scope"}
        if kind not in {"both", "forecast", "estimated"}:
            return {"ok": False, "error": "invalid kind"}

        if scope == "all":
            cities = list(self.catalog)
        elif scope == "continent":
            cities = [c for c in self.catalog if c["continent"].lower() == (name or "").lower()]
        elif scope == "country":
            cities = [c for c in self.catalog if c["country"].lower() == (name or "").lower()]
        else:
            cities = [c for c in self.catalog if c["city"].lower() == (name or "").lower()]

        if not cities:
            return {"ok": False, "error": "no cities selected"}

        job_id = str(uuid.uuid4())[:8]
        job = {
            "id": job_id,
            "state": "running",
            "scope": scope,
            "name": name,
            "kind": kind,
            "total": len(cities),
            "done": 0,
            "ok": 0,
            "err": 0,
            "stage": "starting",
            "started_at": utcnow_iso(),
            "finished_at": None,
        }
        with self.jobs_lock:
            self.jobs[job_id] = job

        t = threading.Thread(target=self._run_refresh_job, args=(job_id, cities, kind), daemon=True)
        t.start()
        return {"ok": True, "job_id": job_id, "city_count": len(cities)}

    def get_job(self, job_id: str):
        with self.jobs_lock:
            return self.jobs.get(job_id)

    def _append_api_log(self, row: dict[str, Any]):
        with open(self.api_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _insert_city_log(self, conn: sqlite3.Connection, run_id: str, city: str, stage: str, status: str, message: str):
        conn.execute(
            """
            INSERT INTO sync_city_log(run_id, city, stage, status, message, ts)
            VALUES(?,?,?,?,?,?)
            """,
            (run_id, city, stage, status, message, utcnow_iso()),
        )

    def _upsert_weather_row(self, conn: sqlite3.Connection, table: str, city: str, d: dict[str, Any], source: str):
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

    def _fetch_vc(self, lat: float, lon: float, start: str, end: str, include_current: bool, key: str):
        include = ",current" if include_current else ""
        url = VC_URL.format(
            lat=lat,
            lon=lon,
            start=start,
            end=end,
            include_current=include,
            key=key,
        )
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.json(), url, r.status_code

    def _run_refresh_job(self, job_id: str, cities: list[dict[str, Any]], kind: str):
        key = get_vc_key()
        if not key:
            with self.jobs_lock:
                self.jobs[job_id]["state"] = "error"
                self.jobs[job_id]["stage"] = "missing VISUAL_CROSSING_API_KEY"
                self.jobs[job_id]["finished_at"] = utcnow_iso()
            return

        conn = db_connect(self.db_path)
        today = date.today()
        est_start = today.isoformat()
        est_end = (today + timedelta(days=364)).isoformat()
        fc_start = today.isoformat()
        fc_end = (today + timedelta(days=15)).isoformat()

        try:
            for c in cities:
                city = c["city"]
                lat = c["lat"]
                lon = c["lng"]
                with self.jobs_lock:
                    self.jobs[job_id]["stage"] = f"{city}"

                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO city_coords(city, lat, lon) VALUES(?,?,?)",
                        (city, lat, lon),
                    )

                    if kind in {"both", "estimated"}:
                        payload, url, code = self._fetch_vc(lat, lon, est_start, est_end, include_current=False, key=key)
                        days = payload.get("days", []) or []
                        for d in days:
                            self._upsert_weather_row(conn, "daily_data_estimated", city, d, "estimated")
                            # materialized best table
                            self._upsert_weather_row(conn, "daily_data", city, d, "estimated")
                        self._insert_city_log(conn, job_id, city, "estimated", "updated", f"rows={len(days)}")
                        self._append_api_log(
                            {
                                "city": city,
                                "kind": "estimated_window",
                                "provider": "visualcrossing",
                                "status_code": code,
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
                            }
                        )

                    if kind in {"both", "forecast"}:
                        payload, url, code = self._fetch_vc(lat, lon, fc_start, fc_end, include_current=True, key=key)
                        days = payload.get("days", []) or []
                        for d in days:
                            self._upsert_weather_row(conn, "daily_data_forecast", city, d, "forecast")
                            self._upsert_weather_row(conn, "daily_data", city, d, "forecast")
                        cur = payload.get("currentConditions", {}) or {}
                        self._insert_city_log(conn, job_id, city, "forecast", "updated", f"rows={len(days)}")
                        self._append_api_log(
                            {
                                "city": city,
                                "kind": "forecast_bundle",
                                "provider": "visualcrossing",
                                "status_code": code,
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
                            }
                        )

                    conn.commit()
                    with self.jobs_lock:
                        self.jobs[job_id]["ok"] += 1

                except Exception as e:
                    conn.rollback()
                    msg = str(e)
                    if kind in {"both", "estimated"}:
                        self._insert_city_log(conn, job_id, city, "estimated", "error", msg)
                    if kind in {"both", "forecast"}:
                        self._insert_city_log(conn, job_id, city, "forecast", "error", msg)
                    conn.commit()
                    with self.jobs_lock:
                        self.jobs[job_id]["err"] += 1

                with self.jobs_lock:
                    self.jobs[job_id]["done"] += 1

            with self.jobs_lock:
                self.jobs[job_id]["state"] = "done"
                self.jobs[job_id]["stage"] = "complete"
                self.jobs[job_id]["finished_at"] = utcnow_iso()
        finally:
            conn.close()


class Handler(BaseHTTPRequestHandler):
    app_state = None

    def _send_json(self, obj: dict[str, Any], status: int = 200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, content: str):
        data = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = {}
        if parsed.query:
            for part in parsed.query.split("&"):
                if not part:
                    continue
                if "=" in part:
                    k, v = part.split("=", 1)
                else:
                    k, v = part, ""
                qs[k] = unquote_plus(v)

        app = self.app_state
        if app is None:
            return self._send_json({"error": "app not initialized"}, 500)

        if path == "/":
            return self._send_html(HTML)
        if path == "/api/tree":
            return self._send_json(app.build_tree_payload(qs.get("scope", "catalog")))
        if path == "/api/city":
            city_id = qs.get("id", "")
            return self._send_json(app.city_detail(city_id))
        if path == "/api/job":
            jid = qs.get("id", "")
            return self._send_json({"job": app.get_job(jid)})

        return self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        app = self.app_state
        if app is None:
            return self._send_json({"error": "app not initialized"}, 500)

        if parsed.path == "/api/refresh":
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception:
                return self._send_json({"ok": False, "error": "invalid json"}, 400)
            res = app.start_refresh(payload.get("scope", ""), payload.get("name", ""), payload.get("kind", "both"))
            return self._send_json(res, 200 if res.get("ok") else 400)

        return self._send_json({"error": "not found"}, 404)


def main():
    parser = argparse.ArgumentParser(description="City weather management dashboard")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--catalog", default=CATALOG_DEFAULT)
    parser.add_argument("--api-log", default=API_LOG_PATH)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8791)
    args = parser.parse_args()

    app_state = AppState(args.db, args.catalog, args.api_log)
    Handler.app_state = app_state

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"City Weather Manager: http://{args.host}:{args.port}")
    print(f"DB: {args.db}")
    print(f"Catalog: {args.catalog}")
    server.serve_forever()


if __name__ == "__main__":
    main()
