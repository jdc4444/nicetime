#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

API_LOG_PATH = "/Users/jos/Desktop/Archive/sync_api_calls.ndjson"

HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Sunseeker Sync Monitor</title>
  <style>
    :root {
      --bg: #f3f5f7;
      --card: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --line: #d1d5db;
      --good: #0f766e;
      --warn: #b45309;
      --bad: #b91c1c;
      --accent: #2563eb;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #e6eef9 0%, var(--bg) 240px);
    }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 18px; }
    .header {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      margin-bottom: 12px;
    }
    .title { font-size: 26px; font-weight: 800; }
    .sub { color: var(--muted); margin-top: 4px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 560px) {
      .grid { grid-template-columns: 1fr; }
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
    }
    .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 26px; font-weight: 800; margin-top: 2px; }
    .value.good { color: var(--good); }
    .value.warn { color: var(--warn); }
    .value.bad { color: var(--bad); }
    .progress {
      margin-top: 8px;
      width: 100%;
      background: #eef2f7;
      border: 1px solid var(--line);
      border-radius: 999px;
      height: 14px;
      overflow: hidden;
    }
    .bar {
      background: linear-gradient(90deg, #0ea5e9, #2563eb);
      height: 100%;
      width: 0%;
      transition: width .25s ease;
    }
    .section {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      margin-bottom: 12px;
    }
    .section h3 { margin: 0 0 10px; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      text-align: left;
      padding: 8px 6px;
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 700; }
    tr:last-child td { border-bottom: none; }
    .pill {
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 9px;
      font-size: 12px;
      background: #f8fafc;
    }
    .pill.ok { border-color: #99f6e4; background: #ccfbf1; color: #115e59; }
    .pill.updated { border-color: #93c5fd; background: #dbeafe; color: #1d4ed8; }
    .pill.error { border-color: #fecaca; background: #fee2e2; color: #991b1b; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .toolbar {
      display: flex;
      gap: 8px;
      align-items: center;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }
    .toolbar input {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 7px 10px;
      min-width: 260px;
      background: #fff;
      color: var(--ink);
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div class="title">Sunseeker Sync Monitor</div>
      <div class="sub" id="meta">Connecting...</div>
      <div class="progress"><div class="bar" id="bar"></div></div>
    </div>

    <div class="grid">
      <div class="card"><div class="label">Target Cities</div><div class="value" id="target">-</div></div>
      <div class="card"><div class="label">Historical Complete</div><div class="value good" id="hist_complete">-</div></div>
      <div class="card"><div class="label">Historical Missing</div><div class="value warn" id="hist_missing">-</div></div>
      <div class="card"><div class="label">Forecast Fresh / Stale</div><div class="value" id="forecast">-</div></div>
      <div class="card"><div class="label">Historical Updated (Run)</div><div class="value" id="hist_updated">-</div></div>
      <div class="card"><div class="label">Forecast Updated (Run)</div><div class="value" id="forecast_updated">-</div></div>
      <div class="card"><div class="label">Errors (Run)</div><div class="value bad" id="errors">-</div></div>
      <div class="card"><div class="label">Rows in daily_data</div><div class="value" id="rows">-</div></div>
    </div>

    <div class="section">
      <h3>Run Status</h3>
      <table>
        <tbody>
          <tr><th>Run ID</th><td class="mono" id="run_id">-</td></tr>
          <tr><th>Status</th><td id="status">-</td></tr>
          <tr><th>Started</th><td id="started">-</td></tr>
          <tr><th>Finished</th><td id="finished">-</td></tr>
          <tr><th>Notes</th><td id="notes">-</td></tr>
        </tbody>
      </table>
    </div>

    <div class="section">
      <h3>Recent City Events</h3>
      <table>
        <thead>
          <tr>
            <th>Time (UTC)</th>
            <th>City</th>
            <th>Stage</th>
            <th>Status</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody id="events"></tbody>
      </table>
    </div>

    <div class="section">
      <h3>Recent API Calls (Actual Data)</h3>
      <table>
        <thead>
          <tr>
            <th>Time (UTC)</th>
            <th>City</th>
            <th>Kind</th>
            <th>Status</th>
            <th>Records</th>
            <th>Sample</th>
            <th>URL</th>
          </tr>
        </thead>
        <tbody id="calls"></tbody>
      </table>
    </div>

    <div class="section">
      <h3>Latest Values by City</h3>
      <div class="toolbar">
        <input id="cityFilter" placeholder="Filter city (e.g. Mexico City)" />
      </div>
      <table>
        <thead>
          <tr>
            <th>City</th>
            <th>Kind</th>
            <th>Status</th>
            <th>Records</th>
            <th>Current C</th>
            <th>Tmax C</th>
            <th>Tmin C</th>
            <th>Precip mm</th>
            <th>Precip %</th>
            <th>Solar W/m2</th>
            <th>Window</th>
            <th>Time (UTC)</th>
          </tr>
        </thead>
        <tbody id="cityValues"></tbody>
      </table>
    </div>
  </div>
  <script>
    const REFRESH_MS = 2000;

    function fmt(n) {
      if (n === null || n === undefined) return "-";
      return String(n);
    }

    function statusPill(s) {
      const cls = s === "error" ? "error" : (s === "updated" ? "updated" : "ok");
      return `<span class="pill ${cls}">${s || "-"}</span>`;
    }

    function renderCityValues(rows) {
      const q = (document.getElementById("cityFilter").value || "").toLowerCase().trim();
      const filtered = q ? rows.filter(r => String(r.city || "").toLowerCase().includes(q)) : rows;
      const html = filtered.map(r => {
        const win = (r.start_date && r.end_date) ? `${r.start_date}..${r.end_date}` : "";
        return `<tr>
          <td>${r.city || ""}</td>
          <td>${r.kind || ""}</td>
          <td>${statusPill(r.ok === false ? "error" : "ok")}</td>
          <td>${r.records ?? ""}</td>
          <td>${r.current_temp_c ?? ""}</td>
          <td>${r.sample_tmax_c ?? ""}</td>
          <td>${r.sample_tmin_c ?? ""}</td>
          <td>${r.sample_precip_mm ?? ""}</td>
          <td>${r.sample_precip_prob_pct ?? ""}</td>
          <td>${r.sample_solarradiation_wm2 ?? ""}</td>
          <td class="mono">${win}</td>
          <td class="mono">${r.ts || ""}</td>
        </tr>`;
      }).join("");
      document.getElementById("cityValues").innerHTML =
        html || "<tr><td colspan='12'>No city value rows yet.</td></tr>";
    }

    async function refresh() {
      try {
        const sRes = await fetch("/api/summary");
        const s = await sRes.json();
        const run = s.run || {};
        const stats = s.stats || {};
        const db = s.db || {};

        document.getElementById("meta").textContent =
          `${db.path || "-"} • refreshed ${s.generated_at || "-"}`;
        document.getElementById("target").textContent = fmt(stats.target_cities);
        document.getElementById("hist_complete").textContent = fmt(stats.historical_complete);
        document.getElementById("hist_missing").textContent = fmt(stats.historical_missing);
        document.getElementById("forecast").textContent =
          `${fmt(stats.forecast_fresh)} / ${fmt(stats.forecast_stale)}`;
        document.getElementById("hist_updated").textContent = fmt(run.historical_updated);
        document.getElementById("forecast_updated").textContent = fmt(run.forecast_updated);
        document.getElementById("errors").textContent = fmt(run.errors);
        document.getElementById("rows").textContent = fmt(stats.daily_rows);

        document.getElementById("run_id").textContent = run.run_id || "-";
        document.getElementById("status").innerHTML = statusPill(run.status || "-");
        document.getElementById("started").textContent = run.started_at || "-";
        document.getElementById("finished").textContent = run.finished_at || "-";
        document.getElementById("notes").textContent = run.notes || "-";

        const total = Number(stats.target_cities || 0);
        const done = Number(stats.historical_complete || 0);
        const pct = total > 0 ? Math.max(0, Math.min(100, (done / total) * 100)) : 0;
        document.getElementById("bar").style.width = pct.toFixed(1) + "%";

        const eRes = await fetch("/api/events?limit=120");
        const ev = await eRes.json();
        const rows = (ev.events || []).map(r => {
          return `<tr>
            <td class="mono">${r.ts || ""}</td>
            <td>${r.city || ""}</td>
            <td>${r.stage || ""}</td>
            <td>${statusPill(r.status)}</td>
            <td class="mono">${r.message || ""}</td>
          </tr>`;
        }).join("");
        document.getElementById("events").innerHTML = rows || "<tr><td colspan='5'>No events yet.</td></tr>";

        const cRes = await fetch("/api/calls?limit=120");
        const calls = await cRes.json();
        const cRows = (calls.calls || []).map(r => {
          const sample = [];
          if (r.current_temp_c !== undefined && r.current_temp_c !== null) sample.push(`cur ${Number(r.current_temp_c).toFixed(1)}C`);
          if (r.sample_tmax_c !== undefined && r.sample_tmax_c !== null) sample.push(`max ${Number(r.sample_tmax_c).toFixed(1)}C`);
          if (r.sample_tmin_c !== undefined && r.sample_tmin_c !== null) sample.push(`min ${Number(r.sample_tmin_c).toFixed(1)}C`);
          if (r.sample_precip_mm !== undefined && r.sample_precip_mm !== null) sample.push(`precip ${r.sample_precip_mm}mm`);
          if (r.sample_precip_prob_pct !== undefined && r.sample_precip_prob_pct !== null) sample.push(`precip% ${r.sample_precip_prob_pct}`);
          if (r.sample_solarradiation_wm2 !== undefined && r.sample_solarradiation_wm2 !== null) sample.push(`solar ${r.sample_solarradiation_wm2}W/m2`);
          if (r.start_date && r.end_date) sample.push(`${r.start_date}..${r.end_date}`);
          const s = sample.join(" · ");
          return `<tr>
            <td class="mono">${r.ts || ""}</td>
            <td>${r.city || ""}</td>
            <td>${r.kind || ""}</td>
            <td>${statusPill(r.ok === false ? "error" : "ok")}</td>
            <td>${r.records ?? ""}</td>
            <td class="mono">${s}</td>
            <td class="mono">${r.url || ""}</td>
          </tr>`;
        }).join("");
        document.getElementById("calls").innerHTML = cRows || "<tr><td colspan='7'>No API calls logged yet.</td></tr>";

        const cvRes = await fetch("/api/city-values?limit=5000");
        const cv = await cvRes.json();
        window.__cityValuesRows = cv.rows || [];
        renderCityValues(window.__cityValuesRows);
      } catch (e) {
        document.getElementById("meta").textContent = "Dashboard error: " + e;
      }
    }

    document.getElementById("cityFilter").addEventListener("input", () => {
      renderCityValues(window.__cityValuesRows || []);
    });

    refresh();
    setInterval(refresh, REFRESH_MS);
  </script>
</body>
</html>
"""


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


class Handler(BaseHTTPRequestHandler):
    db_path = ""
    api_log_path = API_LOG_PATH

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def _send_json(self, obj, status=200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, content, status=200):
        data = content.encode("utf-8")
        self.send_response(status)
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
        qs = parse_qs(parsed.query)

        if path == "/":
            return self._send_html(HTML)
        if path == "/api/summary":
            return self._send_json(self._summary())
        if path == "/api/events":
            limit = 120
            try:
                limit = max(1, min(500, int(qs.get("limit", ["120"])[0])))
            except Exception:
                pass
            return self._send_json(self._events(limit))
        if path == "/api/calls":
            limit = 120
            try:
                limit = max(1, min(500, int(qs.get("limit", ["120"])[0])))
            except Exception:
                pass
            return self._send_json(self._calls(limit))
        if path == "/api/city-values":
            limit = 5000
            try:
                limit = max(1, min(50000, int(qs.get("limit", ["5000"])[0])))
            except Exception:
                pass
            return self._send_json(self._city_values(limit))
        return self._send_json({"error": "not found"}, status=404)

    def _summary(self):
        generated_at = datetime.now(timezone.utc).isoformat()
        if not os.path.exists(self.db_path):
            return {
                "generated_at": generated_at,
                "db": {"path": self.db_path, "exists": False},
                "run": {},
                "stats": {},
            }
        with self._connect() as conn:
            run = {}
            if table_exists(conn, "sync_runs"):
                row = conn.execute(
                    "SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                if row:
                    run = dict(row)

            daily_rows = 0
            daily_cities = 0
            forecast_rows = 0
            if table_exists(conn, "daily_data"):
                daily_rows = conn.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0]
                daily_cities = conn.execute("SELECT COUNT(DISTINCT city) FROM daily_data").fetchone()[0]
                has_source = any(
                    r["name"] == "data_source"
                    for r in conn.execute("PRAGMA table_info(daily_data)").fetchall()
                )
                if has_source:
                    forecast_rows = conn.execute(
                        "SELECT COUNT(*) FROM daily_data WHERE data_source='forecast'"
                    ).fetchone()[0]

            target = run.get("total_cities", 0) if run else 0
            hist_complete = run.get("historical_complete", 0) if run else 0
            hist_missing = run.get("historical_missing", 0) if run else 0
            forecast_fresh = run.get("forecast_fresh", 0) if run else 0
            forecast_stale = run.get("forecast_stale", 0) if run else 0

            if target == 0 and daily_cities:
                target = daily_cities
            if hist_complete == 0 and daily_cities:
                hist_complete = daily_cities
            if target and hist_missing == 0 and hist_complete <= target:
                hist_missing = max(0, target - hist_complete)

            return {
                "generated_at": generated_at,
                "db": {
                    "path": self.db_path,
                    "exists": True,
                    "size_bytes": os.path.getsize(self.db_path),
                },
                "run": run,
                "stats": {
                    "target_cities": target,
                    "historical_complete": hist_complete,
                    "historical_missing": hist_missing,
                    "forecast_fresh": forecast_fresh,
                    "forecast_stale": forecast_stale,
                    "daily_rows": daily_rows,
                    "daily_cities": daily_cities,
                    "forecast_rows": forecast_rows,
                },
            }

    def _events(self, limit: int):
        generated_at = datetime.now(timezone.utc).isoformat()
        if not os.path.exists(self.db_path):
            return {"generated_at": generated_at, "events": []}
        with self._connect() as conn:
            if not table_exists(conn, "sync_city_log"):
                return {"generated_at": generated_at, "events": []}
            run_id = None
            if table_exists(conn, "sync_runs"):
                row = conn.execute(
                    "SELECT run_id FROM sync_runs ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                if row:
                    run_id = row["run_id"]
            if run_id:
                rows = conn.execute(
                    """
                    SELECT ts, city, stage, status, message
                    FROM sync_city_log
                    WHERE run_id=?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (run_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT ts, city, stage, status, message
                    FROM sync_city_log
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            events = [dict(r) for r in rows]
        return {"generated_at": generated_at, "events": events}

    def _calls(self, limit: int):
        generated_at = datetime.now(timezone.utc).isoformat()
        if not os.path.exists(self.api_log_path):
            return {"generated_at": generated_at, "calls": []}
        out = []
        try:
            with open(self.api_log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()[-limit:]
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
        except Exception:
            out = []
        return {"generated_at": generated_at, "calls": out}

    def _city_values(self, limit: int):
        generated_at = datetime.now(timezone.utc).isoformat()
        if not os.path.exists(self.api_log_path):
            return {"generated_at": generated_at, "rows": []}
        latest_by_city = {}
        try:
            with open(self.api_log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    city = row.get("city")
                    if not city:
                        continue
                    if (
                        row.get("current_temp_c") is None
                        and row.get("sample_tmax_c") is None
                        and row.get("sample_tmin_c") is None
                        and row.get("records") in (None, 0)
                    ):
                        continue
                    latest_by_city[city] = row
        except Exception:
            return {"generated_at": generated_at, "rows": []}

        rows = sorted(
            latest_by_city.values(),
            key=lambda r: (str(r.get("city", "")).lower(), str(r.get("ts", ""))),
        )
        if len(rows) > limit:
            rows = rows[:limit]
        return {"generated_at": generated_at, "rows": rows}


def main():
    parser = argparse.ArgumentParser(description="Live monitor for sunseeker sync progress.")
    parser.add_argument("--db", default="/Users/jos/Desktop/Archive/weather_data_v2.db", help="SQLite DB path")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8787, help="Bind port")
    parser.add_argument("--api-log", default=API_LOG_PATH, help="Path to API NDJSON log")
    args = parser.parse_args()

    Handler.db_path = args.db
    Handler.api_log_path = args.api_log
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Dashboard: http://{args.host}:{args.port}")
    print(f"Watching DB: {args.db}")
    print(f"Watching API log: {args.api_log}")
    server.serve_forever()


if __name__ == "__main__":
    main()
