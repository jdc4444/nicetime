#!/usr/bin/env python3
import argparse
import html
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def format_value(value):
    if value is None:
        return "<span class='null'>NULL</span>"
    text = str(value)
    if len(text) > 200:
        text = text[:200] + "..."
    return html.escape(text)


def html_table(columns, rows):
    if not columns:
        return "<p>No columns.</p>"
    head = "".join(f"<th>{html.escape(c)}</th>" for c in columns)
    body_rows = []
    for row in rows:
        tds = "".join(f"<td>{format_value(v)}</td>" for v in row)
        body_rows.append(f"<tr>{tds}</tr>")
    body = "".join(body_rows) if body_rows else "<tr><td colspan='999'>No rows</td></tr>"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def get_tables(conn):
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [r[0] for r in cur.fetchall()]


def page(title, body):
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f4f6f8;
      --panel: #ffffff;
      --text: #1a2433;
      --muted: #5f6d81;
      --line: #d8dee8;
      --accent: #145da0;
      --accent-2: #0c7c59;
      --danger: #b42318;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Menlo, Monaco, "SFMono-Regular", Consolas, monospace;
      background: linear-gradient(180deg, #eaf2fb 0%, var(--bg) 220px);
      color: var(--text);
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 16px;
      margin-bottom: 16px;
      box-shadow: 0 4px 18px rgba(0, 0, 0, 0.04);
    }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h1 {{ font-size: 24px; }}
    h2 {{ font-size: 18px; }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .row {{ display: flex; gap: 12px; flex-wrap: wrap; }}
    .pill {{
      background: #edf4ff;
      border: 1px solid #c6d8f3;
      border-radius: 999px;
      padding: 5px 10px;
      color: #12395c;
      font-size: 13px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      margin-top: 10px;
    }}
    th, td {{
      border: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      padding: 8px;
    }}
    th {{
      background: #f1f5f9;
      position: sticky;
      top: 0;
    }}
    .muted {{ color: var(--muted); }}
    .null {{ color: var(--muted); font-style: italic; }}
    .err {{
      color: var(--danger);
      background: #fee4e2;
      border: 1px solid #fecdca;
      padding: 8px;
      border-radius: 8px;
    }}
    input, textarea, button {{
      font: inherit;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      width: 100%;
    }}
    textarea {{ min-height: 120px; }}
    button {{
      width: auto;
      background: var(--accent-2);
      color: #fff;
      border: 1px solid #0a6649;
      cursor: pointer;
    }}
    .nav a {{ margin-right: 12px; }}
  </style>
</head>
<body>
  <div class="wrap">
    {body}
  </div>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    db_path = ""

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _send_html(self, content, status=200):
        data = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        try:
            if path == "/":
                return self.home()
            if path == "/table":
                return self.table_view(params)
            if path == "/query":
                return self.query_view(params)
            return self._send_html(page("Not Found", "<h1>404</h1><p>Not found.</p>"), status=404)
        except Exception as exc:
            msg = f"<h1>Dashboard Error</h1><div class='err'>{html.escape(str(exc))}</div>"
            return self._send_html(page("Error", msg), status=500)

    def home(self):
        db_size = os.path.getsize(self.db_path)
        db_size_mb = f"{db_size / (1024 * 1024):,.1f} MB"
        with self._connect() as conn:
            tables = get_tables(conn)
            counts = []
            for t in tables:
                c = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(t)}").fetchone()[0]
                counts.append((t, c))
            table_rows = "".join(
                f"<tr><td><a href='/table?name={html.escape(t)}'>{html.escape(t)}</a></td><td>{c:,}</td></tr>"
                for t, c in counts
            )
        body = f"""
<div class='card'>
  <h1>SQLite Dashboard</h1>
  <div class='row'>
    <span class='pill'>DB: {html.escape(self.db_path)}</span>
    <span class='pill'>Size: {db_size_mb}</span>
  </div>
</div>
<div class='card nav'>
  <a href='/'>Home</a>
  <a href='/query'>Query</a>
</div>
<div class='card'>
  <h2>Tables</h2>
  <table>
    <thead><tr><th>Table</th><th>Rows</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
</div>
"""
        return self._send_html(page("SQLite Dashboard", body))

    def table_view(self, params):
        table = params.get("name", [""])[0]
        limit = min(max(int(params.get("limit", ["100"])[0]), 1), 1000)
        offset = max(int(params.get("offset", ["0"])[0]), 0)

        with self._connect() as conn:
            tables = set(get_tables(conn))
            if table not in tables:
                return self._send_html(page("Bad table", "<p class='err'>Unknown table.</p>"), status=400)

            col_rows = conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
            schema = "".join(
                f"<tr><td>{r['cid']}</td><td>{html.escape(r['name'])}</td><td>{html.escape(r['type'] or '')}</td>"
                f"<td>{r['notnull']}</td><td>{'PK' if r['pk'] else ''}</td></tr>"
                for r in col_rows
            )

            total = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0]
            rows = conn.execute(
                f"SELECT * FROM {quote_ident(table)} LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            columns = [d[0] for d in conn.execute(f"SELECT * FROM {quote_ident(table)} LIMIT 1").description]

        prev_offset = max(offset - limit, 0)
        next_offset = offset + limit
        table_html = html_table(columns, [tuple(r) for r in rows])
        body = f"""
<div class='card nav'>
  <a href='/'>Home</a>
  <a href='/query'>Query</a>
</div>
<div class='card'>
  <h1>Table: {html.escape(table)}</h1>
  <p class='muted'>Rows: {total:,} | Showing {offset:,} - {min(offset + limit, total):,} | Page size: {limit}</p>
  <p>
    <a href='/table?name={html.escape(table)}&limit={limit}&offset={prev_offset}'>Prev</a> |
    <a href='/table?name={html.escape(table)}&limit={limit}&offset={next_offset}'>Next</a>
  </p>
</div>
<div class='card'>
  <h2>Schema</h2>
  <table>
    <thead><tr><th>#</th><th>Name</th><th>Type</th><th>Not Null</th><th>Key</th></tr></thead>
    <tbody>{schema}</tbody>
  </table>
</div>
<div class='card'>
  <h2>Data</h2>
  {table_html}
</div>
"""
        return self._send_html(page(f"Table {table}", body))

    def query_view(self, params):
        sql = params.get("sql", ["SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 50"])[0]
        limit = min(max(int(params.get("limit", ["200"])[0]), 1), 2000)
        err = ""
        result_html = ""
        row_count = 0

        if sql.strip():
            lowered = sql.strip().lower()
            if not (lowered.startswith("select") or lowered.startswith("with") or lowered.startswith("pragma")):
                err = "Only read-only SELECT/WITH/PRAGMA queries are allowed."
            elif ";" in sql.strip().rstrip(";"):
                err = "Only one query at a time is allowed."
            else:
                try:
                    with self._connect() as conn:
                        cur = conn.execute(sql)
                        cols = [d[0] for d in cur.description] if cur.description else []
                        rows = cur.fetchmany(limit)
                        row_count = len(rows)
                        result_html = html_table(cols, rows)
                except Exception as exc:
                    err = str(exc)

        err_html = f"<div class='err'>{html.escape(err)}</div>" if err else ""
        body = f"""
<div class='card nav'>
  <a href='/'>Home</a>
  <a href='/query'>Query</a>
</div>
<div class='card'>
  <h1>SQL Query</h1>
  <p class='muted'>Read-only mode. Max rows returned: {limit}</p>
  <form method='GET' action='/query'>
    <label for='limit'>Row limit</label>
    <input id='limit' name='limit' value='{limit}' />
    <label for='sql'>SQL</label>
    <textarea id='sql' name='sql'>{html.escape(sql)}</textarea>
    <p><button type='submit'>Run Query</button></p>
  </form>
  {err_html}
</div>
<div class='card'>
  <h2>Result ({row_count} rows)</h2>
  {result_html}
</div>
"""
        return self._send_html(page("Query", body))

    def log_message(self, fmt, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Simple SQLite dashboard")
    parser.add_argument("--db", default="weather_data.db", help="Path to SQLite database")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"Database not found: {db_path}")

    DashboardHandler.db_path = db_path
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard running at http://{args.host}:{args.port}")
    print(f"Database: {db_path}")
    server.serve_forever()


if __name__ == "__main__":
    main()
