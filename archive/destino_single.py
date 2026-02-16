#!/usr/bin/env python3
"""
Destino — single‑file *rich* demo (PDF parity)
=============================================
This script now mirrors **all key controls in Destino.pdf**:

* Months selector (Jan‑Dec chips)
* Start city + # stops
* Budget range (min/max)
* Ideal temp (°F range slider)
* Languages (Eng/Sp/Fr/It/De)
* Incoming transport (Plane/Road/Boat/Train)
* Local transport (Transit/Ride‑share/Bike/Walk)
* Vibes (Seaside | Urban | Old‑world | Nightlife | Nature)
* Interests (Art | Sport | Food | History | Surf | Ski)
* Continents (NA/SA/EU/AS/AF/OCE)

Backend ships with **9 dummy destinations** across continents and
scores them with a *very* simple rule‑of‑thumb so something different
always shows up based on your inputs.

Quick start
-----------
```bash
python -m venv venv && source venv/bin/activate  # optional
pip install flask flask_cors

# run (tries 5000‑5019 by default):
python destino_single.py
# or explicit
python destino_single.py --port 8000
```
Browse to the printed URL → fill the wizard chips → hit **Show me places**.
You’ll land on a 3‑D globe with up to 3 recommended spots and sidebar cards.

No Node/Vite — just Flask + vanilla JS + CDN Globe.gl.
"""
from __future__ import annotations
import argparse, json, textwrap, socket, sys, random, webbrowser, threading
from typing import Dict, Any, List

from flask import Flask, request, jsonify
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Port helper ----------------------------------------------------------------
# ---------------------------------------------------------------------------
MAX_ATTEMPTS = 20  # start + 19

def first_free_port(start: int) -> int:
    for p in range(start, start + MAX_ATTEMPTS):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    raise RuntimeError("No free port in range")

# ---------------------------------------------------------------------------
# Dummy catalog ----------------------------------------------------------------
# ---------------------------------------------------------------------------
CATALOG: List[Dict[str, Any]] = [
    {
        "id": 1,
        "name": "Tenerife, Spain",
        "continent": "EU",
        "coords": [28.2916, -16.6291],
        "vibes": {"Seaside", "Nature", "Nightlife"},
        "interests": {"Surf", "Food", "Nature"},
        "languages": {"Spanish"},
        "temp_f": (60, 82),
    },
    {
        "id": 2,
        "name": "Honolulu, USA",
        "continent": "NA",
        "coords": [21.3069, -157.8583],
        "vibes": {"Seaside", "Nightlife", "Nature"},
        "interests": {"Surf", "Food", "Art"},
        "languages": {"English"},
        "temp_f": (70, 88),
    },
    {
        "id": 3,
        "name": "Reykjavík, Iceland",
        "continent": "EU",
        "coords": [64.1466, -21.9426],
        "vibes": {"Urban", "Nightlife", "Nature"},
        "interests": {"History", "Art", "Ski"},
        "languages": {"English"},
        "temp_f": (30, 60),
    },
    {
        "id": 4,
        "name": "Kyoto, Japan",
        "continent": "AS",
        "coords": [35.0116, 135.7681],
        "vibes": {"Old‑world", "Urban", "Nature"},
        "interests": {"History", "Art", "Food"},
        "languages": {"Japanese"},
        "temp_f": (40, 90),
    },
    {
        "id": 5,
        "name": "Cape Town, South Africa",
        "continent": "AF",
        "coords": [-33.9249, 18.4241],
        "vibes": {"Seaside", "Nature", "Nightlife"},
        "interests": {"Surf", "Food", "History"},
        "languages": {"English"},
        "temp_f": (50, 80),
    },
    {
        "id": 6,
        "name": "Queenstown, New Zealand",
        "continent": "OCE",
        "coords": [-45.0312, 168.6626],
        "vibes": {"Nature", "Nightlife"},
        "interests": {"Ski", "Sport", "Food"},
        "languages": {"English"},
        "temp_f": (25, 70),
    },
    {
        "id": 7,
        "name": "Buenos Aires, Argentina",
        "continent": "SA",
        "coords": [-34.6037, -58.3816],
        "vibes": {"Urban", "Nightlife", "Old‑world"},
        "interests": {"Art", "Sport", "Food"},
        "languages": {"Spanish"},
        "temp_f": (45, 88),
    },
    {
        "id": 8,
        "name": "Vancouver, Canada",
        "continent": "NA",
        "coords": [49.2827, -123.1207],
        "vibes": {"Urban", "Nature"},
        "interests": {"Ski", "Art", "Food"},
        "languages": {"English", "French"},
        "temp_f": (30, 75),
    },
    {
        "id": 9,
        "name": "Lisbon, Portugal",
        "continent": "EU",
        "coords": [38.7223, -9.1393],
        "vibes": {"Seaside", "Old‑world", "Nightlife"},
        "interests": {"History", "Art", "Food"},
        "languages": {"Portuguese"},
        "temp_f": (50, 85),
    },
]

# ---------------------------------------------------------------------------
# Flask setup ----------------------------------------------------------------
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)


@app.post("/prefs")
def receive_prefs():
    prefs = request.get_json(silent=True) or {}
    recs = recommend(prefs)
    return jsonify({"recommendations": recs[:3]})


@app.get("/catalog")
def catalog():
    return jsonify(CATALOG)


# ---------------------------------------------------------------------------
# Very naïve recommender ------------------------------------------------------
# ---------------------------------------------------------------------------

def recommend(prefs: Dict[str, Any]):
    choices = CATALOG.copy()

    # filter by continents
    conts = set(prefs.get("continents", []))
    if conts:
        choices = [d for d in choices if d["continent"] in conts]

    # ideal temp range overlap
    temp_min = int(prefs.get("temp_min", -100))
    temp_max = int(prefs.get("temp_max", 999))
    choices = [d for d in choices if not (d["temp_f"][1] < temp_min or d["temp_f"][0] > temp_max)]

    # vibe intersection scoring
    pref_vibes = set(prefs.get("vibes", []))
    if pref_vibes:
        choices.sort(key=lambda d: len(d["vibes"] & pref_vibes), reverse=True)

    # ensure variability
    random.shuffle(choices)
    return choices

# ---------------------------------------------------------------------------
# Inline HTML ----------------------------------------------------------------
# ---------------------------------------------------------------------------

def index_html() -> str:
    return textwrap.dedent(
        """
        <!doctype html><html lang='en'><head><meta charset='utf-8'/>
        <title>Destino Wizard</title>
        <style>
            body{font-family:sans-serif;margin:0;display:flex;justify-content:center;padding:2rem;background:#fafafa}
            form{background:#fff;padding:2rem 2.5rem;border-radius:12px;max-width:760px;width:100%;box-shadow:0 2px 8px rgba(0,0,0,.08)}
            h1{margin-top:0}
            .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:.75rem}
            .chip{padding:.4rem .8rem;border:1px solid #888;border-radius:9999px;cursor:pointer;text-align:center}
            .chip.selected{background:#ff7e1b;color:#fff;border-color:#ff7e1b}
            label{display:block;font-weight:600;margin-top:1.4rem;margin-bottom:.3rem}
            button{margin-top:1.6rem;background:#ff7e1b;color:#fff;border:none;padding:.6rem 1.4rem;font-size:1rem;border-radius:6px;cursor:pointer}
            input[type=range]{width:100%}
            input[type=number]{width:100%;padding:.4rem .6rem}
        </style></head><body>
        <form id='wizard'>
            <h1>Destino – Trip Wizard</h1>

            <label>When could you travel? (months)</label>
            <div class='grid' id='month-chips'></div>

            <label>Start city</label>
            <input name='start_city' placeholder='e.g. New York, NY' required>

            <label>Stops in between</label>
            <input type='number' name='stops' value='2' min='0' max='10'>

            <label>Budget (USD)</label>
            <div style='display:flex;gap:.5rem'>
              <input type='number' name='budget_min' value='50' min='0' step='10'>
              <input type='number' name='budget_max' value='400' min='0' step='10'>
            </div>

            <label>Ideal temperature (°F)</label>
            <input type='range' name='temp_min' id='tmin' min='0' max='100' value='60' oninput="tmin_val.textContent=this.value">
            <input type='range' name='temp_max' id='tmax' min='0' max='100' value='80' oninput="tmax_val.textContent=this.value">
            <div> <span id='tmin_val'>60</span>°F – <span id='tmax_val'>80</span>°F</div>

            <label>Languages</label>
            <div class='grid' id='lang-chips'></div>

            <label>Incoming transport</label>
            <div class='grid' id='in-trans-chips'></div>

            <label>Local transport</label>
            <div class='grid' id='local-trans-chips'></div>

            <label>Vibe</label>
            <div class='grid' id='vibe-chips'></div>

            <label>Interests</label>
            <div class='grid' id='interest-chips'></div>

            <label>Continents</label>
            <div class='grid' id='cont-chips'></div>

            <button type='submit'>Show me places</button>
        </form>
        <script>
        const chips = (el, list, single=false)=>{
            list.forEach(txt=>{
                const c=document.createElement('div');
                c.className='chip';
                c.textContent=txt;
                c.onclick=()=>{
                    if(single){el.querySelectorAll('.chip').forEach(x=>x.classList.remove('selected'));}
                    c.classList.toggle('selected');
                };
                el.appendChild(c);
            });
        };
        chips(month_chips, ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']);
        chips(lang_chips, ['English','Spanish','French','Italian','German']);
        chips(in_trans_chips, ['Plane','Road','Boat','Train']);
        chips(local_trans_chips, ['Transit','Ride‑share','Bike','Walk']);
        chips(vibe_chips, ['Seaside','Urban','Old‑world','Nightlife','Nature']);
        chips(interest_chips, ['Art','Sport','Food','History','Surf','Ski']);
        chips(cont_chips, ['NA','SA','EU','AS','AF','OCE']);

        const api=(p,o)=>fetch(p,o).then(r=>r.json());
        wizard.addEventListener('submit', async e=>{
            e.preventDefault();
            const sel = (id)=>[...id.querySelectorAll('.selected')].map(x=>x.textContent);
            const data={
              months: sel(month_chips),
              languages: sel(lang_chips),
              transport_in: sel(in_trans_chips),
              transport_local: sel(local_trans_chips),
              vibes: sel(vibe_chips),
              interests: sel(interest_chips),
              continents: sel(cont_chips),
              start_city: wizard.start_city.value,
              stops: wizard.stops.value,
              budget_min: wizard.budget_min.value,
              budget_max: wizard.budget_max.value,
              temp_min: wizard.temp_min.value,
              temp_max: wizard.temp_max.value,
            };
            const res=await api('/prefs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
            sessionStorage.setItem('destino_recs', JSON.stringify(res.recommendations));
            location.href='/view';
        });
        </script></body></html>
        """
    )


def view_html() -> str:
    return textwrap.dedent(
        """
        <!doctype html><html lang='en'><head><meta charset='utf-8'/>
        <title>Destino Results</title>
        <style>
            html,body{margin:0;height:100%;display:flex;font-family:sans-serif}
            aside{width:320px;overflow:auto;padding:1rem;border-right:1px solid #ccc;background:#fafafa}
            h2{margin:.2rem 0}
            .card{margin-bottom:1.2rem;padding-bottom:1.2rem;border-bottom:1px solid #ddd}
            a{color:#ff7e1b;text-decoration:none;font-size:.9rem}
        </style>
        <script src='https://unpkg.com/three'></script>
        <script src='https://unpkg.com/globe.gl'></script>
        </head><body>
        <aside><a href='/'>← new search</a><div id='cards'></div></aside>
        <canvas id='globe' style='flex:1'></canvas>
        <script>
        const recs=JSON.parse(sessionStorage.getItem('destino_recs')||'[]');
        const cards=document.getElementById('cards');
        recs.forEach(d=>{
           const div=document.createElement('div');div.className='card';
           div.innerHTML=`<h2>${d.name}</h2><p><em>${d.continent}</em></p>`;
           cards.appendChild(div);
        });
        const g=Globe()(document.getElementById('globe'))
           .globeImageUrl('https://unpkg.com/three-globe/example/img/earth-dark.jpg')
           .pointsData(recs)
           .pointLat(d=>d.coords[0]).pointLng(d=>d.coords[1])
           .pointColor(()=>'#ff7e1b');
        if(recs[0])g.pointOfView({lat:recs[0].coords[0],lng:recs[0].coords[1],altitude:2});
        </script></body></html>
        """
    )

# ---------------------------------------------------------------------------
# Flask routes ---------------------------------------------------------------
# ---------------------------------------------------------------------------

@app.get("/")
def _idx():
    return index_html()

@app.get("/view")
def _view():
    return view_html()

# ---------------------------------------------------------------------------
# Main -----------------------------------------------------------------------
# ---------------------------------------------------------------------------

def main(argv=None):
    argv=argv or sys.argv[1:]
    p=argparse.ArgumentParser()
    p.add_argument('--port','-p',type=int,default=5000)
    ns=p.parse_args(argv)
    port=first_free_port(ns.port)
    if port!=ns.port:
        print(f"⚠️  port {ns.port} busy → using {port}")
    url=f"http://localhost:{port}"
    print(f"Destino running → {url}  (Ctrl+C to quit)")
    threading.Timer(1.0,lambda:webbrowser.open(url)).start()
    app.run(host="127.0.0.1",port=port,use_reloader=False)


if __name__=='__main__':
    main()
