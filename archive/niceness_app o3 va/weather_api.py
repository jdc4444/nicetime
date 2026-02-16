import requests

CITY_COORDS = {
    "Honolulu":        (21.3069, -157.8583),
    "Los Angeles":     (34.0522, -118.2437),
    "New York":        (40.7128, -74.0060),
    "London":          (51.5074, -0.1278),
    "Tenerife":        (28.2916, -16.6291),
    "Milos":           (36.7260, 24.4443),
}

def fetch_current_weather(lat, lon):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}&current_weather=true&temperature_unit=fahrenheit"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data["current_weather"]

def get_all_cities():
    results = []
    for city, (lat, lon) in CITY_COORDS.items():
        cw = fetch_current_weather(lat, lon)
        results.append({
            "city": city,
            "temp_f": cw["temperature"],
            "weathercode": cw["weathercode"],
        })
    return results
