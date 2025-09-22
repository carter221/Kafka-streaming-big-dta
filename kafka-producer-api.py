import argparse
import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:9092"
TOPIC = "weather_stream"
API_URL = "https://api.open-meteo.com/v1/forecast"
GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"

def geocode_city(name, country=None):
    """
    Résout (lat, lon) via l’API de géocodage.
    Filtre sur le pays si fourni (égalité insensible à la casse).
    """
    params = {
        "name": name,
        "count": 5,
        "language": "en",
        "format": "json"
    }
    r = requests.get(GEOCODE_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    if not results:
        raise ValueError(f"Aucun résultat géocodage pour '{name}'")

    if country:
        for res in results:
            if res.get("country", "").lower() == country.lower():
                return (res["latitude"], res["longitude"], res["name"], res["country"])
        # si pas trouvé avec pays, on avertit et prend le premier
        print(f"[WARN] Pays '{country}' non trouvé parmi les résultats, utilisation du premier.")
    res0 = results[0]
    return (res0["latitude"], res0["longitude"], res0["name"], res0["country"])

def get_producer(retries=8, wait=2):
    for i in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all"
            )
        except NoBrokersAvailable:
            print(f"[WAIT] Broker non joignable ({i+1}/{retries})")
            time.sleep(wait)
    raise SystemExit("Broker inaccessible.")

def fetch_weather(lat, lon, city=None, country=None):
    url = f"{API_URL}?latitude={lat}&longitude={lon}&current_weather=true"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    cw = data.get("current_weather", {}) or {}

    iso_time = cw.get("time")
    epoch_now = int(time.time())

    return {
        "city": city,
        "country": country,
        "latitude": lat,
        "longitude": lon,
        "timestamp": iso_time,
        "timestamp_epoch": epoch_now,
        "weather": {
            "temperature": cw.get("temperature"),
            "windspeed": cw.get("windspeed"),
            "winddirection": cw.get("winddirection"),
            "weathercode": cw.get("weathercode"),
            "is_day": cw.get("is_day"),
            "time": iso_time
        },
        "location_info": {
            "timezone": data.get("timezone"),
            "timezone_abbreviation": data.get("timezone_abbreviation"),
            "elevation": data.get("elevation"),
            "city": city,
            "country": country
        }
    }

def run(lat=None, lon=None, city=None, country=None):
    # Géocodage si city fourni (prioritaire)
    if city:
        try:
            g_lat, g_lon, g_city, g_country = geocode_city(city, country)
            lat, lon = g_lat, g_lon
            # on remplace potentiellement par noms normalisés
            city, country = g_city, g_country
            print(f"[GEO] {city}, {country} -> ({lat}, {lon})")
        except Exception as e:
            raise SystemExit(f"Echec géocodage: {e}")
    else:
        if lat is None or lon is None:
            raise SystemExit("Fournir --city (optionnel --country) OU bien --lat et --lon.")

    producer = get_producer()
    try:
        payload = fetch_weather(lat, lon, city, country)
        key = f"{(city or 'NA')}|{(country or 'NA')}"
        fut = producer.send(TOPIC, key=key, value=payload)
        fut.get(timeout=5)
        print(f"[OK] Message envoyé sur {TOPIC} clé={key}")
    except requests.RequestException as e:
        print(f"[ERR] API: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Producteur météo (ville/pays ou lat/lon)")
    parser.add_argument("--city", type=str, help="Nom de la ville (utilise géocodage)")
    parser.add_argument("--country", type=str, help="Pays (affine le géocodage)")
    parser.add_argument("--lat", type=float, help="Latitude (si pas de --city)")
    parser.add_argument("--lon", type=float, help="Longitude (si pas de --city)")
    args = parser.parse_args()
    run(args.lat, args.lon, args.city, args.country)