import argparse
import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:9092"
TOPIC = "weather_stream"
API_URL = "https://api.open-meteo.com/v1/forecast"

def get_producer(retries=8, wait=2):
    for i in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all"
            )
        except NoBrokersAvailable:
            print(f"[WAIT] Broker non joignable ({i+1}/{retries})")
            time.sleep(wait)
    raise SystemExit("Broker inaccessible.")

def fetch_weather(lat, lon):
    url = f"{API_URL}?latitude={lat}&longitude={lon}&current_weather=true"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    cw = data.get("current_weather", {}) or {}

    iso_time = cw.get("time")            # ex: 2025-09-22T14:15
    epoch_now = int(time.time())         # ingestion time

    return {
        "latitude": lat,
        "longitude": lon,
        "timestamp": iso_time,            # pour to_timestamp dans Spark
        "timestamp_epoch": epoch_now,     # optionnel (diagnostic / latence)
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
            "elevation": data.get("elevation")
        }
    }

def run(lat, lon):
    producer = get_producer()
    try:
        payload = fetch_weather(lat, lon)
        fut = producer.send(TOPIC, payload)
        fut.get(timeout=5)
        print(f"[OK] Message envoyé sur {TOPIC}")
    except requests.RequestException as e:
        print(f"[ERR] API: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Producteur météo enrichi (un seul envoi)")
    parser.add_argument("--lat", type=float, required=True)
    parser.add_argument("--lon", type=float, required=True)
    args = parser.parse_args()
    run(args.lat, args.lon)
