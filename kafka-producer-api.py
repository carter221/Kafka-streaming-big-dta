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
    params = {"latitude": lat, "longitude": lon, "current_weather": "true"}
    r = requests.get(API_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    cw = data.get("current_weather", {})
    return {
        "latitude": lat,
        "longitude": lon,
        "timestamp": cw.get("time"),
        "temperature": cw.get("temperature"),
        "windspeed": cw.get("windspeed"),
        "winddirection": cw.get("winddirection"),
        "raw": cw
    }

def run(lat, lon):
    producer = get_producer()
    try:
        payload = fetch_weather(lat, lon)
        future = producer.send(TOPIC, payload)
        meta = future.get(timeout=5)
        print(f"Message envoyé topic={TOPIC}")
    except requests.RequestException as e:
        print(f"[ERR] Requête API: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Producteur météo (un seul envoi)")
    parser.add_argument("--lat", type=float, required=True, help="Latitude")
    parser.add_argument("--lon", type=float, required=True, help="Longitude")
    args = parser.parse_args()
    run(args.lat, args.lon)