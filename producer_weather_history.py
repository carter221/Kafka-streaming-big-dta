"""
Exercice 9 : Récupération de séries historiques longues
1. Télécharge des données météo (10 ans) via l'API archive Open-Meteo.
2. Publie chaque enregistrement horaire brut dans Kafka (topic: weather_history_raw).
3. Sauvegarde en parallèle les données brutes dans HDFS
   Chemin: /hdfs-data/{country}/{city}/weather_history_raw/year=YYYY/part-YYYY.json (JSON Lines)
Utilisation:
  python historical_fetch.py --city Paris --country France --years 10
Options:
  --start-year 2015 (sinon start = année_courante - years)
  --no-hdfs (désactive l'écriture HDFS)
Variables env:
  KAFKA_BOOTSTRAP (default localhost:9092)
  HISTORY_TOPIC (default weather_history_raw)
  HDFS_NAMENODE_HTTP (default http://localhost:9870)
  HDFS_USER (default root)
"""
import os, sys, json, time, argparse, datetime, logging
from typing import Tuple, List
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
try:
    from hdfs import InsecureClient
except Exception:
    InsecureClient = None  # HDFS optionnel

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
HISTORY_TOPIC = os.getenv("HISTORY_TOPIC", "weather_history_raw")
HDFS_URL = os.getenv("HDFS_NAMENODE_HTTP", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_PARAMS = [
    "temperature_2m",
    "windspeed_10m",
    "winddirection_10m",
    "weathercode",
    "precipitation"        
]

def geocode_city(name: str, country: str = None) -> Tuple[float, float, str, str]:
    params = {
        "name": name,
        "count": 5,
        "language": "en",
        "format": "json"
    }
    r = requests.get(GEOCODE_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    if not results:
        raise SystemExit(f"Ville introuvable: {name}")
    if country:
        country_lower = country.lower()
        filtered = [r for r in results if r.get("country","").lower() == country_lower]
        if filtered:
            results = filtered
    r0 = results[0]
    return r0["latitude"], r0["longitude"], r0["name"], r0["country"]

def build_year_ranges(start_year: int, years: int) -> List[Tuple[str, str, int]]:
    ranges = []
    for y in range(start_year, start_year + years):
        start = f"{y}-01-01"
        end = f"{y}-12-31"
        ranges.append((start, end, y))
    # Dernière année si elle dépasse la date actuelle -> tronquer
    today = datetime.date.today()
    last_start, last_end, last_year = ranges[-1]
    if last_year == today.year:
        last_end = today.strftime("%Y-%m-%d")
        ranges[-1] = (last_start, last_end, last_year)
    return ranges

def init_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda d: json.dumps(d).encode("utf-8")
        )
        return producer
    except NoBrokersAvailable:
        raise SystemExit(f"Broker Kafka indisponible: {BOOTSTRAP}")

def init_hdfs(enable: bool):
    if not enable:
        return None
    if InsecureClient is None:
        logging.warning("Lib hdfs non installée -> pas d'écriture HDFS.")
        return None
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        # simple ping
        client.status("/", strict=False)
        return client
    except Exception as e:
        logging.error("HDFS indisponible (%s) -> désactivation écriture.", e)
        return None

def fetch_year(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_PARAMS),
        "timezone": "UTC"
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def write_year_hdfs(client, country: str, city: str, year: int, lines: List[str]):
    if not client:
        return
    base_dir = f"/hdfs-data/{country}/{city}/weather_history_raw/year={year}"
    client.makedirs(base_dir)
    file_path = f"{base_dir}/part-{year}.json"
    append = bool(client.status(file_path, strict=False))
    data_blob = "".join(lines)
    client.write(file_path, data_blob, append=append, encoding="utf-8")
    logging.info("HDFS écrit %s (%d lignes, mode=%s)", file_path, len(lines), "append" if append else "create")

def process_year(producer, client, payload_year: dict, city: str, country: str,
                 lat: float, lon: float, year: int):
    hourly = payload_year.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        logging.warning("Aucune donnée année %d", year)
        return
    # collect arrays
    temp_arr = hourly.get("temperature_2m") or []
    wind_arr = hourly.get("windspeed_10m") or []
    winddir_arr = hourly.get("winddirection_10m") or []
    code_arr = hourly.get("weathercode") or []
    total = len(times)
    out_lines = []
    for i, t in enumerate(times):
        msg = {
            "source": "archive_api",
            "city": city,
            "country": country,
            "latitude": lat,
            "longitude": lon,
            "event_time": t,
            "year": year,
            "temperature_2m": temp_arr[i] if i < len(temp_arr) else None,
            "windspeed_10m": wind_arr[i] if i < len(wind_arr) else None,
            "winddirection_10m": winddir_arr[i] if i < len(winddir_arr) else None,
            "weathercode": code_arr[i] if i < len(code_arr) else None
        }
        # Envoi Kafka
        producer.send(HISTORY_TOPIC, msg)
        # Pour HDFS (JSON line brute)
        out_lines.append(json.dumps(msg, ensure_ascii=False) + "\n")
        if (i + 1) % 5000 == 0:
            producer.flush()
            logging.info("Progress année %d: %d/%d", year, i + 1, total)
    producer.flush()
    write_year_hdfs(client, country, city, year, out_lines)
    logging.info("Année %d terminée (%d messages).", year, total)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True, help="Ville (ex: Paris)")
    ap.add_argument("--country", help="Pays (ex: France)")
    ap.add_argument("--years", type=int, default=10, help="Nombre d'années (10)")
    ap.add_argument("--start-year", type=int, help="Année de départ (défaut: année_courante - years)")
    ap.add_argument("--no-hdfs", action="store_true", help="Désactive écriture HDFS")
    args = ap.parse_args()

    lat, lon, resolved_city, resolved_country = geocode_city(args.city, args.country)
    logging.info("Géocodé: %s, %s -> (%.4f, %.4f)", resolved_city, resolved_country, lat, lon)

    if args.start_year:
        start_year = args.start_year
    else:
        current_year = datetime.date.today().year
        start_year = current_year - args.years

    ranges = build_year_ranges(start_year, args.years)
    logging.info("Plages: %s", ranges)

    producer = init_producer()
    hdfs_client = init_hdfs(not args.no_hdfs)

    start = time.time()
    for (start_date, end_date, year) in ranges:
        logging.info("Téléchargement année %d (%s -> %s)", year, start_date, end_date)
        data = fetch_year(lat, lon, start_date, end_date)
        process_year(producer, hdfs_client, data, resolved_city, resolved_country, lat, lon, year)

    logging.info("Terminé en %.1fs", time.time() - start)

if __name__ == "__main__":
    main()