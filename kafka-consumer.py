

from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import os
import time
import logging
import signal
import sys
import datetime

# ---------- Configuration ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_transformed")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "weather_hdfs_consumer")
AUTO_OFFSET = os.getenv("KAFKA_AUTO_OFFSET", "latest")  # earliest | latest

HDFS_URL = os.getenv("HDFS_NAMENODE_HTTP", "http://namenode:9870")  # adapter selon env (docker: http://namenode:9870)
HDFS_BASE = os.getenv("HDFS_BASE_PATH", "/hdfs-data")
HDFS_USER = os.getenv("HDFS_USER", "root")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

# ---------- HDFS Client ----------
try:
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
except Exception as e:
    logging.error("Impossible d'initialiser le client HDFS (%s)", e)
    sys.exit(1)

_shutdown = False
def _handle_signal(sig, frame):
    global _shutdown
    logging.info("Signal reçu (%s) -> arrêt après le message courant.", sig)
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------- Utilitaires ----------
def sanitize(value: str) -> str:
    if not value:
        return "unknown"
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in value)[:80] or "unknown"

def is_alert(payload: dict) -> bool:
    if payload.get("alert") is True:
        return True
    if str(payload.get("type", "")).lower() == "alert":
        return True
    sev = str(payload.get("severity", "")).lower()
    if sev in ("warning", "alert", "critical"):
        return True
    # Ajout niveaux dérivés Spark
    if payload.get("wind_alert_level") in ("level_1", "level_2"):
        return True
    if payload.get("heat_alert_level") in ("level_1", "level_2"):
        return True
    return False

def build_hdfs_path(payload: dict) -> str:
    country = sanitize(payload.get("country"))
    city = sanitize(payload.get("city"))
    return f"{HDFS_BASE}/{country}/{city}/alerts.json"

def write_alert(payload: dict):
    file_path = build_hdfs_path(payload)
    dir_path = os.path.dirname(file_path)
    hdfs_client.makedirs(dir_path)

    event_ts = _parse_event_ts(payload)

    record = {
        "ingest_ts": int(time.time()),
        "event_ts": event_ts,
        "country": sanitize(payload.get("country")),
        "city": sanitize(payload.get("city")),
        "data": payload
    }
    line = json.dumps(record, ensure_ascii=False) + "\n"
    try:
        append_mode = bool(hdfs_client.status(file_path, strict=False))
    except Exception:
        append_mode = False
    hdfs_client.write(file_path, line, append=append_mode, encoding="utf-8")
    logging.info("Alerte écrite (%s) -> %s (event_ts=%d)", "append" if append_mode else "create", file_path, event_ts)

def create_consumer() -> KafkaConsumer:
    logging.info("Connexion Kafka bootstrap=%s topic=%s group=%s offset_mode=%s",
                 BOOTSTRAP, TOPIC, GROUP_ID, AUTO_OFFSET)
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset=AUTO_OFFSET,
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8", errors="ignore"))
    )

def _parse_event_ts(payload: dict) -> int:
    """
    Retourne un epoch (int secondes) à partir des champs possibles du message.
    Priorité :
      1. timestamp_epoch (numérique)
      2. timestamp / event_time / time (ISO 8601)
      3. maintenant
    """
    # 1) Champ déjà epoch
    for k in ("timestamp_epoch", "event_ts_epoch"):
        v = payload.get(k)
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str) and v.isdigit():
            return int(v)

    # 2) Champs texte ISO
    for k in ("timestamp", "event_time", "time"):
        v = payload.get(k)
        if not v or not isinstance(v, str):
            continue
        # tenter plusieurs formats
        fmts = [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M"
        ]
        for fmt in fmts:
            try:
                dt = datetime.datetime.strptime(v, fmt)
                # si pas de tz, on suppose UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return int(dt.timestamp())
            except Exception:
                pass
    # 3) fallback
    return int(time.time())


def main():
    consumer = create_consumer()
    if AUTO_OFFSET == "latest":
        logging.info("Mode latest: produire un NOUVEAU message pour tester.")
    processed = 0
    alerts = 0
    for msg in consumer:
        if _shutdown:
            logging.info("Arrêt demandé. Total messages=%d alertes=%d", processed, alerts)
            break
        payload = msg.value
        processed += 1
        if not isinstance(payload, dict):
            continue
        if not is_alert(payload):
            continue
        try:
            write_alert(payload)
            alerts += 1
        except Exception as e:
            logging.error("Échec écriture HDFS: %s", e)

if __name__ == "__main__":
    main()