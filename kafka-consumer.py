from kafka import KafkaConsumer
import json
BOOTSTRAP = "localhost:9092"
TOPIC = "weather_transformed"

for msg in KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="latest",   # changer en earliest si besoin
    enable_auto_commit=True,
    group_id="weather_consumer_group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
):
    print(msg.value)