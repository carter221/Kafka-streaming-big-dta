from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('weather_stream', json.dumps({'msg': 'Hello Kafka'}).encode('utf-8'))
producer.flush()
print("Message envoyé")
