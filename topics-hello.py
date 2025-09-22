# create_topic.py
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

admin = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="topic_creator")

topic = NewTopic(name="weather_stream", num_partitions=1, replication_factor=1)

try:
    admin.create_topics([topic])
    print("Topic créé.")
except TopicAlreadyExistsError:
    print("Topic déjà existant.")
finally:
    admin.close()