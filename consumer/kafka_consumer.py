from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

# ==========================
# Kafka Consumer
# ==========================
consumer = KafkaConsumer(
    "tweets-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Connected to Kafka...")

# ==========================
# Elasticsearch
# ==========================
es = Elasticsearch("http://localhost:9200")

print("Connected to Elasticsearch...")
print("Waiting for tweets...\n")

# ==========================
# Streaming
# ==========================
for message in consumer:

    tweet = message.value

    # تحويل label إلى sentiment
    if tweet["label"] == 0:
        sentiment = "Positive"
    else:
        sentiment = "Negative"

    document = {
        "id": tweet["id"],
        "tweet": tweet["tweet"],
        "label": tweet["label"],
        "sentiment": sentiment
    }

    # Elasticsearch
    es.index(index="tweets", document=document)

    print(document)