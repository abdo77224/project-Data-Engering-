from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from textblob import TextBlob
import json

# ==========================
# Kafka Consumer
# ==========================
consumer = KafkaConsumer(
    "tweets-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="twitter-group",
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

    # -------------------------
    # TextBlob Sentiment Analysis
    # -------------------------
    polarity = TextBlob(tweet["tweet"]).sentiment.polarity

    if polarity > 0:
        prediction = "Positive"
    elif polarity < 0:
        prediction = "Negative"
    else:
        prediction = "Neutral"

    # -------------------------
    # Ground Truth
    # -------------------------
    true_label = "Positive" if tweet["label"] == 0 else "Negative"

    # -------------------------
    # Document
    # -------------------------
    document = {
        "id": tweet["id"],
        "tweet": tweet["tweet"],
        "label": tweet["label"],
        "true_sentiment": true_label,
        "prediction": prediction,
        "polarity": round(polarity, 3)
    }

    # -------------------------
    # Elasticsearch
    # -------------------------
    es.index(
        index="tweets",
        document=document
    )

    # -------------------------
    # Console
    # -------------------------
    print("=" * 60)
    print(f"Tweet ID         : {tweet['id']}")
    print(f"Dataset Label    : {true_label}")
    print(f"TextBlob Predict : {prediction}")
    print(f"Polarity         : {polarity:.3f}")
    print(f"Tweet            : {tweet['tweet']}")