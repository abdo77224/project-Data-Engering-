import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read dataset
df = pd.read_csv("data/twitter.csv")

print(f"Dataset loaded: {len(df)} tweets")

# Send tweets one by one
for index, row in df.iterrows():

    tweet_data = {
        "id": int(row["id"]),
        "tweet": str(row["tweet"]),
        "label": int(row["label"])
    }

    producer.send("tweets-topic", tweet_data)

    print(f"Sent Tweet ID {tweet_data['id']}")

    time.sleep(5)

producer.flush()