import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC_NAME = "taxi-topic"
DATA_PATH = Path("dataset/yellow_tripdata_2025-01.parquet")  # change if your file name is different

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Checking dataset path...")
if not DATA_PATH.exists():
    raise FileNotFoundError(f"Dataset not found: {DATA_PATH.resolve()}")

print("Reading dataset...")
df = pd.read_parquet(DATA_PATH)

# First test with a small sample
df = df.head(1000)

print(f"Streaming {len(df)} rows to Kafka topic '{TOPIC_NAME}'...")

sent_count = 0

try:
    for i, row in df.iterrows():
        message = {
            "VendorID": None if pd.isna(row.get("VendorID")) else int(row.get("VendorID")),
            "tpep_pickup_datetime": None if pd.isna(row.get("tpep_pickup_datetime")) else str(row.get("tpep_pickup_datetime")),
            "tpep_dropoff_datetime": None if pd.isna(row.get("tpep_dropoff_datetime")) else str(row.get("tpep_dropoff_datetime")),
            "passenger_count": None if pd.isna(row.get("passenger_count")) else float(row.get("passenger_count")),
            "trip_distance": None if pd.isna(row.get("trip_distance")) else float(row.get("trip_distance")),
            "PULocationID": None if pd.isna(row.get("PULocationID")) else int(row.get("PULocationID")),
            "DOLocationID": None if pd.isna(row.get("DOLocationID")) else int(row.get("DOLocationID")),
            "fare_amount": None if pd.isna(row.get("fare_amount")) else float(row.get("fare_amount")),
            "total_amount": None if pd.isna(row.get("total_amount")) else float(row.get("total_amount"))
        }

        future = producer.send(TOPIC_NAME, value=message)
        record_metadata = future.get(timeout=10)

        sent_count += 1

        if sent_count % 100 == 0:
            print(
                f"Sent {sent_count} rows "
                f"(topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset})"
            )

        time.sleep(0.01)

except KafkaError as e:
    print(f"Kafka error occurred: {e}")
except Exception as e:
    print(f"Unexpected error occurred: {e}")
finally:
    producer.flush()
    producer.close()

print(f"Finished streaming {sent_count} rows to Kafka.")