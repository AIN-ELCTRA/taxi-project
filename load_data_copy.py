import pyarrow.parquet as pq
import pandas as pd
import psycopg2
from io import StringIO

PARQUET_FILE = "dataset/yellow_tripdata_2025-01.parquet"
BATCH_SIZE = 100000

conn = psycopg2.connect(
    host="localhost",
    database="taxi_db",
    user="postgres",
    password="postgres"
)
cursor = conn.cursor()

parquet_file = pq.ParquetFile(PARQUET_FILE)
total_inserted = 0

for batch in parquet_file.iter_batches(
    batch_size=BATCH_SIZE,
    columns=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]
):
    chunk = batch.to_pandas()

    chunk = chunk.rename(columns={
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    })

    chunk["passenger_count"] = pd.to_numeric(chunk["passenger_count"], errors="coerce")
    chunk["trip_distance"] = pd.to_numeric(chunk["trip_distance"], errors="coerce")
    chunk["fare_amount"] = pd.to_numeric(chunk["fare_amount"], errors="coerce")
    chunk["total_amount"] = pd.to_numeric(chunk["total_amount"], errors="coerce")

    chunk = chunk.dropna(subset=[
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ])

    chunk["passenger_count"] = chunk["passenger_count"].astype(int)

    buffer = StringIO()
    chunk.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(
        """
        COPY taxi_trips (
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            total_amount
        )
        FROM STDIN WITH CSV
        """,
        buffer
    )

    conn.commit()
    total_inserted += len(chunk)
    print(f"Inserted {total_inserted} rows so far...")

cursor.close()
conn.close()

print("Bulk insert with batch COPY completed successfully ✅")
print(f"Total inserted rows: {total_inserted}")