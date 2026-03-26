import pandas as pd
import psycopg2

# connect to database
conn = psycopg2.connect(
    host="localhost",
    database="taxi_db",
    user="postgres",
    password="postgres"
)

cursor = conn.cursor()

# load ONLY 1000 rows (safe test)
df = pd.read_parquet("dataset/yellow_tripdata_2025-01.parquet").head(1000)

print(df.columns)  # just to confirm columns

# insert data
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO taxi_trips (
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            fare_amount,
            total_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row['tpep_pickup_datetime'],
        row['tpep_dropoff_datetime'],
        row['passenger_count'],
        row['trip_distance'],
        row['fare_amount'],
        row['total_amount']
    ))

conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully ✅")