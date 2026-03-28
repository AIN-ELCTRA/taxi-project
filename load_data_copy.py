import os
import io
import pandas as pd
import psycopg2

# ----------------------------
# Environment variables
# ----------------------------
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "taxi_db")

# ----------------------------
# File path
# ----------------------------
FILE_PATH = "dataset/yellow_tripdata_2025-01.parquet"

# ----------------------------
# Read parquet file
# ----------------------------
print("Reading Parquet file...")
df = pd.read_parquet(FILE_PATH)

# ----------------------------
# Keep and rename needed columns
# ----------------------------
df = df[
    [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]
].copy()

df.rename(
    columns={
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    },
    inplace=True,
)

# ----------------------------
# Basic cleaning
# ----------------------------
df = df.dropna()

df["passenger_count"] = df["passenger_count"].astype(int)
df["trip_distance"] = df["trip_distance"].astype(float)
df["fare_amount"] = df["fare_amount"].astype(float)
df["total_amount"] = df["total_amount"].astype(float)

print(f"Rows ready for loading: {len(df):,}")

# ----------------------------
# Connect to PostgreSQL
# ----------------------------
print("Connecting to PostgreSQL...")
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cur = conn.cursor()

# ----------------------------
# Create table
# ----------------------------
create_table_query = """
CREATE TABLE IF NOT EXISTS public.taxi_trips (
    id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
);
"""
cur.execute(create_table_query)
conn.commit()

# ----------------------------
# Optional: clear old data
# ----------------------------
cur.execute("TRUNCATE TABLE public.taxi_trips RESTART IDENTITY;")
conn.commit()

# ----------------------------
# Export dataframe to CSV buffer
# ----------------------------
print("Preparing in-memory CSV buffer for COPY...")
buffer = io.StringIO()
df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

# ----------------------------
# COPY into PostgreSQL
# ----------------------------
copy_query = """
COPY public.taxi_trips (
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount
)
FROM STDIN WITH CSV
"""

print("Loading data into PostgreSQL with COPY...")
cur.copy_expert(copy_query, buffer)
conn.commit()

# ----------------------------
# Check row count
# ----------------------------
cur.execute("SELECT COUNT(*) FROM public.taxi_trips;")
row_count = cur.fetchone()[0]

print(f"Data loading complete. Rows in table: {row_count:,}")

# ----------------------------
# Close connection
# ----------------------------
cur.close()
conn.close()
print("Connection closed.")