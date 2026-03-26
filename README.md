# taxi-project
🚖 Taxi Data Engineering Pipeline
📌 Overview

This project presents the design and implementation of a data engineering pipeline for processing large-scale taxi trip data. The system efficiently ingests a Parquet dataset, processes it using Python, and stores it in a PostgreSQL database using optimized batch techniques.

🎯 Objectives
Build a scalable data ingestion pipeline
Process large datasets efficiently (~3.5 million rows)
Optimize database insertion using PostgreSQL COPY
Prepare the system for future real-time streaming
📂 Dataset
Source: NYC Yellow Taxi Dataset
Format: Parquet
Size: ~3.5 million records
Content: Trip duration, passenger count, fare, distance
⚙️ Technologies Used
Python — Data processing
Pandas — Data manipulation
PyArrow — Parquet file handling
PostgreSQL — Data storage
Docker — Containerized database environment

🏗️ System Architecture
Parquet File
   ↓
Python Processing (PyArrow + Pandas)
   ↓
Data Cleaning & Transformation
   ↓
Batch Processing
   ↓
COPY Command
   ↓
PostgreSQL Database (Docker Container)
🗄️ Database Schema

Table: taxi_trips

Column Name	Data Type
id	SERIAL PRIMARY KEY
pickup_datetime	TIMESTAMP
dropoff_datetime	TIMESTAMP
passenger_count	INT
trip_distance	FLOAT
fare_amount	FLOAT
total_amount	FLOAT

🚀 Implementation
Step 1: Data Loading
Parquet file loaded using PyArrow
Converted into manageable batches
Step 2: Data Processing
Data cleaned and transformed
Data types adjusted for PostgreSQL compatibility
Step 3: Data Ingestion
Batch insertion using PostgreSQL COPY command
Significant performance improvement over row-based insertion

📊 Results
Successfully inserted ~2.9 million records
High performance using batch processing
Efficient memory usage
Scalable pipeline architecture

⚠️ Challenges
Handling large datasets
Data type mismatches (e.g., float vs integer)
Optimizing insertion performance
Managing memory usage

🔮 Future Work
Integrate Apache Kafka for real-time streaming
Add data visualization dashboard
Deploy using Docker Compose
Scale to distributed data processing

▶️ How to Run
1. Start PostgreSQL (Docker)
docker compose up -d
2. Install dependencies
pip install pandas pyarrow psycopg2-binary
3. Run data ingestion
python load_data_copy.py

📸 Example Output
Inserted 1000000 rows so far...
Inserted 2000000 rows so far...
Bulk insert completed successfully
Total inserted rows: 2935077

📚 References
Apache Software Foundation. (2023). Apache Parquet documentation. https://parquet.apache.org
PostgreSQL Global Development Group. (2024). PostgreSQL documentation. https://www.postgresql.org/docs
McKinney, W. (2022). Python for Data Analysis (3rd ed.). O’Reilly Media
Pandas Development Team. (2024). https://pandas.pydata.org/docs
Apache Arrow. (2024). https://arrow.apache.org/docs
