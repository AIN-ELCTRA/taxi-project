import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="taxi_db",
        user="postgres",
        password="postgres"
    )

    print("Connection successful ✅")

    conn.close()

except Exception as e:
    print("Error:", e)