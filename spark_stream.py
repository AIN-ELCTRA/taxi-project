import os

# Fix for Windows Hadoop issue
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

TOPIC_NAME = "taxi-topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaxiKafkaStream")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.hadoop.native.lib", "false")  # 🔥 fix crash
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")  # 🔥 extra fix
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = create_spark_session()

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_string = df_raw.selectExpr("CAST(value AS STRING) AS json_string")

    df_parsed = df_string.select(
        from_json(col("json_string"), schema).alias("data")
    )

    df_final = (
        df_parsed.select("data.*")
        .withColumn(
            "tpep_pickup_datetime",
            to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "tpep_dropoff_datetime",
            to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
        )
    )

    df_clean = df_final.filter(col("VendorID").isNotNull())

    query = (
        df_clean.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()