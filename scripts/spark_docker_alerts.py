"""
TVMJNS — Spark Batch Alert Processor (Docker Version)

Reads telemetry from Kafka, checks thresholds, writes alerts to PostgreSQL.
Runs INSIDE the Spark Docker container to avoid Windows/Java compatibility issues.

Usage (from project root):
    docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 /scripts/spark_docker_alerts.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ── Configuration (Docker internal network) ───────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"       # Docker internal listener (PLAINTEXT)
KAFKA_TOPIC = "telemetry"

TEMP_THRESHOLD = 30.0      # Alert if temperature > 30°C
BATTERY_THRESHOLD = 20.0   # Alert if battery < 20%

DB_URL = "jdbc:postgresql://postgres:5432/streaming_db"  # Docker service name
DB_PROPS = {
    "user": "streaming_user",
    "password": "streaming_pass",
    "driver": "org.postgresql.Driver"
}

# Schema matching producer.py output
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("battery_level", DoubleType()),
])


def main():
    print("=" * 60)
    print("TVMJNS — Spark Batch Alert Processor (Docker)")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"PostgreSQL: {DB_URL}")
    print(f"Thresholds: temp > {TEMP_THRESHOLD}°C, battery < {BATTERY_THRESHOLD}%")
    print("=" * 60)
    
    # Create Spark session
    spark = (SparkSession.builder
        .appName("TVMJNS-BatchAlerts")
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    
    # ── Read all messages from Kafka ──────────────────────────────────────
    print(f"\nReading from Kafka topic '{KAFKA_TOPIC}'...")
    
    raw_df = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load())
    
    # Parse JSON
    telemetry = (raw_df
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json("json", schema).alias("d"))
        .select("d.*"))
    
    total = telemetry.count()
    print(f"Found {total} telemetry records\n")
    
    if total == 0:
        print("No data to process. Run producer.py first!")
        spark.stop()
        return
    
    print("Sample telemetry data:")
    telemetry.show(5, truncate=False)
    
    # ── Find threshold violations ─────────────────────────────────────────
    print(f"\nChecking thresholds:")
    print(f"  - Temperature > {TEMP_THRESHOLD}C")
    print(f"  - Battery < {BATTERY_THRESHOLD}%\n")
    
    # HIGH TEMPERATURE alerts
    high_temp = (telemetry
        .filter(col("temperature") > TEMP_THRESHOLD)
        .select(
            col("sensor_id"),
            lit("HIGH_TEMPERATURE").alias("alert_type"),
            lit("warning").alias("severity"),
            concat(lit("Temp="), col("temperature").cast("string"), lit("C")).alias("message"),
            current_timestamp().alias("triggered_at")))
    
    # LOW BATTERY alerts  
    low_batt = (telemetry
        .filter(col("battery_level") < BATTERY_THRESHOLD)
        .select(
            col("sensor_id"),
            lit("LOW_BATTERY").alias("alert_type"),
            lit("critical").alias("severity"),
            concat(lit("Battery="), col("battery_level").cast("string"), lit("%")).alias("message"),
            current_timestamp().alias("triggered_at")))
    
    alerts = high_temp.union(low_batt)
    alert_count = alerts.count()
    
    # ── Show and write results ────────────────────────────────────────────
    print(f"ALERTS FOUND: {alert_count}\n")
    
    if alert_count > 0:
        alerts.show(20, truncate=False)
        
        # Write to PostgreSQL
        print("Writing to PostgreSQL...")
        alerts.write.jdbc(DB_URL, "alerts", mode="append", properties=DB_PROPS)
        print(f"Wrote {alert_count} alerts to 'alerts' table")
        print("\nView in Adminer: http://localhost:8888")
    else:
        print("No threshold violations found.")
    
    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
