"""
TVMJNS — Simple Batch Alert Processor (Demo-Friendly)

Reads telemetry from Kafka, checks thresholds, writes alerts to PostgreSQL.
Runs in LOCAL mode — no Spark cluster needed!

Usage:
    python scripts/spark_batch_alerts.py

This is the EASY version for demos - just run it and show results in Adminer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "telemetry"

TEMP_THRESHOLD = 30.0      # Alert if temperature > 30°C
BATTERY_THRESHOLD = 20.0   # Alert if battery < 20%

DB_URL = "jdbc:postgresql://localhost:5432/streaming_db"
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
    print("TVMJNS — Spark Batch Alert Processor")
    print("=" * 60)
    
    # Create LOCAL Spark session (no cluster needed)
    spark = (SparkSession.builder
        .appName("TVMJNS-BatchAlerts")
        .master("local[*]")  # ← Runs locally, easy demo
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.7.3")
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
    print(f"✓ Found {total} telemetry records\n")
    
    if total == 0:
        print("No data to process. Run producer.py first!")
        spark.stop()
        return
    
    print("Sample telemetry data:")
    telemetry.show(5, truncate=False)
    
    # ── Find threshold violations ─────────────────────────────────────────
    print(f"\nChecking thresholds:")
    print(f"  • Temperature > {TEMP_THRESHOLD}°C")
    print(f"  • Battery < {BATTERY_THRESHOLD}%\n")
    
    # HIGH TEMPERATURE alerts
    high_temp = (telemetry
        .filter(col("temperature") > TEMP_THRESHOLD)
        .select(
            col("sensor_id"),
            lit("HIGH_TEMPERATURE").alias("alert_type"),
            lit("warning").alias("severity"),
            (lit("Temp=") + col("temperature").cast("string") + lit("°C")).alias("message"),
            current_timestamp().alias("triggered_at")))
    
    # LOW BATTERY alerts
    low_batt = (telemetry
        .filter(col("battery_level") < BATTERY_THRESHOLD)
        .select(
            col("sensor_id"),
            lit("LOW_BATTERY").alias("alert_type"),
            lit("critical").alias("severity"),
            (lit("Battery=") + col("battery_level").cast("string") + lit("%")).alias("message"),
            current_timestamp().alias("triggered_at")))
    
    alerts = high_temp.union(low_batt)
    alert_count = alerts.count()
    
    # ── Show and write results ────────────────────────────────────────────
    print(f"🚨 Found {alert_count} alerts:\n")
    
    if alert_count > 0:
        alerts.show(20, truncate=False)
        
        # Write to PostgreSQL
        print("Writing to PostgreSQL...")
        alerts.write.jdbc(DB_URL, "alerts", mode="append", properties=DB_PROPS)
        print(f"✓ Wrote {alert_count} alerts to 'alerts' table")
        print("\nView in Adminer: http://localhost:8888")
    else:
        print("No threshold violations found.")
    
    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
