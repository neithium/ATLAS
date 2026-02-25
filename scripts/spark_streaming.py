"""
TVMJNS — Spark Structured Streaming Job (Threshold Alert Detector)

Reads telemetry from Kafka, checks for threshold violations, and writes alerts to PostgreSQL.

Thresholds:
  - Temperature > 30°C  → HIGH_TEMPERATURE alert
  - Battery < 20%       → LOW_BATTERY alert

Usage:
    python scripts/spark_streaming.py

Architecture:
    Kafka (telemetry topic) → Spark Streaming → PostgreSQL (alerts table)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "telemetry"

# Thresholds for alerts
TEMP_THRESHOLD = 30.0      # Alert if temperature > 30°C
BATTERY_THRESHOLD = 20.0   # Alert if battery < 20%

# PostgreSQL connection
DB_URL = "jdbc:postgresql://localhost:5432/streaming_db"
DB_PROPERTIES = {
    "user": "streaming_user",
    "password": "streaming_pass",
    "driver": "org.postgresql.Driver"
}

# ── Telemetry Schema ──────────────────────────────────────────────────────────
# Must match the JSON structure from producer.py
telemetry_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("battery_level", DoubleType(), True),
])


def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka and PostgreSQL packages."""
    return (
        SparkSession.builder
        .appName("TVMJNS-ThresholdAlertDetector")
        .master("spark://localhost:7077")  # Connect to our Spark cluster
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .getOrCreate()
    )


def process_batch(df, epoch_id):
    """
    Process each micro-batch: check thresholds and write alerts to PostgreSQL.
    
    This function is called for each batch of data from Kafka.
    """
    if df.isEmpty():
        return
    
    print(f"\n{'='*60}")
    print(f"Processing batch {epoch_id} with {df.count()} records")
    print("="*60)
    
    # Show incoming data
    df.show(truncate=False)
    
    # ── Check for HIGH TEMPERATURE alerts ─────────────────────────────────
    high_temp_alerts = (
        df.filter(col("temperature") > TEMP_THRESHOLD)
        .select(
            col("sensor_id"),
            lit("HIGH_TEMPERATURE").alias("alert_type"),
            lit("warning").alias("severity"),
            (lit("Temperature ") + col("temperature").cast("string") + 
             lit("°C exceeds threshold of ") + lit(str(TEMP_THRESHOLD)) + lit("°C"))
            .alias("message"),
            current_timestamp().alias("triggered_at")
        )
    )
    
    # ── Check for LOW BATTERY alerts ──────────────────────────────────────
    low_battery_alerts = (
        df.filter(col("battery_level") < BATTERY_THRESHOLD)
        .select(
            col("sensor_id"),
            lit("LOW_BATTERY").alias("alert_type"),
            lit("critical").alias("severity"),
            (lit("Battery level ") + col("battery_level").cast("string") + 
             lit("% is below threshold of ") + lit(str(BATTERY_THRESHOLD)) + lit("%"))
            .alias("message"),
            current_timestamp().alias("triggered_at")
        )
    )
    
    # ── Combine all alerts ────────────────────────────────────────────────
    all_alerts = high_temp_alerts.union(low_battery_alerts)
    
    if all_alerts.count() > 0:
        print(f"\n🚨 ALERTS DETECTED:")
        all_alerts.show(truncate=False)
        
        # Write to PostgreSQL
        (all_alerts
         .write
         .jdbc(url=DB_URL, table="alerts", mode="append", properties=DB_PROPERTIES))
        
        print(f"✓ {all_alerts.count()} alert(s) written to PostgreSQL")
    else:
        print("✓ No threshold violations in this batch")


def main():
    print("=" * 70)
    print("TVMJNS — Spark Structured Streaming (Threshold Alert Detector)")
    print("=" * 70)
    print(f"Kafka: {KAFKA_BOOTSTRAP} / Topic: {KAFKA_TOPIC}")
    print(f"Thresholds: Temperature > {TEMP_THRESHOLD}°C, Battery < {BATTERY_THRESHOLD}%")
    print(f"Output: PostgreSQL alerts table")
    print("=" * 70)
    print("Press Ctrl+C to stop\n")
    
    # Create Spark session
    print("Connecting to Spark cluster...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Connected to Spark\n")
    
    # ── Read from Kafka ───────────────────────────────────────────────────
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")  # Only process new messages
        .load()
    )
    
    # ── Parse JSON and extract fields ─────────────────────────────────────
    telemetry_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), telemetry_schema).alias("data"))
        .select("data.*")
    )
    
    # ── Process stream with foreachBatch ──────────────────────────────────
    # foreachBatch allows us to use JDBC writes (which need batch operations)
    query = (
        telemetry_df
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime="5 seconds")  # Process every 5 seconds
        .start()
    )
    
    print("✓ Streaming started. Waiting for telemetry data...\n")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping stream...")
        query.stop()
        spark.stop()
        print("Stream stopped.")


if __name__ == "__main__":
    main()
