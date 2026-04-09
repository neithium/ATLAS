from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
)
logger = logging.getLogger("ATLAS")

# ---------------- SPARK ----------------
spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

logger.info("STREAMING STARTED")

# ---------------- SCHEMA ----------------
schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("cpu", IntegerType()),
    StructField("mem", IntegerType())
])

# ---------------- READ KAFKA ----------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "raw-server-metrics") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

parsed = parsed.withColumn(
    "event_time",
    to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

# ---------------- AGG ----------------
  
agg = parsed \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("device_id")
    ) \
    .agg(
        avg("cpu").alias("avg_cpu"),
        avg("mem").alias("avg_mem"),
        count("*").alias("num_records")
    )

final_df = agg.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "device_id",
    "avg_cpu",
    "avg_mem",
    "num_records"
)

# ---------------- WRITE ----------------
# query = final_df \
    # .writeStream \
    # .format("parquet") \
    # .outputMode("append") \
    # .option("path", "/app/data/processed/stream") \
    # .option("checkpointLocation", "/app/checkpoint/stream") \
    # .trigger(processingTime="30 seconds") \
    # .start()
def log_and_write(batch_df, batch_id):
    rows = batch_df.count()

    print(f"🚀 STREAM BATCH | id={batch_id} | rows={rows}")

    logger.info(f"STREAM BATCH | id={batch_id} | rows={rows}")

    batch_df.write.mode("append").parquet("/app/data/processed/stream")

query = final_df \
    .writeStream \
    .foreachBatch(log_and_write) \
    .outputMode("append") \
    .option("checkpointLocation", "/app/checkpoint/stream") \
    .trigger(processingTime="30 seconds") \
    .start()
logger.info("Streaming query started")

query.awaitTermination()