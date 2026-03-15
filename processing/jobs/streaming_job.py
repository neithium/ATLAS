# # # Spark Processor
# # # ✔ Container running
# # # ✔ Spark session initialized
# # # ✔ Spark UI accessible
# # # ✔ Executor active
# # # ✖ No streaming job yet
# # # Currently the Spark processor container initializes a Spark session 
# # # and exposes the Spark UI. Since Kafka ingestion is not yet integrated, 
# # # no streaming queries are running, so the executor remains idle.
# # #  An infinite sleep loop keeps the Spark driver alive for debugging until the Kafka streaming pipeline is implemented.


# # from pyspark.sql import SparkSession
# # import time

# # spark = SparkSession.builder \
# #     .appName("AtlasProcessor") \
# #     .getOrCreate()

# # print("Spark Processor Container Initialized Successfully")

# # # Keep Spark running so Spark UI stays alive
# # while True:
# #     time.sleep(60)


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, window, rand
# import time

# # ---------------------------------------------------
# # Spark Session
# # ---------------------------------------------------

# spark = SparkSession.builder \
#     .appName("ATLAS Processor Demo") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# print("====================================")
# print("ATLAS PROCESSOR CONTAINER STARTED")
# print("====================================")

# # ---------------------------------------------------
# # STREAMING PIPELINE
# # Simulates telemetry until Kafka exists
# # ---------------------------------------------------

# stream_df = spark.readStream \
#     .format("rate") \
#     .option("rowsPerSecond", 50) \
#     .load()

# # Create fake telemetry
# telemetry = stream_df.select(
#     (col("value") % 100).alias("device_id"),
#     (rand()*100).alias("cpu_usage"),
#     (rand()*100).alias("memory_usage"),
#     col("timestamp")
# )

# # 1 minute window aggregation (demo)
# aggregated = telemetry \
#     .withWatermark("timestamp", "1 minute") \
#     .groupBy(
#         window(col("timestamp"), "1 minute"),
#         col("device_id")
#     ) \
#     .avg("cpu_usage", "memory_usage")

# # ---------------------------------------------------
# # Write streaming output
# # ---------------------------------------------------

# query = aggregated.writeStream \
#     .format("parquet") \
#     .option("path", "/app/data/processed/stream") \
#     .option("checkpointLocation", "/app/checkpoint/stream") \
#     .outputMode("append") \
#     .start()

# print("Streaming job started")
# print("Writing parquet to /app/data/processed/stream")

# # ---------------------------------------------------
# # BATCH AGGREGATION JOB
# # ---------------------------------------------------

# def run_batch():

#     print("Running batch aggregation...")

#     try:

#         df = spark.read.parquet("/app/data/processed/stream")

#         result = df.groupBy("device_id") \
#             .avg("avg(cpu_usage)", "avg(memory_usage)")

#         result.write \
#             .mode("overwrite") \
#             .parquet("/app/data/processed/batch")

#         print("Batch parquet written")

#     except Exception as e:

#         print("Waiting for streaming files...")
#         print(e)

# # ---------------------------------------------------
# # Run batch every minute
# # ---------------------------------------------------

# while True:

#     time.sleep(60)

#     run_batch()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, rand

spark = SparkSession.builder \
    .appName("AtlasStreamingProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("====================================")
print("ATLAS STREAMING PROCESSOR STARTED")
print("====================================")

# Simulated telemetry stream
stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 50) \
    .load()

telemetry = stream_df.select(
    (col("value") % 100).alias("device_id"),
    (rand()*100).alias("cpu_usage"),
    (rand()*100).alias("memory_usage"),
    col("timestamp")
)

aggregated = telemetry \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("device_id")
    ) \
    .avg("cpu_usage", "memory_usage")

query = aggregated.writeStream \
    .format("parquet") \
    .option("path", "/app/data/processed/stream") \
    .option("checkpointLocation", "/app/checkpoint/stream") \
    .outputMode("append") \
    .start()

print("Streaming pipeline running...")

query.awaitTermination()