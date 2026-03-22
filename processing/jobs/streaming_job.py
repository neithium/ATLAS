# # # # # # Spark Processor
# # # # # # ✔ Container running
# # # # # # ✔ Spark session initialized
# # # # # # ✔ Spark UI accessible
# # # # # # ✔ Executor active
# # # # # # ✖ No streaming job yet
# # # # # # Currently the Spark processor container initializes a Spark session 
# # # # # # and exposes the Spark UI. Since Kafka ingestion is not yet integrated, 
# # # # # # no streaming queries are running, so the executor remains idle.
# # # # # #  An infinite sleep loop keeps the Spark driver alive for debugging until the Kafka streaming pipeline is implemented.


# # # # # from pyspark.sql import SparkSession
# # # # # import time

# # # # # spark = SparkSession.builder \
# # # # #     .appName("AtlasProcessor") \
# # # # #     .getOrCreate()

# # # # # print("Spark Processor Container Initialized Successfully")

# # # # # # Keep Spark running so Spark UI stays alive
# # # # # while True:
# # # # #     time.sleep(60)


# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import col, window, rand
# # # # import time

# # # # # ---------------------------------------------------
# # # # # Spark Session
# # # # # ---------------------------------------------------

# # # # spark = SparkSession.builder \
# # # #     .appName("ATLAS Processor Demo") \
# # # #     .getOrCreate()

# # # # spark.sparkContext.setLogLevel("WARN")

# # # # print("====================================")
# # # # print("ATLAS PROCESSOR CONTAINER STARTED")
# # # # print("====================================")

# # # # # ---------------------------------------------------
# # # # # STREAMING PIPELINE
# # # # # Simulates telemetry until Kafka exists
# # # # # ---------------------------------------------------

# # # # stream_df = spark.readStream \
# # # #     .format("rate") \
# # # #     .option("rowsPerSecond", 50) \
# # # #     .load()

# # # # # Create fake telemetry
# # # # telemetry = stream_df.select(
# # # #     (col("value") % 100).alias("device_id"),
# # # #     (rand()*100).alias("cpu_usage"),
# # # #     (rand()*100).alias("memory_usage"),
# # # #     col("timestamp")
# # # # )

# # # # # 1 minute window aggregation (demo)
# # # # aggregated = telemetry \
# # # #     .withWatermark("timestamp", "1 minute") \
# # # #     .groupBy(
# # # #         window(col("timestamp"), "1 minute"),
# # # #         col("device_id")
# # # #     ) \
# # # #     .avg("cpu_usage", "memory_usage")

# # # # # ---------------------------------------------------
# # # # # Write streaming output
# # # # # ---------------------------------------------------

# # # # query = aggregated.writeStream \
# # # #     .format("parquet") \
# # # #     .option("path", "/app/data/processed/stream") \
# # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # #     .outputMode("append") \
# # # #     .start()

# # # # print("Streaming job started")
# # # # print("Writing parquet to /app/data/processed/stream")

# # # # # ---------------------------------------------------
# # # # # BATCH AGGREGATION JOB
# # # # # ---------------------------------------------------

# # # # def run_batch():

# # # #     print("Running batch aggregation...")

# # # #     try:

# # # #         df = spark.read.parquet("/app/data/processed/stream")

# # # #         result = df.groupBy("device_id") \
# # # #             .avg("avg(cpu_usage)", "avg(memory_usage)")

# # # #         result.write \
# # # #             .mode("overwrite") \
# # # #             .parquet("/app/data/processed/batch")

# # # #         print("Batch parquet written")

# # # #     except Exception as e:

# # # #         print("Waiting for streaming files...")
# # # #         print(e)

# # # # # ---------------------------------------------------
# # # # # Run batch every minute
# # # # # ---------------------------------------------------

# # # # while True:

# # # #     time.sleep(60)

# # # #     run_batch()

# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import col, window, rand

# # # spark = SparkSession.builder \
# # #     .appName("AtlasStreamingProcessor") \
# # #     .getOrCreate()

# # # spark.sparkContext.setLogLevel("WARN")

# # # print("====================================")
# # # print("ATLAS STREAMING PROCESSOR STARTED")
# # # print("====================================")

# # # # Simulated telemetry stream
# # # stream_df = spark.readStream \
# # #     .format("rate") \
# # #     .option("rowsPerSecond", 50) \
# # #     .load()

# # # telemetry = stream_df.select(
# # #     (col("value") % 100).alias("device_id"),
# # #     (rand()*100).alias("cpu_usage"),
# # #     (rand()*100).alias("memory_usage"),
# # #     col("timestamp")
# # # )

# # # aggregated = telemetry \
# # #     .withWatermark("timestamp", "1 minute") \
# # #     .groupBy(
# # #         window(col("timestamp"), "1 minute"),
# # #         col("device_id")
# # #     ) \
# # #     .avg("cpu_usage", "memory_usage")

# # # query = aggregated.writeStream \
# # #     .format("parquet") \
# # #     .option("path", "/app/data/processed/stream") \
# # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # #     .outputMode("append") \
# # #     .start()

# # # print("Streaming pipeline running...")

# # # query.awaitTermination()


# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, explode, window
# # from pyspark.sql.types import *

# # schema = StructType([
# #     StructField("devices", MapType(StringType(), StructType([
# #         StructField("device_id", StringType()),
# #         StructField("application_customer_id", StringType()),
# #         StructField("platform_customer_id", StringType()),
# #         StructField("report_type", StringType()),
# #         StructField("data", StructType([
# #             StructField("PowerDetail", ArrayType(StructType([
# #                 StructField("Time", StringType()),
# #                 StructField("Average", DoubleType()),
# #                 StructField("Minimum", DoubleType()),
# #                 StructField("Peak", DoubleType()),
# #                 StructField("CpuUtil", IntegerType()),
# #                 StructField("CpuWatts", IntegerType()),
# #                 StructField("GpuWatts", IntegerType()),
# #                 StructField("AmbTemp", DoubleType()),
# #                 StructField("is_fresh", BooleanType())
# #             ])))
# #         ]))
# #     ])))
# # ])

# # spark = SparkSession.builder \
# #     .appName("AtlasStreamingProcessor") \
# #     .getOrCreate()

# # spark.sparkContext.setLogLevel("WARN")

# # print("🚀 STREAMING STARTED")

# # # ----------------------------------
# # # READ JSON STREAM
# # # ----------------------------------
# # df = spark.readStream \
# #     .schema(schema) \
# #     .format("json") \
# #     .option("maxFilesPerTrigger", 1) \
# #     .load("/app/data/raw")

# # # ----------------------------------
# # # EXPLODE DEVICES
# # # ----------------------------------
# # devices = df.selectExpr("explode(devices) as (key, value)") \
# #     .select("value.*")

# # # ----------------------------------
# # # EXPLODE POWER DETAIL
# # # ----------------------------------
# # flattened = devices \
# #     .select(
# #         col("device_id"),
# #         col("application_customer_id"),
# #         col("platform_customer_id"),
# #         col("report_type"),
# #         explode("data.PowerDetail").alias("pd")
# #     ) \
# #     .select(
# #         col("device_id"),
# #         col("application_customer_id"),
# #         col("platform_customer_id"),
# #         col("report_type"),
# #         col("pd.Time").alias("event_time"),
# #         col("pd.Average").alias("power"),
# #         col("pd.CpuUtil").alias("cpu_util"),
# #         col("pd.AmbTemp").alias("temp")
# #     )

# # # ----------------------------------
# # # CONVERT TIMESTAMP
# # # ----------------------------------
# # from pyspark.sql.functions import to_timestamp

# # flattened = flattened.withColumn(
# #     "event_time",
# #     to_timestamp("event_time")
# # )

# # # ----------------------------------
# # # WINDOW AGGREGATION
# # # ----------------------------------
# # aggregated = flattened \
# #     .withWatermark("event_time", "1 minute") \
# #     .groupBy(
# #         window(col("event_time"), "1 minute"),
# #         col("device_id")
# #     ) \
# #     .avg("power", "cpu_util", "temp")

# # # ----------------------------------
# # # WRITE OUTPUT
# # # ----------------------------------
# # query = aggregated.writeStream \
# #     .format("parquet") \
# #     .option("path", "/app/data/processed/stream") \
# #     .option("checkpointLocation", "/app/checkpoint/stream") \
# #     .outputMode("append") \
# #     .start()

# # query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, explode, window, to_timestamp
# from pyspark.sql.types import *

# spark = SparkSession.builder \
#     .appName("AtlasStreamingProcessor") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# spark.conf.set("spark.sql.shuffle.partitions", "4")

# print("🚀 STREAMING STARTED")

# # ---------------- SCHEMA ----------------
# schema = StructType([
#     StructField("application_customer_id", StringType()),
#     StructField("device_count", IntegerType()),
#     StructField("devices", MapType(StringType(), StructType([
#         StructField("device_id", StringType()),
#         StructField("platform_customer_id", StringType()),
#         StructField("application_customer_id", StringType()),
#         StructField("report_type", StringType()),
#         StructField("data", StructType([
#             StructField("PowerDetail", ArrayType(StructType([
#                 StructField("Time", StringType()),
#                 StructField("Average", DoubleType()),
#                 StructField("CpuUtil", IntegerType()),
#                 StructField("AmbTemp", DoubleType()),
#                 StructField("Minimum", DoubleType()),
#                 StructField("Peak", DoubleType()),
#                 StructField("is_fresh", BooleanType())
#             ])))
#         ]))
#     ])))
# ])

# # ---------------- READ STREAM ----------------
# df = spark.readStream \
#     .schema(schema) \
#     .format("json") \
#     .option("maxFilesPerTrigger", 1) \
#     .load("/app/data/raw")

# # ---------------- FLATTEN ----------------
# devices = df.selectExpr("explode(devices) as (k, v)").select("v.*")

# flattened = devices \
#     .select(
#         col("device_id"),
#         explode("data.PowerDetail").alias("pd")
#     ) \
#     .select(
#         col("device_id"),
#         to_timestamp("pd.Time").alias("event_time"),
#         col("pd.Average").alias("power"),
#         col("pd.CpuUtil").alias("cpu"),
#         col("pd.AmbTemp").alias("temp"),
#         col("pd.is_fresh")
#     )

# # ---------------- WINDOW ----------------
# agg = flattened \
#     .withWatermark("event_time", "2 minutes") \
#     .groupBy(
#         window(col("event_time"), "3 minutes"),
#         col("device_id")
#     ) \
#     .avg("power", "cpu", "temp")

# # ---------------- WRITE ----------------
# query = agg.writeStream \
#     .format("parquet") \
#     .option("path", "/app/data/processed/stream") \
#     .option("checkpointLocation", "/app/checkpoint/stream") \
#     .outputMode("append") \
#     .trigger(processingTime="3 minutes") \
#     .start()

# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, window, to_timestamp
from pyspark.sql.types import *
import time, json, os

spark = SparkSession.builder.appName("Streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.sql.shuffle.partitions", "9")
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")

print("🚀 STREAMING STARTED")

# ---------------- SCHEMA ----------------
schema = StructType([
    StructField("application_customer_id", StringType()),
    StructField("device_count", IntegerType()),
    StructField("devices", MapType(StringType(), StructType([
        StructField("device_id", StringType()),
        StructField("platform_customer_id", StringType()),
        StructField("application_customer_id", StringType()),
        StructField("report_type", StringType()),
        StructField("data", StructType([
            StructField("PowerDetail", ArrayType(StructType([
                StructField("Time", StringType()),
                StructField("Average", DoubleType()),
                StructField("CpuUtil", IntegerType()),
                StructField("AmbTemp", DoubleType()),
                StructField("Minimum", DoubleType()),
                StructField("Peak", DoubleType()),
                StructField("is_fresh", BooleanType())
            ])))
        ]))
    ])))
])

df = spark.readStream \
    .schema(schema) \
    .format("json") \
    .load("/app/data/raw")

devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

flat = devices.select(
    col("device_id"),
    explode("data.PowerDetail").alias("pd")
).select(
    col("device_id"),
    to_timestamp("pd.Time").alias("event_time"),
    col("pd.Average").alias("power"),
    col("pd.CpuUtil").alias("cpu"),
    col("pd.AmbTemp").alias("temp")
)

agg = flat.withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "3 minutes"), col("device_id")) \
    .avg("power", "cpu", "temp")

# ---------------- METRICS ----------------
METRICS = "/app/data/metrics/stream_metrics.json"
os.makedirs("/app/data/metrics", exist_ok=True)

def process_batch(df, batch_id):
    start = time.time()
    rows = df.count()

    df.write.mode("append").parquet("/app/data/processed/stream")

    duration = time.time() - start

    record = {
        "batch_id": batch_id,
        "rows": rows,
        "duration": duration,
        "throughput": rows/duration if duration else 0
    }

    with open(METRICS, "a") as f:
        f.write(json.dumps(record) + "\n")

    print(f"Batch {batch_id} | Rows {rows} | Time {duration}")

query = agg.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/app/checkpoint/stream") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()