# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, explode, window, to_timestamp, from_json
# from pyspark.sql.types import *

# spark = SparkSession.builder \
#     .appName("KafkaStreaming") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# print("🚀 KAFKA STREAMING STARTED")

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
#                 StructField("CpuUtil", LongType()),
#                 StructField("AmbTemp", DoubleType()),
#                 StructField("Minimum", DoubleType()),
#                 StructField("Peak", DoubleType()),
#                 StructField("is_fresh", BooleanType())
#             ])))
#         ]))
#     ])))
# ])

# # ---------------- READ FROM KAFKA ----------------
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "broker1:9092") \
#     .option("subscribe", "raw-server-metrics") \
#     .option("startingOffsets", "latest") \
#     .option("failOnDataLoss", "false") \
#     .load()

# # ---------------- PARSE JSON ----------------
# json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# df = json_df.select(
#     from_json(col("value"), schema).alias("data")
# ).select("data.*")

# # ---------------- FLATTEN ----------------
# devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

# flat = devices.select(
#     col("device_id"),
#     explode("data.PowerDetail").alias("pd")
# ).filter(col("pd.is_fresh") == True).select(
#     col("device_id"),
#     to_timestamp("pd.Time").alias("event_time"),
#     col("pd.Average").alias("power"),
#     col("pd.CpuUtil").alias("cpu"),
#     col("pd.AmbTemp").alias("temp")
# )

# # ---------------- AGG ----------------
# agg = flat.withWatermark("event_time", "10 minutes") \
#     .groupBy(window(col("event_time"), "1 hour"), col("device_id")) \
#     .avg("power", "cpu", "temp")

# # ---------------- WRITE ----------------
# query = agg.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "/app/data/processed/kafka_stream") \
#     .option("checkpointLocation", "/app/checkpoint/kafka") \
#     .trigger(processingTime="5 minutes") \
#     .start()

# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaVirtualTimeStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 STREAMING STARTED")

schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("cpu", IntegerType()),
    StructField("mem", IntegerType())
])

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

query = final_df \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/app/data/processed/stream") \
    .option("checkpointLocation", "/app/checkpoint/stream") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()