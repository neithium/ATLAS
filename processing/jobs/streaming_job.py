# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import col, explode, window, to_timestamp
# # # # from pyspark.sql.types import *
# # # # import time, json, os

# # # # spark = SparkSession.builder.appName("Streaming").getOrCreate()
# # # # spark.sparkContext.setLogLevel("WARN")

# # # # spark.conf.set("spark.sql.shuffle.partitions", "32")
# # # # spark.conf.set("spark.sql.streaming.stateStore.providerClass",
# # # #                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")

# # # # print("🚀 STREAMING STARTED")

# # # # # ---------------- SCHEMA ----------------
# # # # schema = StructType([
# # # #     StructField("application_customer_id", StringType()),
# # # #     StructField("device_count", IntegerType()),
# # # #     StructField("devices", MapType(StringType(), StructType([
# # # #         StructField("device_id", StringType()),
# # # #         StructField("platform_customer_id", StringType()),
# # # #         StructField("application_customer_id", StringType()),
# # # #         StructField("report_type", StringType()),
# # # #         StructField("data", StructType([
# # # #             StructField("PowerDetail", ArrayType(StructType([
# # # #                 StructField("Time", StringType()),
# # # #                 StructField("Average", DoubleType()),
# # # #                 StructField("CpuUtil", IntegerType()),
# # # #                 StructField("AmbTemp", DoubleType()),
# # # #                 StructField("Minimum", DoubleType()),
# # # #                 StructField("Peak", DoubleType()),
# # # #                 StructField("is_fresh", BooleanType())
# # # #             ])))
# # # #         ]))
# # # #     ])))
# # # # ])

# # # # df = spark.readStream \
# # # #     .schema(schema) \
# # # #     .format("json") \
# # # #     .option("maxFilesPerTrigger", 200) \
# # # #     .load("/app/data/raw")

# # # # devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

# # # # flat = devices.select(
# # # #     col("device_id"),
# # # #     explode("data.PowerDetail").alias("pd")
# # # # ).select(
# # # #     col("device_id"),
# # # #     to_timestamp("pd.Time").alias("event_time"),
# # # #     col("pd.Average").alias("power"),
# # # #     col("pd.CpuUtil").alias("cpu"),
# # # #     col("pd.AmbTemp").alias("temp")
# # # # )

# # # # agg = flat.withWatermark("event_time", "1 minute") \
# # # #     .groupBy(window(col("event_time"), "3 minutes"), col("device_id")) \
# # # #     .avg("power", "cpu", "temp")

# # # # # ---------------- METRICS ----------------
# # # # METRICS = "/app/data/metrics/stream_metrics.json"
# # # # os.makedirs("/app/data/metrics", exist_ok=True)

# # # # def process_batch(df, batch_id):
# # # #     start = time.time()
# # # #     rows = df.count()

# # # #     df.write.mode("append").parquet("/app/data/processed/stream")

# # # #     duration = time.time() - start

# # # #     record = {
# # # #         "batch_id": batch_id,
# # # #         "rows": rows,
# # # #         "duration": duration,
# # # #         "throughput": rows/duration if duration else 0
# # # #     }

# # # #     with open(METRICS, "a") as f:
# # # #         f.write(json.dumps(record) + "\n")

# # # #     print(f"Batch {batch_id} | Rows {rows} | Time {duration}")

# # # # query = agg.writeStream \
# # # #     .foreachBatch(process_batch) \
# # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # #     .trigger(processingTime="30 seconds") \
# # # #     .start()

# # # # query.awaitTermination()
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import col, explode, window, to_timestamp
# # # from pyspark.sql.types import *
# # # import time, json, os

# # # spark = SparkSession.builder.appName("Streaming").getOrCreate()
# # # spark.sparkContext.setLogLevel("WARN")

# # # spark.conf.set("spark.sql.shuffle.partitions", "32")

# # # print("🚀 STREAMING STARTED")

# # # # ---------------- SCHEMA ----------------
# # # schema = StructType([
# # #     StructField("application_customer_id", StringType()),
# # #     StructField("device_count", IntegerType()),
# # #     StructField("devices", MapType(StringType(), StructType([
# # #         StructField("device_id", StringType()),
# # #         StructField("platform_customer_id", StringType()),
# # #         StructField("application_customer_id", StringType()),
# # #         StructField("report_type", StringType()),
# # #         StructField("data", StructType([
# # #             StructField("PowerDetail", ArrayType(StructType([
# # #                 StructField("Time", StringType()),
# # #                 StructField("Average", DoubleType()),
# # #                 StructField("CpuUtil", IntegerType()),
# # #                 StructField("AmbTemp", DoubleType()),
# # #                 StructField("Minimum", DoubleType()),
# # #                 StructField("Peak", DoubleType()),
# # #                 StructField("is_fresh", BooleanType())
# # #             ])))
# # #         ]))
# # #     ])))
# # # ])

# # # df = spark.readStream \
# # #     .schema(schema) \
# # #     .format("json") \
# # #     .option("maxFilesPerTrigger", 200) \
# # #     .load("/app/data/raw")

# # # # ---------------- FLATTEN + FILTER ----------------
# # # devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

# # # flat = devices.select(
# # #     col("device_id"),
# # #     explode("data.PowerDetail").alias("pd")
# # # ).filter(
# # #     col("pd.is_fresh") == True
# # # ).select(
# # #     col("device_id"),
# # #     to_timestamp("pd.Time").alias("event_time"),
# # #     col("pd.Average").alias("power"),
# # #     col("pd.CpuUtil").alias("cpu"),
# # #     col("pd.AmbTemp").alias("temp")
# # # )

# # # # ---------------- AGGREGATION ----------------
# # # agg = flat.withWatermark("event_time", "10 minutes") \
# # #     .groupBy(
# # #         window(col("event_time"), "1 hour"),
# # #         col("device_id")
# # #     ).avg("power", "cpu", "temp")

# # # # ---------------- METRICS ----------------
# # # METRICS = "/app/data/metrics/stream_metrics.json"
# # # os.makedirs("/app/data/metrics", exist_ok=True)

# # # def process_batch(df, batch_id):
# # #     start = time.time()

# # #     rows = df.count()

# # #     df.write.mode("append").parquet("/app/data/processed/stream")

# # #     duration = time.time() - start

# # #     record = {
# # #         "batch_id": batch_id,
# # #         "rows": rows,
# # #         "duration": duration,
# # #         "throughput": rows / duration if duration else 0
# # #     }

# # #     with open(METRICS, "a") as f:
# # #         f.write(json.dumps(record) + "\n")

# # #     print(f"✅ Batch {batch_id} | Rows {rows} | Time {duration:.2f}s")

# # # query = agg.writeStream \
# # #     .foreachBatch(process_batch) \
# # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # #     .trigger(processingTime="5 minutes") \
# # #     .start()

# # # query.awaitTermination()

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, explode, window, to_timestamp
# # from pyspark.sql.types import *
# # import time, json, os

# # spark = SparkSession.builder.appName("Streaming").getOrCreate()
# # spark.sparkContext.setLogLevel("WARN")

# # spark.conf.set("spark.sql.shuffle.partitions", "32")

# # print("🚀 STREAMING STARTED")

# # # ---------------- SCHEMA ----------------
# # schema = StructType([
# #     StructField("application_customer_id", StringType()),
# #     StructField("device_count", IntegerType()),
# #     StructField("devices", MapType(StringType(), StructType([
# #         StructField("device_id", StringType()),
# #         StructField("platform_customer_id", StringType()),
# #         StructField("application_customer_id", StringType()),
# #         StructField("report_type", StringType()),
# #         StructField("data", StructType([
# #             StructField("PowerDetail", ArrayType(StructType([
# #                 StructField("Time", StringType()),
# #                 StructField("Average", DoubleType()),
# #                 StructField("CpuUtil", IntegerType()),
# #                 StructField("AmbTemp", DoubleType()),
# #                 StructField("Minimum", DoubleType()),
# #                 StructField("Peak", DoubleType()),
# #                 StructField("is_fresh", BooleanType())
# #             ])))
# #         ]))
# #     ])))
# # ])

# # df = spark.readStream \
# #     .schema(schema) \
# #     .format("json") \
# #     .option("maxFilesPerTrigger", 200) \
# #     .load("/app/data/raw")

# # devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

# # flat = devices.select(
# #     col("device_id"),
# #     explode("data.PowerDetail").alias("pd")
# # ).filter(
# #     col("pd.is_fresh") == True
# # ).select(
# #     col("device_id"),
# #     to_timestamp("pd.Time").alias("event_time"),
# #     col("pd.Average").alias("power"),
# #     col("pd.CpuUtil").alias("cpu"),
# #     col("pd.AmbTemp").alias("temp")
# # )

# # # REAL WINDOW (UNCHANGED)
# # agg = flat.withWatermark("event_time", "10 minutes") \
# #     .groupBy(
# #         window(col("event_time"), "1 hour"),
# #         col("device_id")
# #     ).avg("power", "cpu", "temp")

# # # ---------------- METRICS ----------------
# # METRICS = "/app/data/metrics/stream_metrics.json"
# # os.makedirs("/app/data/metrics", exist_ok=True)

# # def process_batch(df, batch_id):
# #     start = time.time()
# #     rows = df.count()

# #     df.write.mode("append").parquet("/app/data/processed/stream")

# #     duration = time.time() - start

# #     record = {
# #         "batch_id": batch_id,
# #         "rows": rows,
# #         "duration": duration,
# #         "throughput": rows / duration if duration else 0
# #     }

# #     with open(METRICS, "a") as f:
# #         f.write(json.dumps(record) + "\n")

# #     print(f"✅ Batch {batch_id} | Rows {rows} | Time {duration:.2f}s")

# # query = agg.writeStream \
# #     .foreachBatch(process_batch) \
# #     .option("checkpointLocation", "/app/checkpoint/stream") \
# #     .trigger(processingTime="5 minutes") \
# #     .start()

# # query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, explode, window, to_timestamp
# from pyspark.sql.types import *
# import time, json, os

# spark = SparkSession.builder.appName("Streaming").getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# spark.conf.set("spark.sql.shuffle.partitions", "32")

# print("🚀 STREAMING STARTED")

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

# df = spark.readStream \
#     .schema(schema) \
#     .format("json") \
#     .option("maxFilesPerTrigger", 200) \
#     .load("/app/data/raw")

# devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

# flat = devices.select(
#     col("device_id"),
#     explode("data.PowerDetail").alias("pd")
# ).filter(
#     col("pd.is_fresh") == True
# ).select(
#     col("device_id"),
#     to_timestamp("pd.Time").alias("event_time"),
#     col("pd.Average").alias("power"),
#     col("pd.CpuUtil").alias("cpu"),
#     col("pd.AmbTemp").alias("temp")
# )

# agg = flat.withWatermark("event_time", "10 minutes") \
#     .groupBy(
#         window(col("event_time"), "1 hour"),
#         col("device_id")
#     ).avg("power", "cpu", "temp")

# METRICS = "/app/data/metrics/stream_metrics.json"
# os.makedirs("/app/data/metrics", exist_ok=True)

# def process_batch(df, batch_id):
#     start = time.time()
#     rows = df.count()

#     df.write.mode("append").parquet("/app/data/processed/stream")

#     duration = time.time() - start

#     record = {
#         "batch_id": batch_id,
#         "rows": rows,
#         "duration": duration,
#         "throughput": rows / duration if duration else 0
#     }

#     with open(METRICS, "a") as f:
#         f.write(json.dumps(record) + "\n")

#     print(f"✅ Stream Batch {batch_id} | Rows {rows}")

# query = agg.writeStream \
#     .foreachBatch(process_batch) \
#     .option("checkpointLocation", "/app/checkpoint/stream") \
#     .trigger(processingTime="5 minutes") \
#     .start()

# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, window, to_timestamp
from pyspark.sql.types import *
import time, json, os

spark = SparkSession.builder.appName("Streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.sql.shuffle.partitions", "32")

print("🚀 STREAMING STARTED")

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
                StructField("CpuUtil", LongType()),  # FIXED
                StructField("AmbTemp", DoubleType()),
                StructField("Minimum", DoubleType()),
                StructField("Peak", DoubleType()),
                StructField("is_fresh", BooleanType())
            ])))
        ]))
    ])))
])

df = spark.readStream.schema(schema).json("/app/data/raw")

devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

flat = devices.select(
    col("device_id"),
    explode("data.PowerDetail").alias("pd")
).filter(col("pd.is_fresh") == True).select(
    col("device_id"),
    to_timestamp("pd.Time").alias("event_time"),
    col("pd.Average").alias("power"),
    col("pd.CpuUtil").alias("cpu"),
    col("pd.AmbTemp").alias("temp")
)

agg = flat.withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "1 hour"), col("device_id")) \
    .avg("power", "cpu", "temp")

METRICS = "/app/data/metrics/stream_metrics.json"
os.makedirs("/app/data/metrics", exist_ok=True)

def process_batch(df, batch_id):
    start = time.time()
    rows = df.count()

    df.write.mode("append").parquet("/app/data/processed/stream")

    duration = time.time() - start

    with open(METRICS, "a") as f:
        f.write(json.dumps({
            "batch_id": batch_id,
            "rows": rows,
            "duration": duration,
            "throughput": rows / duration if duration else 0
        }) + "\n")

    print(f"✅ Stream Batch {batch_id} | Rows {rows}")

query = agg.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/app/checkpoint/stream") \
    .trigger(processingTime="5 minutes") \
    .start()

query.awaitTermination()