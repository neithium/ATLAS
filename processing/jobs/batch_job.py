# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, avg, count
# from pyspark.sql.types import *
# import time, json, os
# import logging

# # ---------------- LOGGING ----------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
# )
# logger = logging.getLogger("ATLAS")

# # ---------------- SPARK ----------------
# spark = SparkSession.builder.appName("Batch").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")

# logging.getLogger("py4j").setLevel(logging.ERROR)
# logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

# # ---------------- PATHS ----------------
# INPUT = "/app/data/raw"
# OUTPUT = "/app/data/processed/batch"
# METRICS = "/app/data/metrics/batch_metrics.json"

# os.makedirs("/app/data/metrics", exist_ok=True)

# logger.info("BATCH JOB STARTED")

# # ---------------- SCHEMA ----------------
# schema = StructType([
#     StructField("device_id", StringType()),
#     StructField("timestamp", StringType()),
#     StructField("cpu", IntegerType()),
#     StructField("mem", IntegerType())
# ])

# processed_days = set()
# run_id = 0

# def ensure_scalar(df, c):
#     dt = dict(df.dtypes).get(c)
#     if dt and dt.startswith("array"):
#         return df.withColumn(c, col(c).getItem(0))
#     return df

# # ---------------- LOOP ----------------
# while True:
#     try:
#         if not os.path.exists(INPUT) or not os.listdir(INPUT):
#             logger.debug("Waiting for data...")
#             time.sleep(5)
#             continue

#         df = spark.read.schema(schema).json(INPUT)

#         if df.rdd.isEmpty():
#             logger.debug("Empty dataframe")
#             time.sleep(5)
#             continue

#         for c in ["device_id", "timestamp", "cpu", "mem"]:
#             df = ensure_scalar(df, c)

#         df = df.select(
#             col("device_id").cast("string"),
#             col("timestamp").cast("string"),
#             col("cpu").cast("int"),
#             col("mem").cast("int")
#         ).where(col("device_id").isNotNull() & col("timestamp").isNotNull())

#         flat = df.select(
#             col("device_id"),
#             to_date("timestamp").alias("event_date"),
#             col("cpu"),
#             col("mem")
#         )

#         all_days = [r[0] for r in flat.select("event_date").distinct().collect()]
#         if not all_days:
#             time.sleep(5)
#             continue

#         max_day = max(all_days)

#         days_to_process = [d for d in all_days if d < max_day and d not in processed_days]

#         for day in sorted(days_to_process):
#             logger.info(f"Processing day={day}")
#             start = time.time()

#             daily = flat.filter(col("event_date") == day)

#             result = daily.groupBy("device_id", "event_date").agg(
#                 avg("cpu").alias("avg_cpu"),
#                 avg("mem").alias("avg_mem"),
#                 count("*").alias("num_records")
#             )

#             result.write.mode("append").parquet(OUTPUT)

#             duration = time.time() - start
#             rows = daily.count()

#             metrics_data = {
#                 "run_id": run_id,
#                 "event_date": str(day),
#                 "rows": rows,
#                 "duration": duration,
#                 "throughput": rows / duration if duration else 0
#             }

#             with open(METRICS, "a") as f:
#                 f.write(json.dumps(metrics_data) + "\n")

#             logger.info(f"Batch completed | {metrics_data}")

#             processed_days.add(day)
#             run_id += 1

#         time.sleep(60)

#     except Exception:
#         logger.exception("Batch failed")
#         time.sleep(5)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("Batch") \
    .master("local[6]") \
    .config("spark.sql.shuffle.partitions", "6") \
    .config("spark.default.parallelism", "6") \
    .getOrCreate()

INPUT = "/app/data/raw"
OUTPUT = "/app/data/processed/batch"

print("⏳ Waiting 24 minutes...")
time.sleep(1440)

df = spark.read.json(INPUT)

flat = df.withColumn("p", explode("data.PowerDetail"))

windowSpec = Window.partitionBy("device_id")

result = flat.select(
    "report_id",
    "device_id",
    "application_customer_id",
    "platform_customer_id",
    "status",
    "report_type",
    "error_reason",
    col("p.Average").alias("MetricValue"),
    "model",
    "tags",
    "location_state",
    "location_country",
    "processor_vendor",
    "server_generation",
    "location_id",
    "location_name",
    "location_city",
    "server_name",
    lit("power").alias("metric_id"),
    lit("cpu").alias("cpu_inventory"),
    lit("memory").alias("memory_inventory"),
    lit(0).alias("pcie_devices_count"),
    col("inventory_data.socket_count").alias("socket_count"),
    avg("p.Average").over(windowSpec).alias("avg_metric_value"),
    max("p.Average").over(windowSpec).alias("max_metric_value"),
    min("p.Average").over(windowSpec).alias("min_metric_value"),
    col("p.Time").alias("metric_time"),
    unix_timestamp("p.Time").cast("double").alias("datetime"),
    (unix_timestamp("p.Time")+60).cast("double").alias("timeRangeEnd"),
    col("p.AmbTemp").alias("amb_temp"),
    unix_timestamp().cast("double").alias("Insertiontime"),
    lit(0.5).alias("co2_factor"),
    lit(1.2).alias("energy_cost_factor"),
    col("p.Time").alias("max_metric_time"),
    to_date("p.Time").cast("string").alias("location_date"),
    to_date("p.Time").cast("string").alias("inventory_date")
)

result.coalesce(1).write.mode("overwrite").parquet(OUTPUT)

print("✅ Batch complete")