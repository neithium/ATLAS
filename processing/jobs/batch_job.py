# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, explode, to_date, current_timestamp
# from pyspark.sql.types import *
# import time, json, os

# spark = SparkSession.builder.appName("Batch").getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# INPUT = "/app/data/raw"
# OUTPUT = "/app/data/processed/batch"
# METRICS = "/app/data/metrics/batch_metrics.json"

# os.makedirs("/app/data/metrics", exist_ok=True)

# print("🟡 BATCH STARTED")

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

# processed_days = set()
# run_id = 0

# # ---------------- LOOP ----------------
# while True:
#     try:
#         df = spark.read.schema(schema).json(INPUT)

#         if df.rdd.isEmpty():
#             time.sleep(60)
#             continue

#         # -------- FLATTEN --------
#         devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

#         flat = devices.select(
#             col("device_id"),
#             explode("data.PowerDetail").alias("pd")
#         ).select(
#             col("device_id"),
#             to_date("pd.Time").alias("event_date"),
#             col("pd.Average").alias("power"),
#             col("pd.CpuUtil").alias("cpu"),
#             col("pd.AmbTemp").alias("temp")
#         )

#         # -------- GET ALL DAYS --------
#         all_days = [r[0] for r in flat.select("event_date").distinct().collect()]

#         if not all_days:
#             time.sleep(60)
#             continue

#         # -------- FIND MAX DAY (LATEST DAY) --------
#         max_day = max(all_days)

#         # -------- PROCESS ONLY COMPLETED DAYS --------
#         days_to_process = [
#             day for day in all_days
#             if day < max_day and day not in processed_days
#         ]

#         for day in sorted(days_to_process):

#             print(f"🔥 Processing Day: {day}")
#             start = time.time()

#             daily_df = flat.filter(col("event_date") == day)

#             # -------- AGGREGATION (FIXED) --------
#             result = (
#                 daily_df.groupBy("device_id", "event_date")
#                 .avg("power", "cpu", "temp")
#                 .withColumn("batch_time", current_timestamp())
#             )

#             # -------- WRITE (EVENT-TIME PARTITIONED) --------
#             # result.write.mode("append") \
#             #     .partitionBy("event_date") \
#             #     .parquet(OUTPUT)
#             result.write.mode("append").parquet(OUTPUT)

#             duration = time.time() - start
#             rows = daily_df.count()

#             # -------- METRICS --------
#             with open(METRICS, "a") as f:
#                 f.write(json.dumps({
#                     "run_id": run_id,
#                     "event_date": str(day),
#                     "rows": rows,
#                     "duration": duration,
#                     "throughput": rows / duration if duration else 0
#                 }) + "\n")

#             print(f"✅ Batch {run_id} done")

#             processed_days.add(day)
#             run_id += 1

#         # -------- WAIT --------
#         time.sleep(60)

#     except Exception as e:
#         print("⚠️", e)
#         time.sleep(60)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, when
from pyspark.sql.types import *
import time, json, os

spark = SparkSession.builder.appName("Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

INPUT = "/app/data/raw"
OUTPUT = "/app/data/processed/batch"
METRICS = "/app/data/metrics/batch_metrics.json"

os.makedirs("/app/data/metrics", exist_ok=True)

print("🟡 BATCH STARTED")

# ---- Explicit schema ----
schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("cpu", IntegerType()),
    StructField("mem", IntegerType())
])

processed_days = set()
run_id = 0

def ensure_scalar(df, c):
    """If column c is accidentally read as array, pick first element."""
    dt = dict(df.dtypes).get(c)
    # dt like 'array<string>' or 'string'
    if dt and dt.startswith("array"):
        return df.withColumn(c, col(c).getItem(0))
    return df

while True:
    try:
        if not os.path.exists(INPUT) or not os.listdir(INPUT):
            print("⏳ Waiting for data...")
            time.sleep(5)
            continue

        # ---- Read with schema (no inference) ----
        df = spark.read.schema(schema).json(INPUT)

        if df.rdd.isEmpty():
            time.sleep(5)
            continue

        # ---- HARDEN against array/list columns ----
        for c in ["device_id", "timestamp", "cpu", "mem"]:
            df = ensure_scalar(df, c)

        # ---- Clean + cast ----
        df = df.select(
            col("device_id").cast("string").alias("device_id"),
            col("timestamp").cast("string").alias("timestamp"),
            col("cpu").cast("int").alias("cpu"),
            col("mem").cast("int").alias("mem")
        ).where(
            col("device_id").isNotNull() &
            col("timestamp").isNotNull()
        )

        flat = df.select(
            col("device_id"),
            to_date(col("timestamp")).alias("event_date"),
            col("cpu"),
            col("mem")
        )

        all_days = [r[0] for r in flat.select("event_date").distinct().collect()]
        if not all_days:
            time.sleep(5)
            continue

        max_day = max(all_days)

        days_to_process = [
            d for d in all_days
            if d < max_day and d not in processed_days
        ]

        for day in sorted(days_to_process):
            print(f"🔥 Processing Day: {day}")

            start = time.time()

            daily = flat.filter(col("event_date") == day)

            result = daily.groupBy("device_id", "event_date").agg(
                avg("cpu").alias("avg_cpu"),
                avg("mem").alias("avg_mem"),
                count("*").alias("num_records")
            )

            result.write.mode("append").parquet(OUTPUT)

            duration = time.time() - start
            rows = daily.count()

            with open(METRICS, "a") as f:
                f.write(json.dumps({
                    "run_id": run_id,
                    "event_date": str(day),
                    "rows": rows,
                    "duration": duration,
                    "throughput": rows / duration if duration else 0
                }) + "\n")

            print(f"✅ Batch {run_id} done")

            processed_days.add(day)
            run_id += 1

        time.sleep(60)

    except Exception as e:
        print("⚠️ Batch error:", e)
        time.sleep(5)