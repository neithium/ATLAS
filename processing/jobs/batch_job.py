# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import current_timestamp
# # # # import time, json, os
# # # # from pyspark.sql.functions import date_format
# # # # spark = SparkSession.builder.appName("Batch").getOrCreate()
# # # # spark.sparkContext.setLogLevel("WARN")

# # # # INPUT = "/app/data/processed/stream"
# # # # LATEST = "/app/data/processed/batch/latest"
# # # # HISTORY = "/app/data/processed/batch/history"
# # # # METRICS = "/app/data/metrics/batch_metrics.json"

# # # # os.makedirs("/app/data/metrics", exist_ok=True)

# # # # print("🟡 BATCH STARTED")

# # # # run_id = 0

# # # # while True:
# # # #     print("⏳ Waiting 6 minutes...")
# # # #     time.sleep(360)

# # # #     try:
# # # #         start = time.time()

# # # #         # ---------------- READ + OPTIMIZE ----------------
# # # #         df = spark.read.parquet(INPUT).repartition(32)

# # # #         df = df.filter("device_id IS NOT NULL")

# # # #         df = df.cache()
# # # #         rows = df.count()

# # # #         # ---------------- AGGREGATION ----------------
# # # #         result = df.groupBy("device_id") \
# # # #             .avg("avg(power)", "avg(cpu)", "avg(temp)")

# # # #         # ---------------- ADD TIMESTAMP ----------------
# # # #         result_with_time = result.withColumn("batch_time", current_timestamp())

# # # #         # # ---------------- WRITE LATEST (SNAPSHOT) ----------------
# # # #         # result_with_time.coalesce(1).write \
# # # #         #     .mode("overwrite") \
# # # #         #     .parquet(LATEST)

# # # #         # # ---------------- WRITE HISTORY (APPEND) ----------------
# # # #         # result_with_time.write \
# # # #         #     .mode("append") \
# # # #         #     .partitionBy("batch_time") \
# # # #         #     .parquet(HISTORY)



# # # #     result_with_time = result_with_time.withColumn(
# # # #         "batch_date", date_format("batch_time", "yyyy-MM-dd")
# # # #     )

# # # #     result_with_time.write \
# # # #         .mode("append") \
# # # #         .partitionBy("batch_date") \
# # # #         .parquet("/app/data/processed/batch")
# # # #         # ---------------- CLEANUP ----------------
# # # #         df.unpersist()

# # # #         duration = time.time() - start

# # # #         record = {
# # # #             "run_id": run_id,
# # # #             "rows": rows,
# # # #             "duration": duration,
# # # #             "throughput": rows / duration if duration else 0
# # # #         }

# # # #         with open(METRICS, "a") as f:
# # # #             f.write(json.dumps(record) + "\n")

# # # #         print(f"✅ Batch Run {run_id} complete | Rows: {rows} | Time: {duration:.2f}s")

# # # #         run_id += 1

# # # #     except Exception as e:
# # # #         print("⚠️ Waiting for stream...", e)

# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import current_timestamp, date_format
# # # import time, json, os

# # # spark = SparkSession.builder.appName("Batch").getOrCreate()
# # # spark.sparkContext.setLogLevel("WARN")

# # # INPUT = "/app/data/processed/stream"
# # # OUTPUT = "/app/data/processed/batch"
# # # METRICS = "/app/data/metrics/batch_metrics.json"

# # # os.makedirs("/app/data/metrics", exist_ok=True)

# # # print("🟡 BATCH STARTED")

# # # run_id = 0

# # # while True:
# # #     print("⏳ Waiting 24 hours...")
# # #     time.sleep(86400)  # 24 hours

# # #     try:
# # #         start = time.time()

# # #         df = spark.read.parquet(INPUT).repartition(32)

# # #         df = df.filter("device_id IS NOT NULL")

# # #         df = df.cache()
# # #         rows = df.count()

# # #         # ---------------- AGGREGATION ----------------
# # #         result = df.groupBy("device_id") \
# # #             .avg("avg(power)", "avg(cpu)", "avg(temp)")

# # #         # ---------------- ADD TIME ----------------
# # #         result = result.withColumn("batch_time", current_timestamp())

# # #         result = result.withColumn(
# # #             "batch_date",
# # #             date_format("batch_time", "yyyy-MM-dd")
# # #         )

# # #         # ---------------- SINGLE SINK ----------------
# # #         result.write \
# # #             .mode("append") \
# # #             .partitionBy("batch_date") \
# # #             .parquet(OUTPUT)

# # #         df.unpersist()

# # #         duration = time.time() - start

# # #         record = {
# # #             "run_id": run_id,
# # #             "rows": rows,
# # #             "duration": duration,
# # #             "throughput": rows / duration if duration else 0
# # #         }

# # #         with open(METRICS, "a") as f:
# # #             f.write(json.dumps(record) + "\n")

# # #         print(f"✅ Batch {run_id} | Rows {rows} | Time {duration:.2f}s")

# # #         run_id += 1

# # #     except Exception as e:
# # #         print("⚠️ Waiting for stream...", e)
# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import current_timestamp, date_format
# # import time, json, os

# # spark = SparkSession.builder.appName("Batch").getOrCreate()
# # spark.sparkContext.setLogLevel("WARN")

# # INPUT = "/app/data/processed/stream"
# # OUTPUT = "/app/data/processed/batch"
# # METRICS = "/app/data/metrics/batch_metrics.json"

# # os.makedirs("/app/data/metrics", exist_ok=True)

# # print("🟡 BATCH STARTED")

# # run_id = 0

# # while True:
# #     print("⏳ Waiting 24 hours (REAL TIME)...")
# #     time.sleep(86400)

# #     try:
# #         start = time.time()

# #         df = spark.read.parquet(INPUT).repartition(32)
# #         df = df.filter("device_id IS NOT NULL")

# #         df = df.cache()
# #         rows = df.count()

# #         result = df.groupBy("device_id") \
# #             .avg("avg(power)", "avg(cpu)", "avg(temp)")

# #         result = result.withColumn("batch_time", current_timestamp())

# #         result = result.withColumn(
# #             "batch_date",
# #             date_format("batch_time", "yyyy-MM-dd")
# #         )

# #         result.write \
# #             .mode("append") \
# #             .partitionBy("batch_date") \
# #             .parquet(OUTPUT)

# #         df.unpersist()

# #         duration = time.time() - start

# #         record = {
# #             "run_id": run_id,
# #             "rows": rows,
# #             "duration": duration,
# #             "throughput": rows / duration if duration else 0
# #         }

# #         with open(METRICS, "a") as f:
# #             f.write(json.dumps(record) + "\n")

# #         print(f"✅ Batch {run_id} | Rows {rows} | Time {duration:.2f}s")

# #         run_id += 1

# #     except Exception as e:
# #         print("⚠️ Waiting for stream...", e)
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import max as spark_max, current_timestamp, date_format
# import time, json, os

# spark = SparkSession.builder.appName("Batch").getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# INPUT = "/app/data/processed/stream"
# OUTPUT = "/app/data/processed/batch"
# METRICS = "/app/data/metrics/batch_metrics.json"

# os.makedirs("/app/data/metrics", exist_ok=True)

# print("🟡 BATCH STARTED")

# last_day = None
# run_id = 0

# while True:
#     try:
#         df = spark.read.parquet(INPUT)

#         if df.count() == 0:
#             print("Waiting for data...")
#             time.sleep(60)
#             continue

#         max_time = df.select(spark_max("window.end")).collect()[0][0]
#         current_day = max_time.date()

#         if last_day is None:
#             last_day = current_day
#             print("Initialized batch day")
#             time.sleep(60)
#             continue

#         if current_day > last_day:
#             print("🔥 DAILY BATCH TRIGGERED")

#             start = time.time()

#             result = df.groupBy("device_id") \
#                 .avg("avg(power)", "avg(cpu)", "avg(temp)")

#             result = result.withColumn("batch_time", current_timestamp())
#             result = result.withColumn("batch_date", date_format("batch_time", "yyyy-MM-dd"))

#             result.write.mode("append").partitionBy("batch_date").parquet(OUTPUT)

#             duration = time.time() - start

#             record = {
#                 "run_id": run_id,
#                 "rows": df.count(),
#                 "duration": duration,
#                 "throughput": df.count() / duration if duration else 0
#             }

#             with open(METRICS, "a") as f:
#                 f.write(json.dumps(record) + "\n")

#             print(f"✅ Batch {run_id} done")

#             run_id += 1
#             last_day = current_day

#         else:
#             print("⏳ Waiting for next simulated day...")

#         time.sleep(60)

#     except Exception as e:
#         print("⚠️", e)
#         time.sleep(60)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date, current_timestamp, date_format
from pyspark.sql.types import *
import time, json, os

spark = SparkSession.builder.appName("Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

INPUT = "/app/data/raw"
OUTPUT = "/app/data/processed/batch"
METRICS = "/app/data/metrics/batch_metrics.json"

os.makedirs("/app/data/metrics", exist_ok=True)

print("🟡 BATCH STARTED")

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
                StructField("CpuUtil", LongType()),
                StructField("AmbTemp", DoubleType()),
                StructField("Minimum", DoubleType()),
                StructField("Peak", DoubleType()),
                StructField("is_fresh", BooleanType())
            ])))
        ]))
    ])))
])

processed_days = set()
run_id = 0

while True:
    try:
        df = spark.read.schema(schema).json(INPUT)

        devices = df.selectExpr("explode(devices) as (k,v)").select("v.*")

        flat = devices.select(
            col("device_id"),
            explode("data.PowerDetail").alias("pd")
        ).select(
            col("device_id"),
            to_date("pd.Time").alias("event_date"),
            col("pd.Average").alias("power"),
            col("pd.CpuUtil").alias("cpu"),
            col("pd.AmbTemp").alias("temp")
        )

        days = [r[0] for r in flat.select("event_date").distinct().collect()]

        for day in days:
            if day in processed_days:
                continue

            print(f"🔥 Processing Day: {day}")

            start = time.time()

            daily_df = flat.filter(col("event_date") == day)

            result = daily_df.groupBy("device_id").avg("power", "cpu", "temp")

            result = result.withColumn("batch_time", current_timestamp())
            result = result.withColumn("batch_date", date_format("batch_time", "yyyy-MM-dd"))

            result.write.mode("append").partitionBy("batch_date").parquet(OUTPUT)

            duration = time.time() - start

            with open(METRICS, "a") as f:
                f.write(json.dumps({
                    "run_id": run_id,
                    "rows": daily_df.count(),
                    "duration": duration,
                    "throughput": daily_df.count() / duration if duration else 0
                }) + "\n")

            print(f"✅ Batch {run_id} done")

            processed_days.add(day)
            run_id += 1

        time.sleep(60)

    except Exception as e:
        print("⚠️", e)
        time.sleep(60)