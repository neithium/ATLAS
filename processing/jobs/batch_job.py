# # # # # from pyspark.sql import SparkSession
# # # # # from pyspark.sql.functions import col, to_date, avg, count
# # # # # from pyspark.sql.types import *
# # # # # import time, json, os
# # # # # import logging

# # # # # # ---------------- LOGGING ----------------
# # # # # logging.basicConfig(
# # # # #     level=logging.INFO,
# # # # #     format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
# # # # # )
# # # # # logger = logging.getLogger("ATLAS")

# # # # # # ---------------- SPARK ----------------
# # # # # spark = SparkSession.builder.appName("Batch").getOrCreate()
# # # # # spark.sparkContext.setLogLevel("ERROR")

# # # # # logging.getLogger("py4j").setLevel(logging.ERROR)
# # # # # logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

# # # # # # ---------------- PATHS ----------------
# # # # # INPUT = "/app/data/raw"
# # # # # OUTPUT = "/app/data/processed/batch"
# # # # # METRICS = "/app/data/metrics/batch_metrics.json"

# # # # # os.makedirs("/app/data/metrics", exist_ok=True)

# # # # # logger.info("BATCH JOB STARTED")

# # # # # # ---------------- SCHEMA ----------------
# # # # # schema = StructType([
# # # # #     StructField("device_id", StringType()),
# # # # #     StructField("timestamp", StringType()),
# # # # #     StructField("cpu", IntegerType()),
# # # # #     StructField("mem", IntegerType())
# # # # # ])

# # # # # processed_days = set()
# # # # # run_id = 0

# # # # # def ensure_scalar(df, c):
# # # # #     dt = dict(df.dtypes).get(c)
# # # # #     if dt and dt.startswith("array"):
# # # # #         return df.withColumn(c, col(c).getItem(0))
# # # # #     return df

# # # # # # ---------------- LOOP ----------------
# # # # # while True:
# # # # #     try:
# # # # #         if not os.path.exists(INPUT) or not os.listdir(INPUT):
# # # # #             logger.debug("Waiting for data...")
# # # # #             time.sleep(5)
# # # # #             continue

# # # # #         df = spark.read.schema(schema).json(INPUT)

# # # # #         if df.rdd.isEmpty():
# # # # #             logger.debug("Empty dataframe")
# # # # #             time.sleep(5)
# # # # #             continue

# # # # #         for c in ["device_id", "timestamp", "cpu", "mem"]:
# # # # #             df = ensure_scalar(df, c)

# # # # #         df = df.select(
# # # # #             col("device_id").cast("string"),
# # # # #             col("timestamp").cast("string"),
# # # # #             col("cpu").cast("int"),
# # # # #             col("mem").cast("int")
# # # # #         ).where(col("device_id").isNotNull() & col("timestamp").isNotNull())

# # # # #         flat = df.select(
# # # # #             col("device_id"),
# # # # #             to_date("timestamp").alias("event_date"),
# # # # #             col("cpu"),
# # # # #             col("mem")
# # # # #         )

# # # # #         all_days = [r[0] for r in flat.select("event_date").distinct().collect()]
# # # # #         if not all_days:
# # # # #             time.sleep(5)
# # # # #             continue

# # # # #         max_day = max(all_days)

# # # # #         days_to_process = [d for d in all_days if d < max_day and d not in processed_days]

# # # # #         for day in sorted(days_to_process):
# # # # #             logger.info(f"Processing day={day}")
# # # # #             start = time.time()

# # # # #             daily = flat.filter(col("event_date") == day)

# # # # #             result = daily.groupBy("device_id", "event_date").agg(
# # # # #                 avg("cpu").alias("avg_cpu"),
# # # # #                 avg("mem").alias("avg_mem"),
# # # # #                 count("*").alias("num_records")
# # # # #             )

# # # # #             result.write.mode("append").parquet(OUTPUT)

# # # # #             duration = time.time() - start
# # # # #             rows = daily.count()

# # # # #             metrics_data = {
# # # # #                 "run_id": run_id,
# # # # #                 "event_date": str(day),
# # # # #                 "rows": rows,
# # # # #                 "duration": duration,
# # # # #                 "throughput": rows / duration if duration else 0
# # # # #             }

# # # # #             with open(METRICS, "a") as f:
# # # # #                 f.write(json.dumps(metrics_data) + "\n")

# # # # #             logger.info(f"Batch completed | {metrics_data}")

# # # # #             processed_days.add(day)
# # # # #             run_id += 1

# # # # #         time.sleep(60)

# # # # #     except Exception:
# # # # #         logger.exception("Batch failed")
# # # # #         time.sleep(5)
# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import *
# # # # from pyspark.sql.window import Window
# # # # import time

# # # # spark = SparkSession.builder \
# # # #     .appName("Batch") \
# # # #     .master("local[6]") \
# # # #     .config("spark.sql.shuffle.partitions", "6") \
# # # #     .config("spark.default.parallelism", "6") \
# # # #     .getOrCreate()

# # # # INPUT = "/app/data/raw"
# # # # OUTPUT = "/app/data/processed/batch"

# # # # print("⏳ Waiting 24 minutes...")
# # # # time.sleep(1440)

# # # # df = spark.read.json(INPUT)

# # # # flat = df.withColumn("p", explode("data.PowerDetail"))

# # # # windowSpec = Window.partitionBy("device_id")

# # # # result = flat.select(
# # # #     "report_id",
# # # #     "device_id",
# # # #     "application_customer_id",
# # # #     "platform_customer_id",
# # # #     "status",
# # # #     "report_type",
# # # #     "error_reason",
# # # #     col("p.Average").alias("MetricValue"),
# # # #     "model",
# # # #     "tags",
# # # #     "location_state",
# # # #     "location_country",
# # # #     "processor_vendor",
# # # #     "server_generation",
# # # #     "location_id",
# # # #     "location_name",
# # # #     "location_city",
# # # #     "server_name",
# # # #     lit("power").alias("metric_id"),
# # # #     lit("cpu").alias("cpu_inventory"),
# # # #     lit("memory").alias("memory_inventory"),
# # # #     lit(0).alias("pcie_devices_count"),
# # # #     col("inventory_data.socket_count").alias("socket_count"),
# # # #     avg("p.Average").over(windowSpec).alias("avg_metric_value"),
# # # #     max("p.Average").over(windowSpec).alias("max_metric_value"),
# # # #     min("p.Average").over(windowSpec).alias("min_metric_value"),
# # # #     col("p.Time").alias("metric_time"),
# # # #     unix_timestamp("p.Time").cast("double").alias("datetime"),
# # # #     (unix_timestamp("p.Time")+60).cast("double").alias("timeRangeEnd"),
# # # #     col("p.AmbTemp").alias("amb_temp"),
# # # #     unix_timestamp().cast("double").alias("Insertiontime"),
# # # #     lit(0.5).alias("co2_factor"),
# # # #     lit(1.2).alias("energy_cost_factor"),
# # # #     col("p.Time").alias("max_metric_time"),
# # # #     to_date("p.Time").cast("string").alias("location_date"),
# # # #     to_date("p.Time").cast("string").alias("inventory_date")
# # # # )

# # # # result.coalesce(1).write.mode("overwrite").parquet(OUTPUT)

# # # # print("✅ Batch complete")
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import col, lit
# # # from pyspark.sql.types import *

# # # # ✅ INPUT SCHEMA (FIXED)
# # # input_schema = StructType([
# # #     StructField("data", StructType([
# # #         StructField("Id", StringType(), True),
# # #         StructField("Average", DoubleType(), True),
# # #         StructField("PowerDetail", ArrayType(StructType([
# # #             StructField("Time", StringType(), True),
# # #             StructField("CpuUtil", LongType(), True)
# # #         ])), True)
# # #     ]), True),

# # #     StructField("model", StringType(), True),
# # #     StructField("tags", StringType(), True),
# # #     StructField("status", BooleanType(), True),
# # #     StructField("device_id", StringType(), True),
# # #     StructField("report_id", StringType(), True),
# # #     StructField("created_at", StringType(), True),

# # #     StructField("location_id", StringType(), True),
# # #     StructField("report_type", StringType(), True),
# # #     StructField("server_name", StringType(), True),
# # #     StructField("error_reason", StringType(), True),

# # #     StructField("location_city", StringType(), True),
# # #     StructField("location_name", StringType(), True),
# # #     StructField("location_state", StringType(), True),
# # #     StructField("location_country", StringType(), True),

# # #     StructField("processor_vendor", StringType(), True),
# # #     StructField("server_generation", StringType(), True),

# # #     StructField("platform_customer_id", StringType(), True),
# # #     StructField("application_customer_id", StringType(), True),

# # #     StructField("metric_type", StringType(), True),

# # #     StructField("inventory_data", StructType([
# # #         StructField("cpu_count", IntegerType(), True),
# # #         StructField("socket_count", LongType(), True)
# # #     ]), True)
# # # ])


# # # def create_spark():
# # #     return SparkSession.builder \
# # #         .appName("Batch-Pipeline") \
# # #         .master("local[6]") \
# # #         .config("spark.sql.shuffle.partitions", "6") \
# # #         .config("spark.default.parallelism", "6") \
# # #         .config("spark.driver.memory", "3g") \
# # #         .config("spark.hadoop.fs.s3a.endpoint", "http://ingestion:9000") \
# # #         .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
# # #         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
# # #         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
# # #         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
# # #         .getOrCreate()


# # # def run_batch():
# # #     spark = create_spark()
# # #     spark.sparkContext.setLogLevel("ERROR")

# # #     print("🚀 Starting Batch Job...")

# # #     # ✅ READ ONLY LATEST PARTITION (VERY IMPORTANT)
# # #     df = spark.read.schema(input_schema) \
# # #         .parquet("s3a://telemetry-raw/production/year=2026/month=05/day=03/")

# # #     print("✅ Data loaded")

# # #     # ✅ CONTROL PARALLELISM (IMPORTANT FOR STABILITY)
# # #     df = df.repartition(6)

# # #     # ✅ TRANSFORM
# # #     df_out = df.select(
# # #         col("report_id"),
# # #         col("device_id"),
# # #         col("application_customer_id"),
# # #         col("platform_customer_id"),
# # #         col("status"),
# # #         col("report_type"),
# # #         col("error_reason"),

# # #         col("data.Average").alias("MetricValue"),

# # #         col("model"),
# # #         col("tags"),
# # #         col("location_state"),
# # #         col("location_country"),
# # #         col("processor_vendor"),
# # #         col("server_generation"),
# # #         col("location_id"),
# # #         col("location_name"),
# # #         col("location_city"),
# # #         col("server_name"),

# # #         lit(None).cast("string").alias("metric_id"),

# # #         lit(None).cast("string").alias("cpu_inventory"),
# # #         lit(None).cast("string").alias("memory_inventory"),

# # #         lit(None).cast("int").alias("pcie_devices_count"),
# # #         col("inventory_data.socket_count").alias("socket_count"),

# # #         col("data.Average").alias("avg_metric_value"),
# # #         lit(None).cast("double").alias("max_metric_value"),
# # #         lit(None).cast("double").alias("min_metric_value"),

# # #         col("created_at").alias("metric_time"),

# # #         lit(None).cast("double").alias("datetime"),
# # #         lit(None).cast("double").alias("timeRangeEnd"),

# # #         lit(None).cast("double").alias("amb_temp"),
# # #         lit(None).cast("double").alias("Insertiontime"),
# # #         lit(None).cast("double").alias("co2_factor"),
# # #         lit(None).cast("double").alias("energy_cost_factor"),

# # #         lit(None).cast("string").alias("max_metric_time"),
# # #         lit(None).cast("string").alias("location_date"),
# # #         lit(None).cast("string").alias("inventory_date")
# # #     )

# # #     # ✅ WRITE OUTPUT (CONTROL FILE COUNT)
# # #     # output_path = "/opt/spark-data/processed/batch/"
# # #     output_path = "/app/data/processed/batch/"

# # #     df_out.coalesce(2).write \
# # #         .mode("overwrite") \
# # #         .parquet(output_path)

# # #     print(f"✅ Batch Output Written to {output_path}")

# # #     spark.stop()


# # # if __name__ == "__main__":
# # #     run_batch()
# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, lit
# # from pyspark.sql.types import *

# # # ✅ INPUT SCHEMA (FIXED)
# # input_schema = StructType([
# #     StructField("data", StructType([
# #         StructField("Id", StringType(), True),
# #         StructField("Average", DoubleType(), True),
# #         StructField("PowerDetail", ArrayType(StructType([
# #             StructField("Time", StringType(), True),
# #             StructField("CpuUtil", LongType(), True)
# #         ])), True)
# #     ]), True),

# #     StructField("model", StringType(), True),
# #     StructField("tags", StringType(), True),
# #     StructField("status", BooleanType(), True),
# #     StructField("device_id", StringType(), True),
# #     StructField("report_id", StringType(), True),
# #     StructField("created_at", StringType(), True),

# #     StructField("location_id", StringType(), True),
# #     StructField("report_type", StringType(), True),
# #     StructField("server_name", StringType(), True),
# #     StructField("error_reason", StringType(), True),

# #     StructField("location_city", StringType(), True),
# #     StructField("location_name", StringType(), True),
# #     StructField("location_state", StringType(), True),
# #     StructField("location_country", StringType(), True),

# #     StructField("processor_vendor", StringType(), True),
# #     StructField("server_generation", StringType(), True),

# #     StructField("platform_customer_id", StringType(), True),
# #     StructField("application_customer_id", StringType(), True),

# #     StructField("metric_type", StringType(), True),

# #     StructField("inventory_data", StructType([
# #         StructField("cpu_count", IntegerType(), True),
# #         StructField("socket_count", LongType(), True)
# #     ]), True)
# # ])


# # def create_spark():
# #     return SparkSession.builder \
# #         .appName("Batch-Pipeline") \
# #         .master("local[6]") \
# #         .config("spark.sql.shuffle.partitions", "6") \
# #         .config("spark.default.parallelism", "6") \
# #         .config("spark.driver.memory", "3g") \
# #         .config("spark.hadoop.fs.s3a.endpoint", "http://ingestion:9000") \
# #         .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
# #         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
# #         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
# #         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
# #         .getOrCreate()


# # def run_batch():
# #     spark = create_spark()
# #     spark.sparkContext.setLogLevel("ERROR")

# #     print("🚀 Starting Batch Job...")

# #     # ✅ READ ONLY LATEST PARTITION (VERY IMPORTANT)
# #     df = spark.read.schema(input_schema) \
# #         .parquet("s3a://telemetry-raw/production/year=2026/month=05/day=03/")

# #     print("✅ Data loaded")

# #     # ✅ CONTROL PARALLELISM (IMPORTANT FOR STABILITY)
# #     df = df.repartition(6)

# #     # ✅ TRANSFORM
# #     df_out = df.select(
# #         col("report_id"),
# #         col("device_id"),
# #         col("application_customer_id"),
# #         col("platform_customer_id"),
# #         col("status"),
# #         col("report_type"),
# #         col("error_reason"),

# #         col("data.Average").alias("MetricValue"),

# #         col("model"),
# #         col("tags"),
# #         col("location_state"),
# #         col("location_country"),
# #         col("processor_vendor"),
# #         col("server_generation"),
# #         col("location_id"),
# #         col("location_name"),
# #         col("location_city"),
# #         col("server_name"),

# #         lit(None).cast("string").alias("metric_id"),

# #         lit(None).cast("string").alias("cpu_inventory"),
# #         lit(None).cast("string").alias("memory_inventory"),

# #         lit(None).cast("int").alias("pcie_devices_count"),
# #         col("inventory_data.socket_count").alias("socket_count"),

# #         col("data.Average").alias("avg_metric_value"),
# #         lit(None).cast("double").alias("max_metric_value"),
# #         lit(None).cast("double").alias("min_metric_value"),

# #         col("created_at").alias("metric_time"),

# #         lit(None).cast("double").alias("datetime"),
# #         lit(None).cast("double").alias("timeRangeEnd"),

# #         lit(None).cast("double").alias("amb_temp"),
# #         lit(None).cast("double").alias("Insertiontime"),
# #         lit(None).cast("double").alias("co2_factor"),
# #         lit(None).cast("double").alias("energy_cost_factor"),

# #         lit(None).cast("string").alias("max_metric_time"),
# #         lit(None).cast("string").alias("location_date"),
# #         lit(None).cast("string").alias("inventory_date")
# #     )

# #     # ✅ WRITE OUTPUT (CONTROL FILE COUNT)
# #     # output_path = "/opt/spark-data/processed/batch/"
# #     output_path = "/app/data/processed/batch/"

# #     df_out.coalesce(2).write \
# #         .mode("overwrite") \
# #         .parquet(output_path)

# #     print(f"✅ Batch Output Written to {output_path}")

# #     spark.stop()


# # if __name__ == "__main__":
# #     run_batch()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit
# from pyspark.sql.types import *

# # =========================================================
# # INPUT SCHEMA
# # =========================================================

# input_schema = StructType([

#     StructField("data", StructType([
#         StructField("Id", StringType(), True),

#         StructField("Average", DoubleType(), True),

#         StructField(
#             "PowerDetail",
#             ArrayType(
#                 StructType([
#                     StructField("Time", StringType(), True),
#                     StructField("CpuUtil", LongType(), True)
#                 ])
#             ),
#             True
#         )
#     ]), True),

#     StructField("model", StringType(), True),
#     StructField("tags", StringType(), True),
#     StructField("status", BooleanType(), True),

#     StructField("device_id", StringType(), True),
#     StructField("report_id", StringType(), True),
#     StructField("created_at", StringType(), True),

#     StructField("location_id", StringType(), True),
#     StructField("report_type", StringType(), True),
#     StructField("server_name", StringType(), True),
#     StructField("error_reason", StringType(), True),

#     StructField("location_city", StringType(), True),
#     StructField("location_name", StringType(), True),
#     StructField("location_state", StringType(), True),
#     StructField("location_country", StringType(), True),

#     StructField("processor_vendor", StringType(), True),
#     StructField("server_generation", StringType(), True),

#     StructField("platform_customer_id", StringType(), True),
#     StructField("application_customer_id", StringType(), True),

#     StructField("metric_type", StringType(), True),

#     StructField(
#         "inventory_data",
#         StructType([
#             StructField("cpu_count", IntegerType(), True),
#             StructField("socket_count", LongType(), True)
#         ]),
#         True
#     )
# ])

# # =========================================================
# # SPARK SESSION
# # =========================================================

# def create_spark():

#     return (
#         SparkSession.builder
#         .appName("Batch-Pipeline")

#         # local multicore execution
#         .master("local[6]")

#         # parallelism tuning
#         .config("spark.sql.shuffle.partitions", "6")
#         .config("spark.default.parallelism", "6")

#         # memory tuning
#         .config("spark.driver.memory", "3g")

#         .getOrCreate()
#     )

# # =========================================================
# # BATCH JOB
# # =========================================================

# def run_batch():

#     spark = create_spark()

#     spark.sparkContext.setLogLevel("ERROR")

#     print("🚀 Starting Batch Job...")

#     # =====================================================
#     # INPUT PATHS
#     # =====================================================

#     INPUT_PATH = "/app/data/raw"
#     OUTPUT_PATH = "/app/data/processed/batch"
#     ARCHIVE_PATH = "/app/data/archive"

#     # =====================================================
#     # READ RAW DATA
#     # =====================================================

#     df = (
#         spark.read
#         .schema(input_schema)
#         .json(INPUT_PATH)
#     )

#     print("✅ Raw data loaded")

#     # =====================================================
#     # CHECK EMPTY DATA
#     # =====================================================

#     if df.rdd.isEmpty():

#         print("⚠️ No data found in raw directory")

#         spark.stop()

#         return

#     # =====================================================
#     # REPARTITION FOR PARALLELISM
#     # =====================================================

#     df = df.repartition(6)

#     print("✅ Repartitioned dataframe")

#     # =====================================================
#     # TRANSFORMATIONS
#     # =====================================================

#     df_out = df.select(

#         col("report_id"),

#         col("device_id"),

#         col("application_customer_id"),

#         col("platform_customer_id"),

#         col("status"),

#         col("report_type"),

#         col("error_reason"),

#         col("data.Average")
#         .alias("MetricValue"),

#         col("model"),

#         col("tags"),

#         col("location_state"),

#         col("location_country"),

#         col("processor_vendor"),

#         col("server_generation"),

#         col("location_id"),

#         col("location_name"),

#         col("location_city"),

#         col("server_name"),

#         lit(None)
#         .cast("string")
#         .alias("metric_id"),

#         lit(None)
#         .cast("string")
#         .alias("cpu_inventory"),

#         lit(None)
#         .cast("string")
#         .alias("memory_inventory"),

#         lit(None)
#         .cast("int")
#         .alias("pcie_devices_count"),

#         col("inventory_data.socket_count")
#         .alias("socket_count"),

#         col("data.Average")
#         .alias("avg_metric_value"),

#         lit(None)
#         .cast("double")
#         .alias("max_metric_value"),

#         lit(None)
#         .cast("double")
#         .alias("min_metric_value"),

#         col("created_at")
#         .alias("metric_time"),

#         lit(None)
#         .cast("double")
#         .alias("datetime"),

#         lit(None)
#         .cast("double")
#         .alias("timeRangeEnd"),

#         lit(None)
#         .cast("double")
#         .alias("amb_temp"),

#         lit(None)
#         .cast("double")
#         .alias("Insertiontime"),

#         lit(None)
#         .cast("double")
#         .alias("co2_factor"),

#         lit(None)
#         .cast("double")
#         .alias("energy_cost_factor"),

#         lit(None)
#         .cast("string")
#         .alias("max_metric_time"),

#         lit(None)
#         .cast("string")
#         .alias("location_date"),

#         lit(None)
#         .cast("string")
#         .alias("inventory_date")
#     )

#     print("✅ Transformations completed")

#     # =====================================================
#     # WRITE BATCH OUTPUT
#     # =====================================================

#     (
#         df_out
#         .coalesce(2)
#         .write
#         .mode("overwrite")
#         .parquet(OUTPUT_PATH)
#     )

#     print(f"✅ Batch Output Written → {OUTPUT_PATH}")

#     # =====================================================
#     # OPTIONAL ARCHIVAL
#     # =====================================================

#     print(f"📦 Raw files can now be archived to → {ARCHIVE_PATH}")

#     # =====================================================
#     # STOP SPARK
#     # =====================================================

#     spark.stop()

#     print("✅ Batch Job Completed Successfully")

# # =========================================================
# # MAIN
# # =========================================================

# if __name__ == "__main__":

#     run_batch()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================================================
# INPUT SCHEMA
# =========================================================

input_schema = StructType([

    StructField("data", StructType([

        StructField("Id", StringType(), True),

        StructField("Average", DoubleType(), True),

        StructField("Maximum", DoubleType(), True),

        StructField("Minimum", DoubleType(), True),

        StructField("Name", StringType(), True),

        StructField(
            "PowerDetail",

            ArrayType(

                StructType([

                    StructField("AmbTemp", DoubleType(), True),

                    StructField("Average", DoubleType(), True),

                    StructField("CpuAvgFreq", LongType(), True),

                    StructField("CpuMax", LongType(), True),

                    StructField("CpuPwrSavLim", LongType(), True),

                    StructField("CpuUtil", LongType(), True),

                    StructField("CpuWatts", LongType(), True),

                    StructField("GpuWatts", LongType(), True),

                    StructField("Minimum", DoubleType(), True),

                    StructField("Peak", DoubleType(), True),

                    StructField("Time", StringType(), True),

                    StructField("is_fresh", BooleanType(), True)

                ])

            ),

            True
        )

    ]), True),

    StructField("model", StringType(), True),

    StructField("tags", StringType(), True),

    StructField("status", BooleanType(), True),

    StructField("device_id", StringType(), True),

    StructField("report_id", StringType(), True),

    StructField("created_at", StringType(), True),

    StructField("location_id", StringType(), True),

    StructField("report_type", StringType(), True),

    StructField("server_name", StringType(), True),

    StructField("error_reason", StringType(), True),

    StructField("location_city", StringType(), True),

    StructField("location_name", StringType(), True),

    StructField("location_state", StringType(), True),

    StructField("location_country", StringType(), True),

    StructField("processor_vendor", StringType(), True),

    StructField("server_generation", StringType(), True),

    StructField("platform_customer_id", StringType(), True),

    StructField("application_customer_id", StringType(), True),

    StructField("metric_type", StringType(), True),

    StructField(

        "inventory_data",

        StructType([

            StructField("cpu_count", LongType(), True),

            StructField("socket_count", LongType(), True),

            StructField(

                "cpu_inventory",

                ArrayType(

                    StructType([

                        StructField("model", StringType(), True),

                        StructField("speed", LongType(), True),

                        StructField("total_cores", LongType(), True)

                    ])

                ),

                True
            ),

            StructField(

                "memory_inventory",

                ArrayType(

                    StructType([

                        StructField("memory_size", LongType(), True),

                        StructField("operating_freq", LongType(), True),

                        StructField("memory_device_type", StringType(), True)

                    ])

                ),

                True
            )

        ]),

        True
    )
])

# =========================================================
# CREATE SPARK SESSION
# =========================================================

def create_spark():

    return (
        SparkSession.builder
        .appName("Batch-Pipeline")

        .master("local[*]")

        .config("spark.sql.shuffle.partitions", "8")

        .config("spark.default.parallelism", "8")

        .config("spark.driver.memory", "4g")

        .getOrCreate()
    )

# =========================================================
# RUN BATCH JOB
# =========================================================

def run_batch():

    spark = create_spark()

    spark.sparkContext.setLogLevel("ERROR")

    print("🚀 Starting Batch Job...")

    INPUT_PATH = "/app/data/raw/production"

    OUTPUT_PATH = "/app/data/processed/batch"

    # =====================================================
    # READ PARQUET
    # =====================================================

    df = (
        spark.read
        .schema(input_schema)
        .parquet(INPUT_PATH)
    )

    print("✅ Raw parquet loaded")

    # =====================================================
    # EMPTY CHECK
    # =====================================================

    if df.limit(1).count() == 0:

        print("⚠️ No parquet data found")

        spark.stop()

        return

    # =====================================================
    # REPARTITION
    # =====================================================

    df = df.repartition(8)

    print("✅ Repartition completed")

    # =====================================================
    # EXPLODE POWER DETAIL
    # =====================================================

    flat = df.withColumn(

        "power",

        explode("data.PowerDetail")
    )

    print("✅ PowerDetail exploded")

    # =====================================================
    # DEVICE LEVEL AGGREGATIONS
    # =====================================================

    agg_df = flat.groupBy(

        "device_id",

        "report_id",

        "application_customer_id",

        "platform_customer_id",

        "status",

        "report_type",

        "error_reason",

        "model",

        "tags",

        "location_state",

        "location_country",

        "processor_vendor",

        "server_generation",

        "location_id",

        "location_name",

        "location_city",

        "server_name"

    ).agg(

        avg("power.Average")
        .alias("avg_metric_value"),

        max("power.Average")
        .alias("max_metric_value"),

        min("power.Average")
        .alias("min_metric_value"),

        avg("power.AmbTemp")
        .alias("amb_temp"),

        avg("power.CpuWatts")
        .alias("avg_cpu_watts"),

        max("power.CpuWatts")
        .alias("max_cpu_watts"),

        avg("power.GpuWatts")
        .alias("avg_gpu_watts"),

        avg("power.Peak")
        .alias("avg_peak_power"),

        max("power.Peak")
        .alias("max_peak_power"),

        max("power.Time")
        .alias("metric_time"),

        first("inventory_data.socket_count")
        .alias("socket_count"),

        first("inventory_data.cpu_inventory")
        .alias("cpu_inventory"),

        first("inventory_data.memory_inventory")
        .alias("memory_inventory")
    )

    print("✅ Aggregations completed")

    # =====================================================
    # FINAL OUTPUT
    # =====================================================

    final_df = agg_df.select(

        col("report_id"),

        col("device_id"),

        col("application_customer_id"),

        col("platform_customer_id"),

        col("status"),

        col("report_type"),

        col("error_reason"),

        round(col("avg_metric_value"), 2)
        .alias("MetricValue"),

        col("model"),

        col("tags"),

        col("location_state"),

        col("location_country"),

        col("processor_vendor"),

        col("server_generation"),

        col("location_id"),

        col("location_name"),

        col("location_city"),

        col("server_name"),

        lit("power_metric")
        .alias("metric_id"),

        to_json(col("cpu_inventory"))
        .alias("cpu_inventory"),

        to_json(col("memory_inventory"))
        .alias("memory_inventory"),

        lit(0)
        .cast("int")
        .alias("pcie_devices_count"),

        col("socket_count")
        .cast("int")
        .alias("socket_count"),

        round(col("avg_metric_value"), 2)
        .alias("avg_metric_value"),

        round(col("max_metric_value"), 2)
        .alias("max_metric_value"),

        round(col("min_metric_value"), 2)
        .alias("min_metric_value"),

        round(col("avg_cpu_watts"), 2)
        .alias("avg_cpu_watts"),

        round(col("max_cpu_watts"), 2)
        .alias("max_cpu_watts"),

        round(col("avg_gpu_watts"), 2)
        .alias("avg_gpu_watts"),

        round(col("avg_peak_power"), 2)
        .alias("avg_peak_power"),

        round(col("max_peak_power"), 2)
        .alias("max_peak_power"),

        col("metric_time"),

        unix_timestamp("metric_time")
        .cast("double")
        .alias("datetime"),

        (
            unix_timestamp("metric_time") + 60
        )
        .cast("double")
        .alias("timeRangeEnd"),

        round(col("amb_temp"), 2)
        .alias("amb_temp"),

        unix_timestamp()
        .cast("double")
        .alias("Insertiontime"),

        lit(0.5)
        .cast("double")
        .alias("co2_factor"),

        lit(1.2)
        .cast("double")
        .alias("energy_cost_factor"),

        col("metric_time")
        .alias("max_metric_time"),

        to_date("metric_time")
        .cast("string")
        .alias("location_date"),

        to_date("metric_time")
        .cast("string")
        .alias("inventory_date")
    )

    print("✅ Final output dataframe ready")

    # =====================================================
    # WRITE OUTPUT
    # =====================================================

    (
        final_df
        .coalesce(2)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(f"✅ Batch parquet written → {OUTPUT_PATH}")

    # =====================================================
    # STOP SPARK
    # =====================================================

    spark.stop()

    print("✅ Batch Job Completed Successfully")

# =========================================================
# MAIN
# =====================================================

if __name__ == "__main__":

    run_batch()