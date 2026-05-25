from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count
from pyspark.sql.types import *
import time, json, os
import logging

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
)
logger = logging.getLogger("ATLAS")

# ---------------- SPARK ----------------
spark = SparkSession.builder.appName("Batch").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

# ---------------- PATHS ----------------
INPUT = "/app/data/raw"
OUTPUT = "/app/data/processed/batch"
METRICS = "/app/data/metrics/batch_metrics.json"

os.makedirs("/app/data/metrics", exist_ok=True)

logger.info("BATCH JOB STARTED")

# ---------------- SCHEMA ----------------
schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("cpu", IntegerType()),
    StructField("mem", IntegerType())
])

processed_days = set()
run_id = 0

def ensure_scalar(df, c):
    dt = dict(df.dtypes).get(c)
    if dt and dt.startswith("array"):
        return df.withColumn(c, col(c).getItem(0))
    return df

# ---------------- RUN ONCE ----------------
try:
    if not os.path.exists(INPUT) or not os.listdir(INPUT):
        logger.info("No data found in input directory.")
    else:
        df = spark.read.schema(schema).json(INPUT)

        if df.rdd.isEmpty():
            logger.info("Input dataframe is empty.")
        else:
            for c in ["device_id", "timestamp", "cpu", "mem"]:
                df = ensure_scalar(df, c)

            df = df.select(
                col("device_id").cast("string"),
                col("timestamp").cast("string"),
                col("cpu").cast("int"),
                col("mem").cast("int")
            ).where(col("device_id").isNotNull() & col("timestamp").isNotNull())

            flat = df.select(
                col("device_id"),
                to_date("timestamp").alias("event_date"),
                col("cpu"),
                col("mem")
            )

            all_days = [r[0] for r in flat.select("event_date").distinct().collect()]
            if all_days:
                # Process all available data for this batch run
                for day in sorted(all_days):
                    logger.info(f"Processing day={day}")
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

                    metrics_data = {
                        "run_id": run_id,
                        "event_date": str(day),
                        "rows": rows,
                        "duration": duration,
                        "throughput": rows / duration if duration else 0
                    }

                    with open(METRICS, "a") as f:
                        f.write(json.dumps(metrics_data) + "\n")

                    logger.info(f"Batch completed | {metrics_data}")
                    run_id += 1

except Exception:
    logger.exception("Batch failed")

logger.info("BATCH JOB FINISHED")
