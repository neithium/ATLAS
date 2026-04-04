from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time, json, os

spark = SparkSession.builder.appName("Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

INPUT = "/app/data/processed/stream"
LATEST = "/app/data/processed/batch/latest"
HISTORY = "/app/data/processed/batch/history"
METRICS = "/app/data/metrics/batch_metrics.json"

os.makedirs("/app/data/metrics", exist_ok=True)

print("🟡 BATCH STARTED")

run_id = 0

while True:
    print("⏳ Waiting 6 minutes...")
    time.sleep(360)

    try:
        start = time.time()

        # ---------------- READ + OPTIMIZE ----------------
        df = spark.read.parquet(INPUT).repartition(32)

        df = df.filter("device_id IS NOT NULL")

        df = df.cache()
        rows = df.count()

        # ---------------- AGGREGATION ----------------
        result = df.groupBy("device_id") \
            .avg("avg(power)", "avg(cpu)", "avg(temp)")

        # ---------------- ADD TIMESTAMP ----------------
        result_with_time = result.withColumn("batch_time", current_timestamp())

        # ---------------- WRITE LATEST (SNAPSHOT) ----------------
        result_with_time.coalesce(1).write \
            .mode("overwrite") \
            .parquet(LATEST)

        # ---------------- WRITE HISTORY (APPEND) ----------------
        result_with_time.write \
            .mode("append") \
            .partitionBy("batch_time") \
            .parquet(HISTORY)

        # ---------------- CLEANUP ----------------
        df.unpersist()

        duration = time.time() - start

        record = {
            "run_id": run_id,
            "rows": rows,
            "duration": duration,
            "throughput": rows / duration if duration else 0
        }

        with open(METRICS, "a") as f:
            f.write(json.dumps(record) + "\n")

        print(f"✅ Batch Run {run_id} complete | Rows: {rows} | Time: {duration:.2f}s")

        run_id += 1

    except Exception as e:
        print("⚠️ Waiting for stream...", e)