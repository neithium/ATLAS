from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AtlasBatchProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("====================================")
print("ATLAS BATCH JOB STARTED")
print("====================================")

df = spark.read.parquet("/app/data/processed/stream")

result = df.groupBy("device_id") \
    .avg("avg(cpu_usage)", "avg(memory_usage)")

result.write \
    .mode("overwrite") \
    .parquet("/app/data/processed/batch")

print("Batch job finished successfully")