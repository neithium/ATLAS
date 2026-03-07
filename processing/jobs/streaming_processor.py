from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AtlasProcessor") \
    .getOrCreate()

print("Spark Processor Container Initialized Successfully")

spark.stop()