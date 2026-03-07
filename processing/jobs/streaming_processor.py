# Spark Processor
# ✔ Container running
# ✔ Spark session initialized
# ✔ Spark UI accessible
# ✔ Executor active
# ✖ No streaming job yet
# Currently the Spark processor container initializes a Spark session 
# and exposes the Spark UI. Since Kafka ingestion is not yet integrated, 
# no streaming queries are running, so the executor remains idle.
#  An infinite sleep loop keeps the Spark driver alive for debugging until the Kafka streaming pipeline is implemented.


from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("AtlasProcessor") \
    .getOrCreate()

print("Spark Processor Container Initialized Successfully")

# Keep Spark running so Spark UI stays alive
while True:
    time.sleep(60)