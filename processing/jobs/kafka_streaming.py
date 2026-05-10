# # # # # # from pyspark.sql import SparkSession
# # # # # # from pyspark.sql.functions import *
# # # # # # from pyspark.sql.types import *
# # # # # # import logging

# # # # # # # ---------------- LOGGING ----------------
# # # # # # logging.basicConfig(
# # # # # #     level=logging.INFO,
# # # # # #     format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
# # # # # # )
# # # # # # logger = logging.getLogger("ATLAS")

# # # # # # # ---------------- SPARK ----------------
# # # # # # spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()
# # # # # # spark.sparkContext.setLogLevel("ERROR")

# # # # # # logging.getLogger("py4j").setLevel(logging.ERROR)
# # # # # # logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

# # # # # # logger.info("STREAMING STARTED")

# # # # # # # ---------------- SCHEMA ----------------
# # # # # # schema = StructType([
# # # # # #     StructField("device_id", StringType()),
# # # # # #     StructField("timestamp", StringType()),
# # # # # #     StructField("cpu", IntegerType()),
# # # # # #     StructField("mem", IntegerType())
# # # # # # ])

# # # # # # # ---------------- READ KAFKA ----------------
# # # # # # df = spark.readStream \
# # # # # #     .format("kafka") \
# # # # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # # # #     .option("subscribe", "raw-server-metrics") \
# # # # # #     .option("startingOffsets", "latest") \
# # # # # #     .load()

# # # # # # parsed = df.selectExpr("CAST(value AS STRING)") \
# # # # # #     .select(from_json(col("value"), schema).alias("data")) \
# # # # # #     .select("data.*")

# # # # # # parsed = parsed.withColumn(
# # # # # #     "event_time",
# # # # # #     to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
# # # # # # )

# # # # # # # ---------------- AGG ----------------
  
# # # # # # agg = parsed \
# # # # # #     .withWatermark("event_time", "2 hours") \
# # # # # #     .groupBy(
# # # # # #         window(col("event_time"), "1 hour"),
# # # # # #         col("device_id")
# # # # # #     ) \
# # # # # #     .agg(
# # # # # #         avg("cpu").alias("avg_cpu"),
# # # # # #         avg("mem").alias("avg_mem"),
# # # # # #         count("*").alias("num_records")
# # # # # #     )

# # # # # # final_df = agg.select(
# # # # # #     col("window.start").alias("window_start"),
# # # # # #     col("window.end").alias("window_end"),
# # # # # #     "device_id",
# # # # # #     "avg_cpu",
# # # # # #     "avg_mem",
# # # # # #     "num_records"
# # # # # # )

# # # # # # # ---------------- WRITE ----------------
# # # # # # # query = final_df \
# # # # # #     # .writeStream \
# # # # # #     # .format("parquet") \
# # # # # #     # .outputMode("append") \
# # # # # #     # .option("path", "/app/data/processed/stream") \
# # # # # #     # .option("checkpointLocation", "/app/checkpoint/stream") \
# # # # # #     # .trigger(processingTime="30 seconds") \
# # # # # #     # .start()
# # # # # # def log_and_write(batch_df, batch_id):
# # # # # #     rows = batch_df.count()

# # # # # #     print(f"🚀 STREAM BATCH | id={batch_id} | rows={rows}")

# # # # # #     logger.info(f"STREAM BATCH | id={batch_id} | rows={rows}")

# # # # # #     batch_df.write.mode("append").parquet("/app/data/processed/stream")

# # # # # # query = final_df \
# # # # # #     .writeStream \
# # # # # #     .foreachBatch(log_and_write) \
# # # # # #     .outputMode("append") \
# # # # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # # # #     .trigger(processingTime="30 seconds") \
# # # # # #     .start()
# # # # # # logger.info("Streaming query started")

# # # # # # query.awaitTermination()
# # # # # from pyspark.sql import SparkSession
# # # # # from pyspark.sql.functions import *
# # # # # from input_schema import input_schema

# # # # # spark = SparkSession.builder \
# # # # #     .appName("Streaming") \
# # # # #     .master("local[6]") \
# # # # #     .config("spark.sql.shuffle.partitions", "6") \
# # # # #     .config("spark.default.parallelism", "6") \
# # # # #     .getOrCreate()

# # # # # spark.sparkContext.setLogLevel("ERROR")

# # # # # # Kafka read
# # # # # df = spark.readStream.format("kafka") \
# # # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # # #     .option("subscribe", "raw-server-metrics") \
# # # # #     .option("startingOffsets", "latest") \
# # # # #     .load()

# # # # # # Parse JSON safely
# # # # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # # # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # # # #     .filter(col("data").isNotNull())

# # # # # # Flatten
# # # # # flat = json_df.select("data.*") \
# # # # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # # # Event time
# # # # # flat = flat.withColumn("event_time", to_timestamp("p.Time"))

# # # # # # Window aggregation
# # # # # agg = flat \
# # # # #     .withWatermark("event_time", "10 minutes") \
# # # # #     .groupBy(
# # # # #         window("event_time", "1 minute"),
# # # # #         "device_id"
# # # # #     ) \
# # # # #     .agg(
# # # # #         avg("p.Average").alias("avg_metric_value"),
# # # # #         max("p.Average").alias("max_metric_value"),
# # # # #         min("p.Average").alias("min_metric_value"),
# # # # #         first("report_id").alias("report_id"),
# # # # #         first("application_customer_id").alias("application_customer_id"),
# # # # #         first("platform_customer_id").alias("platform_customer_id"),
# # # # #         first("status").alias("status"),
# # # # #         first("report_type").alias("report_type"),
# # # # #         first("error_reason").alias("error_reason"),
# # # # #         first("model").alias("model"),
# # # # #         first("tags").alias("tags"),
# # # # #         first("location_state").alias("location_state"),
# # # # #         first("location_country").alias("location_country"),
# # # # #         first("processor_vendor").alias("processor_vendor"),
# # # # #         first("server_generation").alias("server_generation"),
# # # # #         first("location_id").alias("location_id"),
# # # # #         first("location_name").alias("location_name"),
# # # # #         first("location_city").alias("location_city"),
# # # # #         first("server_name").alias("server_name"),
# # # # #         first("inventory_data.socket_count").alias("socket_count")
# # # # #     )

# # # # # # Final schema mapping
# # # # # result = agg.select(
# # # # #     "report_id",
# # # # #     "device_id",
# # # # #     "application_customer_id",
# # # # #     "platform_customer_id",
# # # # #     "status",
# # # # #     "report_type",
# # # # #     "error_reason",
# # # # #     col("avg_metric_value").alias("MetricValue"),
# # # # #     "model",
# # # # #     "tags",
# # # # #     "location_state",
# # # # #     "location_country",
# # # # #     "processor_vendor",
# # # # #     "server_generation",
# # # # #     "location_id",
# # # # #     "location_name",
# # # # #     "location_city",
# # # # #     "server_name",
# # # # #     lit("power").alias("metric_id"),
# # # # #     lit("cpu").alias("cpu_inventory"),
# # # # #     lit("memory").alias("memory_inventory"),
# # # # #     lit(0).alias("pcie_devices_count"),
# # # # #     "socket_count",
# # # # #     "avg_metric_value",
# # # # #     "max_metric_value",
# # # # #     "min_metric_value",
# # # # #     col("window.start").cast("string").alias("metric_time"),
# # # # #     unix_timestamp("window.start").cast("double").alias("datetime"),
# # # # #     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
# # # # #     lit(25.0).alias("amb_temp"),
# # # # #     unix_timestamp().cast("double").alias("Insertiontime"),
# # # # #     lit(0.5).alias("co2_factor"),
# # # # #     lit(1.2).alias("energy_cost_factor"),
# # # # #     col("window.end").cast("string").alias("max_metric_time"),
# # # # #     to_date("window.start").cast("string").alias("location_date"),
# # # # #     to_date("window.start").cast("string").alias("inventory_date")
# # # # # )

# # # # # # One file per window
# # # # # def write_batch(df, epoch_id):
# # # # #     df.coalesce(1).write.mode("append").parquet("/app/data/processed/stream")

# # # # # query = result.writeStream \
# # # # #     .foreachBatch(write_batch) \
# # # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # # #     .trigger(processingTime="5 seconds") \
# # # # #     .start()

# # # # # query.awaitTermination()
# # # # # from pyspark.sql import SparkSession
# # # # # from pyspark.sql.functions import *
# # # # # from input_schema import input_schema

# # # # # # ---------------- SPARK SESSION ----------------
# # # # # spark = SparkSession.builder \
# # # # #     .appName("Streaming") \
# # # # #     .master("local[6]") \
# # # # #     .config("spark.sql.shuffle.partitions", "6") \
# # # # #     .config("spark.default.parallelism", "6") \
# # # # #     .getOrCreate()

# # # # # spark.sparkContext.setLogLevel("ERROR")

# # # # # print("🚀 Spark Streaming Started (Listening to Kafka...)")

# # # # # # ---------------- READ FROM KAFKA ----------------
# # # # # df = spark.readStream.format("kafka") \
# # # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # # #     .option("subscribe", "raw-server-metrics") \
# # # # #     .option("startingOffsets", "latest") \
# # # # #     .load()

# # # # # # ---------------- PARSE JSON ----------------
# # # # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # # # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # # # #     .filter(col("data").isNotNull())

# # # # # # ---------------- FLATTEN ----------------
# # # # # flat = json_df.select("data.*") \
# # # # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # # # ---------------- FIXED TIMESTAMP PARSING ----------------
# # # # # flat = flat.withColumn(
# # # # #     "event_time",
# # # # #     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# # # # # )

# # # # # # 🔍 DEBUG: verify parsing (will print in logs)
# # # # # flat.select("p.Time", "event_time").writeStream \
# # # # #     .format("console") \
# # # # #     .outputMode("append") \
# # # # #     .option("truncate", False) \
# # # # #     .start()

# # # # # # ---------------- AGGREGATION ----------------
# # # # # agg = flat \
# # # # #     .filter(col("event_time").isNotNull()) \
# # # # #     .withWatermark("event_time", "10 seconds") \
# # # # #     .groupBy(
# # # # #         window("event_time", "1 minute"),
# # # # #         "device_id"
# # # # #     ) \
# # # # #     .agg(
# # # # #         avg("p.Average").alias("avg_metric_value"),
# # # # #         max("p.Average").alias("max_metric_value"),
# # # # #         min("p.Average").alias("min_metric_value"),
# # # # #         first("report_id").alias("report_id"),
# # # # #         first("application_customer_id").alias("application_customer_id"),
# # # # #         first("platform_customer_id").alias("platform_customer_id"),
# # # # #         first("status").alias("status"),
# # # # #         first("report_type").alias("report_type"),
# # # # #         first("error_reason").alias("error_reason"),
# # # # #         first("model").alias("model"),
# # # # #         first("tags").alias("tags"),
# # # # #         first("location_state").alias("location_state"),
# # # # #         first("location_country").alias("location_country"),
# # # # #         first("processor_vendor").alias("processor_vendor"),
# # # # #         first("server_generation").alias("server_generation"),
# # # # #         first("location_id").alias("location_id"),
# # # # #         first("location_name").alias("location_name"),
# # # # #         first("location_city").alias("location_city"),
# # # # #         first("server_name").alias("server_name"),
# # # # #         first("inventory_data.socket_count").alias("socket_count")
# # # # #     )

# # # # # # ---------------- FINAL OUTPUT ----------------
# # # # # result = agg.select(
# # # # #     "report_id",
# # # # #     "device_id",
# # # # #     "application_customer_id",
# # # # #     "platform_customer_id",
# # # # #     "status",
# # # # #     "report_type",
# # # # #     "error_reason",
# # # # #     col("avg_metric_value").alias("MetricValue"),
# # # # #     "model",
# # # # #     "tags",
# # # # #     "location_state",
# # # # #     "location_country",
# # # # #     "processor_vendor",
# # # # #     "server_generation",
# # # # #     "location_id",
# # # # #     "location_name",
# # # # #     "location_city",
# # # # #     "server_name",
# # # # #     lit("power").alias("metric_id"),
# # # # #     lit("cpu").alias("cpu_inventory"),
# # # # #     lit("memory").alias("memory_inventory"),
# # # # #     lit(0).alias("pcie_devices_count"),
# # # # #     "socket_count",
# # # # #     "avg_metric_value",
# # # # #     "max_metric_value",
# # # # #     "min_metric_value",
# # # # #     col("window.start").cast("string").alias("metric_time"),
# # # # #     unix_timestamp("window.start").cast("double").alias("datetime"),
# # # # #     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
# # # # #     lit(25.0).alias("amb_temp"),
# # # # #     unix_timestamp().cast("double").alias("Insertiontime"),
# # # # #     lit(0.5).alias("co2_factor"),
# # # # #     lit(1.2).alias("energy_cost_factor"),
# # # # #     col("window.end").cast("string").alias("max_metric_time"),
# # # # #     to_date("window.start").cast("string").alias("location_date"),
# # # # #     to_date("window.start").cast("string").alias("inventory_date")
# # # # # )

# # # # # # ---------------- WRITE ----------------
# # # # # def write_batch(df, epoch_id):
# # # # #     count = df.count()
# # # # #     print(f"🚀 Batch {epoch_id} | rows={count}")

# # # # #     if count > 0:
# # # # #         df.write.mode("append").parquet("/app/data/processed/stream")

# # # # # # ---------------- START STREAM ----------------
# # # # # query = result.writeStream \
# # # # #     .foreachBatch(write_batch) \
# # # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # # #     .trigger(processingTime="5 seconds") \
# # # # #     .start()

# # # # # query.awaitTermination()
# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import *
# # # # from input_schema import input_schema

# # # # # ---------------- SPARK SESSION ----------------
# # # # spark = SparkSession.builder \
# # # #     .appName("Streaming") \
# # # #     .master("local[6]") \
# # # #     .config("spark.sql.shuffle.partitions", "6") \
# # # #     .config("spark.default.parallelism", "6") \
# # # #     .getOrCreate()

# # # # spark.sparkContext.setLogLevel("ERROR")

# # # # print("🚀 Spark Streaming Started")

# # # # # ---------------- READ FROM KAFKA ----------------
# # # # df = spark.readStream.format("kafka") \
# # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # #     .option("subscribe", "raw-server-metrics") \
# # # #     .option("startingOffsets", "latest") \
# # # #     .option("kafka.group.id", "atlas-processor-streaming-group") \
# # # #     .option("failOnDataLoss", "false") \
# # # #     .load()

# # # # # ---------------- PARSE JSON ----------------
# # # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # # #     .filter(col("data").isNotNull())

# # # # # ---------------- FLATTEN ----------------
# # # # flat = json_df.select("data.*") \
# # # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # # ---------------- TIMESTAMP (FIXED) ----------------
# # # # flat = flat.withColumn(
# # # #     "event_time",
# # # #     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# # # # )

# # # # # ---------------- AGGREGATION ----------------
# # # # agg = flat \
# # # #     .filter(col("event_time").isNotNull()) \
# # # #     .withWatermark("event_time", "10 seconds") \
# # # #     .groupBy(
# # # #         window("event_time", "10 seconds"),   # 🔥 shorter window
# # # #         "device_id"
# # # #     ) \
# # # #     .agg(
# # # #         avg("p.Average").alias("avg_metric_value"),
# # # #         max("p.Average").alias("max_metric_value"),
# # # #         min("p.Average").alias("min_metric_value"),
# # # #         first("report_id").alias("report_id"),
# # # #         first("application_customer_id").alias("application_customer_id"),
# # # #         first("platform_customer_id").alias("platform_customer_id"),
# # # #         first("status").alias("status"),
# # # #         first("report_type").alias("report_type"),
# # # #         first("error_reason").alias("error_reason"),
# # # #         first("model").alias("model"),
# # # #         first("tags").alias("tags"),
# # # #         first("location_state").alias("location_state"),
# # # #         first("location_country").alias("location_country"),
# # # #         first("processor_vendor").alias("processor_vendor"),
# # # #         first("server_generation").alias("server_generation"),
# # # #         first("location_id").alias("location_id"),
# # # #         first("location_name").alias("location_name"),
# # # #         first("location_city").alias("location_city"),
# # # #         first("server_name").alias("server_name"),
# # # #         first("inventory_data.socket_count").alias("socket_count")
# # # #     )

# # # # # ---------------- FINAL OUTPUT ----------------
# # # # result = agg.select(
# # # #     "report_id",
# # # #     "device_id",
# # # #     "application_customer_id",
# # # #     "platform_customer_id",
# # # #     "status",
# # # #     "report_type",
# # # #     "error_reason",
# # # #     col("avg_metric_value").alias("MetricValue"),
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
# # # #     "socket_count",
# # # #     "avg_metric_value",
# # # #     "max_metric_value",
# # # #     "min_metric_value",
# # # #     col("window.start").cast("string").alias("metric_time"),
# # # #     unix_timestamp("window.start").cast("double").alias("datetime"),
# # # #     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
# # # #     lit(25.0).alias("amb_temp"),
# # # #     unix_timestamp().cast("double").alias("Insertiontime"),
# # # #     lit(0.5).alias("co2_factor"),
# # # #     lit(1.2).alias("energy_cost_factor"),
# # # #     col("window.end").cast("string").alias("max_metric_time"),
# # # #     to_date("window.start").cast("string").alias("location_date"),
# # # #     to_date("window.start").cast("string").alias("inventory_date")
# # # # )

# # # # # ---------------- WRITE ----------------
# # # # def write_batch(df, epoch_id):
# # # #     count = df.count()
# # # #     print(f"🚀 Batch {epoch_id} | rows={count}")

# # # #     if count > 0:
# # # #         df.write.mode("append").parquet("/app/data/processed/stream")

# # # # # ---------------- START STREAM ----------------
# # # # query = result.writeStream \
# # # #     .foreachBatch(write_batch) \
# # # #     .outputMode("update") \
# # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # #     .trigger(processingTime="10 seconds") \
# # # #     .start()

# # # # query.awaitTermination()
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import *
# # # from input_schema import input_schema

# # # # ---------------- SPARK ----------------
# # # spark = SparkSession.builder \
# # #     .appName("Streaming-Final") \
# # #     .master("local[6]") \
# # #     .config("spark.sql.shuffle.partitions", "12") \
# # #     .config("spark.default.parallelism", "12") \
# # #     .getOrCreate()

# # # spark.sparkContext.setLogLevel("ERROR")

# # # print("🚀 Final Streaming Started (Schema Strict)")

# # # # ---------------- READ KAFKA ----------------
# # # df = spark.readStream.format("kafka") \
# # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # #     .option("subscribe", "raw-server-metrics") \
# # #     .option("startingOffsets", "latest") \
# # #     .option("kafka.group.id", "atlas-processor-streaming-group") \
# # #     .option("failOnDataLoss", "false") \
# # #     .load()

# # # # ---------------- PARSE ----------------
# # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # #     .filter(col("data").isNotNull())

# # # # ---------------- FLATTEN ----------------
# # # flat = json_df.select("data.*") \
# # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # ---------------- PARALLELISM ----------------
# # # flat = flat.repartition(12, col("device_id"))

# # # # ---------------- TIMESTAMP ----------------
# # # flat = flat.withColumn(
# # #     "event_time",
# # #     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# # # ).filter(col("event_time").isNotNull())

# # # # ---------------- AGG ----------------
# # # agg = flat \
# # #     .withWatermark("event_time", "20 seconds") \
# # #     .groupBy(
# # #         window("event_time", "20 seconds"),
# # #         "device_id"
# # #     ) \
# # #     .agg(
# # #         avg("p.Average").alias("avg_metric_value"),
# # #         max("p.Average").alias("max_metric_value"),
# # #         min("p.Average").alias("min_metric_value"),

# # #         first("report_id").alias("report_id"),
# # #         first("application_customer_id").alias("application_customer_id"),
# # #         first("platform_customer_id").alias("platform_customer_id"),
# # #         first("status").alias("status"),
# # #         first("report_type").alias("report_type"),
# # #         first("error_reason").alias("error_reason"),
# # #         first("model").alias("model"),
# # #         first("tags").alias("tags"),
# # #         first("location_state").alias("location_state"),
# # #         first("location_country").alias("location_country"),
# # #         first("processor_vendor").alias("processor_vendor"),
# # #         first("server_generation").alias("server_generation"),
# # #         first("location_id").alias("location_id"),
# # #         first("location_name").alias("location_name"),
# # #         first("location_city").alias("location_city"),
# # #         first("server_name").alias("server_name"),
# # #         first("inventory_data.socket_count").alias("socket_count")
# # #     )

# # # # ---------------- FINAL SELECT (STRICT ORDER + TYPES) ----------------
# # # result = agg.select(
# # #     col("report_id").cast("string"),
# # #     col("device_id").cast("string"),
# # #     col("application_customer_id").cast("string"),
# # #     col("platform_customer_id").cast("string"),
# # #     col("status").cast("boolean"),
# # #     col("report_type").cast("string"),
# # #     col("error_reason").cast("string"),
# # #     col("avg_metric_value").cast("double").alias("MetricValue"),
# # #     col("model").cast("string"),
# # #     col("tags").cast("string"),
# # #     col("location_state").cast("string"),
# # #     col("location_country").cast("string"),
# # #     col("processor_vendor").cast("string"),
# # #     col("server_generation").cast("string"),
# # #     col("location_id").cast("string"),
# # #     col("location_name").cast("string"),
# # #     col("location_city").cast("string"),
# # #     col("server_name").cast("string"),
# # #     lit("power").cast("string").alias("metric_id"),
# # #     lit("cpu").cast("string").alias("cpu_inventory"),
# # #     lit("memory").cast("string").alias("memory_inventory"),
# # #     lit(0).cast("int").alias("pcie_devices_count"),
# # #     col("socket_count").cast("int"),
# # #     col("avg_metric_value").cast("double"),
# # #     col("max_metric_value").cast("double"),
# # #     col("min_metric_value").cast("double"),
# # #     col("window.start").cast("string").alias("metric_time"),
# # #     unix_timestamp("window.start").cast("double").alias("datetime"),
# # #     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
# # #     lit(25.0).cast("double").alias("amb_temp"),
# # #     unix_timestamp().cast("double").alias("Insertiontime"),
# # #     lit(0.5).cast("double").alias("co2_factor"),
# # #     lit(1.2).cast("double").alias("energy_cost_factor"),
# # #     col("window.end").cast("string").alias("max_metric_time"),
# # #     to_date("window.start").cast("string").alias("location_date"),
# # #     to_date("window.start").cast("string").alias("inventory_date")
# # # )

# # # # ---------------- WRITE ----------------
# # # def write_batch(df, epoch_id):
# # #     print(f"🚀 Batch {epoch_id}")

# # #     if not df.isEmpty():
# # #         df.write \
# # #             .mode("append") \
# # #             .option("compression", "snappy") \
# # #             .parquet("/app/data/processed/stream")

# # # # ---------------- STREAM ----------------
# # # query = result.writeStream \
# # #     .foreachBatch(write_batch) \
# # #     .outputMode("update") \
# # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # #     .trigger(processingTime="10 seconds") \
# # #     .start()

# # # query.awaitTermination()
# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import *
# # from input_schema import input_schema

# # # ---------------- SPARK ----------------
# # spark = SparkSession.builder \
# #     .appName("Streaming-Final") \
# #     .master("local[6]") \
# #     .config("spark.sql.shuffle.partitions", "12") \
# #     .config("spark.default.parallelism", "12") \
# #     .getOrCreate()

# # spark.sparkContext.setLogLevel("ERROR")

# # print("🚀 Final Streaming Started (Schema + Count + Optimized)")

# # # ---------------- READ KAFKA ----------------
# # df = spark.readStream.format("kafka") \
# #     .option("kafka.bootstrap.servers", "broker1:9092") \
# #     .option("subscribe", "raw-server-metrics") \
# #     .option("startingOffsets", "latest") \
# #     .option("kafka.group.id", "atlas-processor-streaming-group") \
# #     .option("failOnDataLoss", "false") \
# #     .load()

# # # ---------------- PARSE ----------------
# # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# #     .select(from_json(col("json"), input_schema).alias("data")) \
# #     .filter(col("data").isNotNull())

# # # ---------------- FLATTEN ----------------
# # flat = json_df.select("data.*") \
# #     .withColumn("p", explode(col("data.PowerDetail")))

# # # ---------------- PARALLELISM ----------------
# # flat = flat.repartition(12, col("device_id"))

# # # ---------------- TIMESTAMP ----------------
# # flat = flat.withColumn(
# #     "event_time",
# #     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# # ).filter(col("event_time").isNotNull())

# # # ---------------- AGGREGATION ----------------
# # agg = flat \
# #     .withWatermark("event_time", "20 seconds") \
# #     .groupBy(
# #         window("event_time", "20 seconds"),
# #         "device_id"
# #     ) \
# #     .agg(
# #         avg("p.Average").alias("avg_metric_value"),
# #         max("p.Average").alias("max_metric_value"),
# #         min("p.Average").alias("min_metric_value"),

# #         first("report_id").alias("report_id"),
# #         first("application_customer_id").alias("application_customer_id"),
# #         first("platform_customer_id").alias("platform_customer_id"),
# #         first("status").alias("status"),
# #         first("report_type").alias("report_type"),
# #         first("error_reason").alias("error_reason"),
# #         first("model").alias("model"),
# #         first("tags").alias("tags"),
# #         first("location_state").alias("location_state"),
# #         first("location_country").alias("location_country"),
# #         first("processor_vendor").alias("processor_vendor"),
# #         first("server_generation").alias("server_generation"),
# #         first("location_id").alias("location_id"),
# #         first("location_name").alias("location_name"),
# #         first("location_city").alias("location_city"),
# #         first("server_name").alias("server_name"),
# #         first("inventory_data.socket_count").alias("socket_count")
# #     )

# # # ---------------- FINAL SELECT (STRICT SCHEMA) ----------------
# # result = agg.select(
# #     col("report_id").cast("string"),
# #     col("device_id").cast("string"),
# #     col("application_customer_id").cast("string"),
# #     col("platform_customer_id").cast("string"),
# #     col("status").cast("boolean"),
# #     col("report_type").cast("string"),
# #     col("error_reason").cast("string"),
# #     col("avg_metric_value").cast("double").alias("MetricValue"),
# #     col("model").cast("string"),
# #     col("tags").cast("string"),
# #     col("location_state").cast("string"),
# #     col("location_country").cast("string"),
# #     col("processor_vendor").cast("string"),
# #     col("server_generation").cast("string"),
# #     col("location_id").cast("string"),
# #     col("location_name").cast("string"),
# #     col("location_city").cast("string"),
# #     col("server_name").cast("string"),
# #     lit("power").cast("string").alias("metric_id"),
# #     lit("cpu").cast("string").alias("cpu_inventory"),
# #     lit("memory").cast("string").alias("memory_inventory"),
# #     lit(0).cast("int").alias("pcie_devices_count"),
# #     col("socket_count").cast("int"),
# #     col("avg_metric_value").cast("double"),
# #     col("max_metric_value").cast("double"),
# #     col("min_metric_value").cast("double"),
# #     col("window.start").cast("string").alias("metric_time"),
# #     unix_timestamp("window.start").cast("double").alias("datetime"),
# #     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
# #     lit(25.0).cast("double").alias("amb_temp"),
# #     unix_timestamp().cast("double").alias("Insertiontime"),
# #     lit(0.5).cast("double").alias("co2_factor"),
# #     lit(1.2).cast("double").alias("energy_cost_factor"),
# #     col("window.end").cast("string").alias("max_metric_time"),
# #     to_date("window.start").cast("string").alias("location_date"),
# #     to_date("window.start").cast("string").alias("inventory_date")
# # )

# # # ---------------- WRITE FUNCTION ----------------
# # def write_batch(df, epoch_id):
# #     # Cache to avoid recomputation
# #     # df.persist()

# #     # row_count = df.count()
# #     # print(f"🚀 Batch {epoch_id} | rows received = {row_count}")
# #     print(f"🚀 Batch {epoch_id} received")

# #     if row_count > 0:
# #         df.write \
# #             .mode("append") \
# #             .option("compression", "snappy") \
# #             .parquet("/app/data/processed/stream")

# #     df.unpersist()

# # # ---------------- START STREAM ----------------
# # query = result.writeStream \
# #     .foreachBatch(write_batch) \
# #     .outputMode("update") \
# #     .option("checkpointLocation", "/app/checkpoint/stream") \
# #     .trigger(processingTime="10 seconds") \
# #     .start()

# # query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from input_schema import input_schema

# # ---------------- SPARK ----------------
# spark = SparkSession.builder \
#     .appName("Streaming-Final-Correct") \
#     .master("local[6]") \
#     .config("spark.sql.shuffle.partitions", "12") \
#     .config("spark.default.parallelism", "12") \
#     .config("spark.sql.streaming.kafka.offsetFetch.timeoutMs", "120000") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# print("🚀 Streaming Started (FINAL CORRECT)")

# # ---------------- READ KAFKA ----------------
# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "broker1:9092") \
#     .option("subscribe", "raw-server-metrics") \
#     .option("startingOffsets", "latest") \
#     .option("kafka.group.id", "atlas-processor-streaming-group") \
#     .option("failOnDataLoss", "false") \
#     .option("kafka.request.timeout.ms", "120000") \
#     .option("kafka.session.timeout.ms", "60000") \
#     .option("kafka.metadata.max.age.ms", "5000") \
#     .option("kafka.consumer.request.timeout.ms", "120000") \
#     .option("kafka.default.api.timeout.ms", "120000") \
#     .load()

# # ---------------- PARSE ----------------
# json_df = df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json(col("json"), input_schema).alias("data")) \
#     .filter(col("data").isNotNull())

# # ---------------- FLATTEN (NO DROP, NO HACKS) ----------------
# flat = json_df.select("data.*") \
#     .withColumn("p", explode_outer(col("data.PowerDetail")))

# # ---------------- OPTIONAL: FILTER FRESH (ONLY IF YOU WANT) ----------------
# # COMMENT THIS IF YOU WANT ALL DATA
# # flat = flat.filter(col("p.is_fresh") == True)

# # ---------------- PARALLELISM ----------------
# flat = flat.repartition(12, col("device_id"))

# # ---------------- TIMESTAMP ----------------
# flat = flat.withColumn(
#     "event_time",
#     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# )

# # fallback (safe)
# flat = flat.withColumn(
#     "event_time",
#     when(col("event_time").isNull(), current_timestamp())
#     .otherwise(col("event_time"))
# )

# # ---------------- AGGREGATION ----------------
# agg = flat \
#     .withWatermark("event_time", "20 seconds") \
#     .groupBy(
#         window("event_time", "20 seconds"),
#         "device_id"
#     ) \
#     .agg(
#         avg("p.Average").alias("avg_metric_value"),
#         max("p.Average").alias("max_metric_value"),
#         min("p.Average").alias("min_metric_value"),

#         first("report_id", True).alias("report_id"),
#         first("application_customer_id", True).alias("application_customer_id"),
#         first("platform_customer_id", True).alias("platform_customer_id"),
#         first("status", True).alias("status"),
#         first("report_type", True).alias("report_type"),
#         first("error_reason", True).alias("error_reason"),
#         first("model", True).alias("model"),
#         first("tags", True).alias("tags"),
#         first("location_state", True).alias("location_state"),
#         first("location_country", True).alias("location_country"),
#         first("processor_vendor", True).alias("processor_vendor"),
#         first("server_generation", True).alias("server_generation"),
#         first("location_id", True).alias("location_id"),
#         first("location_name", True).alias("location_name"),
#         first("location_city", True).alias("location_city"),
#         first("server_name", True).alias("server_name"),
#         first("inventory_data.socket_count", True).alias("socket_count")
#     )

# # ---------------- FINAL SELECT (STRICT SCHEMA) ----------------
# result = agg.select(
#     col("report_id").cast("string"),
#     col("device_id").cast("string"),
#     col("application_customer_id").cast("string"),
#     col("platform_customer_id").cast("string"),
#     col("status").cast("boolean"),
#     col("report_type").cast("string"),
#     col("error_reason").cast("string"),
#     col("avg_metric_value").cast("double").alias("MetricValue"),
#     col("model").cast("string"),
#     col("tags").cast("string"),
#     col("location_state").cast("string"),
#     col("location_country").cast("string"),
#     col("processor_vendor").cast("string"),
#     col("server_generation").cast("string"),
#     col("location_id").cast("string"),
#     col("location_name").cast("string"),
#     col("location_city").cast("string"),
#     col("server_name").cast("string"),
#     lit("power").cast("string").alias("metric_id"),
#     lit("cpu").cast("string").alias("cpu_inventory"),
#     lit("memory").cast("string").alias("memory_inventory"),
#     lit(0).cast("int").alias("pcie_devices_count"),
#     col("socket_count").cast("int"),
#     col("avg_metric_value").cast("double"),
#     col("max_metric_value").cast("double"),
#     col("min_metric_value").cast("double"),
#     col("window.start").cast("string").alias("metric_time"),
#     unix_timestamp("window.start").cast("double").alias("datetime"),
#     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
#     lit(25.0).cast("double").alias("amb_temp"),
#     unix_timestamp().cast("double").alias("Insertiontime"),
#     lit(0.5).cast("double").alias("co2_factor"),
#     lit(1.2).cast("double").alias("energy_cost_factor"),
#     col("window.end").cast("string").alias("max_metric_time"),
#     to_date("window.start").cast("string").alias("location_date"),
#     to_date("window.start").cast("string").alias("inventory_date")
# )

# # ---------------- WRITE ----------------
# def write_batch(df, epoch_id):

    
#     # Removing expensive .isEmpty() to avoid partition handshake timeouts 
#     # Spark only triggers foreachBatch when there is data (or a state/watermark update)
#     print(f"🚀 Batch {epoch_id} processing...")
    
#     df.coalesce(6) \
#       .write \
#       .mode("append") \
#       .option("compression", "snappy") \
#       .parquet("/app/data/processed/stream")

# # ---------------- START STREAM ----------------
# query = result.writeStream \
#     .foreachBatch(write_batch) \
#     .outputMode("update") \
#     .option("checkpointLocation", "/app/checkpoint/stream") \
#     .trigger(processingTime="10 seconds") \
#     .start()

# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================
# 1. SPARK SESSION (6 CORES)
# =========================
spark = (
    SparkSession.builder
    .appName("KafkaToParquet_DailyAggregation")
    .master("local[6]")
    .config("spark.sql.shuffle.partitions", "12")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================
# 2. INPUT SCHEMA
# =========================
input_schema = StructType([
    StructField("device_id", StringType()),
    StructField("report_id", StringType()),
    StructField("created_at", StringType()),
    StructField("status", BooleanType()),
    StructField("model", StringType()),
    StructField("tags", StringType()),
    StructField("report_type", StringType()),
    StructField("server_name", StringType()),
    StructField("error_reason", StringType()),
    StructField("location_id", StringType()),
    StructField("location_city", StringType()),
    StructField("location_name", StringType()),
    StructField("location_state", StringType()),
    StructField("location_country", StringType()),
    StructField("processor_vendor", StringType()),
    StructField("server_generation", StringType()),
    StructField("platform_customer_id", StringType()),
    StructField("application_customer_id", StringType()),
    StructField("metric_type", StringType()),
    StructField("data", StructType([
        StructField("PowerDetail", ArrayType(StructType([
            StructField("Average", DoubleType()),
            StructField("Minimum", DoubleType()),
            StructField("Peak", DoubleType()),
            StructField("Time", StringType())
        ])))
    ])),
    StructField("inventory_data", StructType([
        StructField("socket_count", IntegerType())
    ]))
])

# =========================
# 3. READ FROM KAFKA
# =========================
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "raw-server-metrics")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# =========================
# 4. RAW + PARSE
# =========================
raw_df = df.selectExpr("CAST(value AS STRING) as raw_json")

parsed = raw_df.select(
    col("raw_json"),
    from_json(col("raw_json"), input_schema).alias("data")
)

# =========================
# 5. SPLIT VALID / INVALID
# =========================
valid_df = parsed.filter(
    col("data").isNotNull() &
    col("data.data.PowerDetail").isNotNull()
)

invalid_df = parsed.filter(
    col("data").isNull() |
    col("data.data.PowerDetail").isNull()
)

# =========================
# 6. DLQ WRITE (KAFKA)
# =========================
invalid_kafka_df = invalid_df.selectExpr(
    "CAST(null AS STRING) AS key",
    "raw_json AS value"
)

dlq_query = (
    invalid_kafka_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("topic", "raw-server-metrics-dlq")
    .option("checkpointLocation", "/app/checkpoints/dlq")
    .outputMode("append")
    .start()
)

# =========================
# 7. CONTINUE ORIGINAL PIPELINE (UNCHANGED)
# =========================
parsed_clean = valid_df.select("data.*")

flat = (
    parsed_clean
    .withColumn("p", explode(col("data.PowerDetail")))
    .withColumn("event_time", to_timestamp(col("p.Time")))
)

flat = flat.repartition(12, col("device_id"))

agg = (
    flat.groupBy(
        col("device_id"),
        to_date("event_time").alias("location_date")
    )
    .agg(
        avg("p.Average").alias("avg_metric_value"),
        max("p.Average").alias("max_metric_value"),
        min("p.Average").alias("min_metric_value"),

        first("report_id", True).alias("report_id"),
        first("application_customer_id", True).alias("application_customer_id"),
        first("platform_customer_id", True).alias("platform_customer_id"),
        first("status", True).alias("status"),
        first("report_type", True).alias("report_type"),
        first("error_reason", True).alias("error_reason"),
        first("model", True).alias("model"),
        first("tags", True).alias("tags"),
        first("location_state", True).alias("location_state"),
        first("location_country", True).alias("location_country"),
        first("processor_vendor", True).alias("processor_vendor"),
        first("server_generation", True).alias("server_generation"),
        first("location_id", True).alias("location_id"),
        first("location_name", True).alias("location_name"),
        first("location_city", True).alias("location_city"),
        first("server_name", True).alias("server_name"),
        first("inventory_data.socket_count", True).alias("socket_count")
    )
)

final_df = agg.select(
    col("report_id"),
    col("device_id"),
    col("application_customer_id"),
    col("platform_customer_id"),
    col("status"),
    col("report_type"),
    col("error_reason"),
    lit(0.0).alias("MetricValue"),
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
    lit("power_metrics").alias("metric_id"),
    lit(None).cast("string").alias("cpu_inventory"),
    lit(None).cast("string").alias("memory_inventory"),
    lit(None).cast("int").alias("pcie_devices_count"),
    col("socket_count"),
    col("avg_metric_value"),
    col("max_metric_value"),
    col("min_metric_value"),
    col("location_date").cast("string").alias("metric_time"),
    lit(None).cast("double").alias("datetime"),
    lit(None).cast("double").alias("timeRangeEnd"),
    lit(None).cast("double").alias("amb_temp"),
    lit(None).cast("double").alias("Insertiontime"),
    lit(None).cast("double").alias("co2_factor"),
    lit(None).cast("double").alias("energy_cost_factor"),
    lit(None).cast("string").alias("max_metric_time"),
    col("location_date").cast("string").alias("location_date"),
    col("location_date").cast("string").alias("inventory_date")
)

# def write_batch(df, epoch_id):
#     print(f"🚀 Processing Batch {epoch_id}")

#     if not df.rdd.isEmpty():
#         df.write \
#             .mode("overwrite") \
#             .option("compression", "snappy") \
#             .parquet("/app/data/processed/stream")
import time

def write_batch(df, epoch_id):
    start_time = time.time()

    print(f"🚀 Processing Batch {epoch_id} START")

    if not df.rdd.isEmpty():
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet("/app/data/processed/stream")

    end_time = time.time()
    duration = end_time - start_time

    print(f"✅ Batch {epoch_id} completed in {duration:.2f} seconds")
    
query = (
    final_df.writeStream
    .foreachBatch(write_batch)
    .outputMode("update")
    .option("checkpointLocation", "/app/checkpoints/stream")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()