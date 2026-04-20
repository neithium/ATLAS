# # # # # from pyspark.sql import SparkSession
# # # # # from pyspark.sql.functions import *
# # # # # from pyspark.sql.types import *
# # # # # import logging

# # # # # # ---------------- LOGGING ----------------
# # # # # logging.basicConfig(
# # # # #     level=logging.INFO,
# # # # #     format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
# # # # # )
# # # # # logger = logging.getLogger("ATLAS")

# # # # # # ---------------- SPARK ----------------
# # # # # spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()
# # # # # spark.sparkContext.setLogLevel("ERROR")

# # # # # logging.getLogger("py4j").setLevel(logging.ERROR)
# # # # # logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)

# # # # # logger.info("STREAMING STARTED")

# # # # # # ---------------- SCHEMA ----------------
# # # # # schema = StructType([
# # # # #     StructField("device_id", StringType()),
# # # # #     StructField("timestamp", StringType()),
# # # # #     StructField("cpu", IntegerType()),
# # # # #     StructField("mem", IntegerType())
# # # # # ])

# # # # # # ---------------- READ KAFKA ----------------
# # # # # df = spark.readStream \
# # # # #     .format("kafka") \
# # # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # # #     .option("subscribe", "raw-server-metrics") \
# # # # #     .option("startingOffsets", "latest") \
# # # # #     .load()

# # # # # parsed = df.selectExpr("CAST(value AS STRING)") \
# # # # #     .select(from_json(col("value"), schema).alias("data")) \
# # # # #     .select("data.*")

# # # # # parsed = parsed.withColumn(
# # # # #     "event_time",
# # # # #     to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
# # # # # )

# # # # # # ---------------- AGG ----------------
  
# # # # # agg = parsed \
# # # # #     .withWatermark("event_time", "2 hours") \
# # # # #     .groupBy(
# # # # #         window(col("event_time"), "1 hour"),
# # # # #         col("device_id")
# # # # #     ) \
# # # # #     .agg(
# # # # #         avg("cpu").alias("avg_cpu"),
# # # # #         avg("mem").alias("avg_mem"),
# # # # #         count("*").alias("num_records")
# # # # #     )

# # # # # final_df = agg.select(
# # # # #     col("window.start").alias("window_start"),
# # # # #     col("window.end").alias("window_end"),
# # # # #     "device_id",
# # # # #     "avg_cpu",
# # # # #     "avg_mem",
# # # # #     "num_records"
# # # # # )

# # # # # # ---------------- WRITE ----------------
# # # # # # query = final_df \
# # # # #     # .writeStream \
# # # # #     # .format("parquet") \
# # # # #     # .outputMode("append") \
# # # # #     # .option("path", "/app/data/processed/stream") \
# # # # #     # .option("checkpointLocation", "/app/checkpoint/stream") \
# # # # #     # .trigger(processingTime="30 seconds") \
# # # # #     # .start()
# # # # # def log_and_write(batch_df, batch_id):
# # # # #     rows = batch_df.count()

# # # # #     print(f"🚀 STREAM BATCH | id={batch_id} | rows={rows}")

# # # # #     logger.info(f"STREAM BATCH | id={batch_id} | rows={rows}")

# # # # #     batch_df.write.mode("append").parquet("/app/data/processed/stream")

# # # # # query = final_df \
# # # # #     .writeStream \
# # # # #     .foreachBatch(log_and_write) \
# # # # #     .outputMode("append") \
# # # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # # #     .trigger(processingTime="30 seconds") \
# # # # #     .start()
# # # # # logger.info("Streaming query started")

# # # # # query.awaitTermination()
# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import *
# # # # from input_schema import input_schema

# # # # spark = SparkSession.builder \
# # # #     .appName("Streaming") \
# # # #     .master("local[6]") \
# # # #     .config("spark.sql.shuffle.partitions", "6") \
# # # #     .config("spark.default.parallelism", "6") \
# # # #     .getOrCreate()

# # # # spark.sparkContext.setLogLevel("ERROR")

# # # # # Kafka read
# # # # df = spark.readStream.format("kafka") \
# # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # #     .option("subscribe", "raw-server-metrics") \
# # # #     .option("startingOffsets", "latest") \
# # # #     .load()

# # # # # Parse JSON safely
# # # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # # #     .filter(col("data").isNotNull())

# # # # # Flatten
# # # # flat = json_df.select("data.*") \
# # # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # # Event time
# # # # flat = flat.withColumn("event_time", to_timestamp("p.Time"))

# # # # # Window aggregation
# # # # agg = flat \
# # # #     .withWatermark("event_time", "10 minutes") \
# # # #     .groupBy(
# # # #         window("event_time", "1 minute"),
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

# # # # # Final schema mapping
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

# # # # # One file per window
# # # # def write_batch(df, epoch_id):
# # # #     df.coalesce(1).write.mode("append").parquet("/app/data/processed/stream")

# # # # query = result.writeStream \
# # # #     .foreachBatch(write_batch) \
# # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # #     .trigger(processingTime="5 seconds") \
# # # #     .start()

# # # # query.awaitTermination()
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

# # # # print("🚀 Spark Streaming Started (Listening to Kafka...)")

# # # # # ---------------- READ FROM KAFKA ----------------
# # # # df = spark.readStream.format("kafka") \
# # # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # # #     .option("subscribe", "raw-server-metrics") \
# # # #     .option("startingOffsets", "latest") \
# # # #     .load()

# # # # # ---------------- PARSE JSON ----------------
# # # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # # #     .filter(col("data").isNotNull())

# # # # # ---------------- FLATTEN ----------------
# # # # flat = json_df.select("data.*") \
# # # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # # ---------------- FIXED TIMESTAMP PARSING ----------------
# # # # flat = flat.withColumn(
# # # #     "event_time",
# # # #     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# # # # )

# # # # # 🔍 DEBUG: verify parsing (will print in logs)
# # # # flat.select("p.Time", "event_time").writeStream \
# # # #     .format("console") \
# # # #     .outputMode("append") \
# # # #     .option("truncate", False) \
# # # #     .start()

# # # # # ---------------- AGGREGATION ----------------
# # # # agg = flat \
# # # #     .filter(col("event_time").isNotNull()) \
# # # #     .withWatermark("event_time", "10 seconds") \
# # # #     .groupBy(
# # # #         window("event_time", "1 minute"),
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
# # # #     .option("checkpointLocation", "/app/checkpoint/stream") \
# # # #     .trigger(processingTime="5 seconds") \
# # # #     .start()

# # # # query.awaitTermination()
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import *
# # # from input_schema import input_schema

# # # # ---------------- SPARK SESSION ----------------
# # # spark = SparkSession.builder \
# # #     .appName("Streaming") \
# # #     .master("local[6]") \
# # #     .config("spark.sql.shuffle.partitions", "6") \
# # #     .config("spark.default.parallelism", "6") \
# # #     .getOrCreate()

# # # spark.sparkContext.setLogLevel("ERROR")

# # # print("🚀 Spark Streaming Started")

# # # # ---------------- READ FROM KAFKA ----------------
# # # df = spark.readStream.format("kafka") \
# # #     .option("kafka.bootstrap.servers", "broker1:9092") \
# # #     .option("subscribe", "raw-server-metrics") \
# # #     .option("startingOffsets", "latest") \
# # #     .option("kafka.group.id", "atlas-processor-streaming-group") \
# # #     .option("failOnDataLoss", "false") \
# # #     .load()

# # # # ---------------- PARSE JSON ----------------
# # # json_df = df.selectExpr("CAST(value AS STRING) as json") \
# # #     .select(from_json(col("json"), input_schema).alias("data")) \
# # #     .filter(col("data").isNotNull())

# # # # ---------------- FLATTEN ----------------
# # # flat = json_df.select("data.*") \
# # #     .withColumn("p", explode(col("data.PowerDetail")))

# # # # ---------------- TIMESTAMP (FIXED) ----------------
# # # flat = flat.withColumn(
# # #     "event_time",
# # #     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# # # )

# # # # ---------------- AGGREGATION ----------------
# # # agg = flat \
# # #     .filter(col("event_time").isNotNull()) \
# # #     .withWatermark("event_time", "10 seconds") \
# # #     .groupBy(
# # #         window("event_time", "10 seconds"),   # 🔥 shorter window
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

# # # # ---------------- FINAL OUTPUT ----------------
# # # result = agg.select(
# # #     "report_id",
# # #     "device_id",
# # #     "application_customer_id",
# # #     "platform_customer_id",
# # #     "status",
# # #     "report_type",
# # #     "error_reason",
# # #     col("avg_metric_value").alias("MetricValue"),
# # #     "model",
# # #     "tags",
# # #     "location_state",
# # #     "location_country",
# # #     "processor_vendor",
# # #     "server_generation",
# # #     "location_id",
# # #     "location_name",
# # #     "location_city",
# # #     "server_name",
# # #     lit("power").alias("metric_id"),
# # #     lit("cpu").alias("cpu_inventory"),
# # #     lit("memory").alias("memory_inventory"),
# # #     lit(0).alias("pcie_devices_count"),
# # #     "socket_count",
# # #     "avg_metric_value",
# # #     "max_metric_value",
# # #     "min_metric_value",
# # #     col("window.start").cast("string").alias("metric_time"),
# # #     unix_timestamp("window.start").cast("double").alias("datetime"),
# # #     unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
# # #     lit(25.0).alias("amb_temp"),
# # #     unix_timestamp().cast("double").alias("Insertiontime"),
# # #     lit(0.5).alias("co2_factor"),
# # #     lit(1.2).alias("energy_cost_factor"),
# # #     col("window.end").cast("string").alias("max_metric_time"),
# # #     to_date("window.start").cast("string").alias("location_date"),
# # #     to_date("window.start").cast("string").alias("inventory_date")
# # # )

# # # # ---------------- WRITE ----------------
# # # def write_batch(df, epoch_id):
# # #     count = df.count()
# # #     print(f"🚀 Batch {epoch_id} | rows={count}")

# # #     if count > 0:
# # #         df.write.mode("append").parquet("/app/data/processed/stream")

# # # # ---------------- START STREAM ----------------
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

# # print("🚀 Final Streaming Started (Schema Strict)")

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

# # # ---------------- AGG ----------------
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

# # # ---------------- FINAL SELECT (STRICT ORDER + TYPES) ----------------
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

# # # ---------------- WRITE ----------------
# # def write_batch(df, epoch_id):
# #     print(f"🚀 Batch {epoch_id}")

# #     if not df.isEmpty():
# #         df.write \
# #             .mode("append") \
# #             .option("compression", "snappy") \
# #             .parquet("/app/data/processed/stream")

# # # ---------------- STREAM ----------------
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
#     .appName("Streaming-Final") \
#     .master("local[6]") \
#     .config("spark.sql.shuffle.partitions", "12") \
#     .config("spark.default.parallelism", "12") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# print("🚀 Final Streaming Started (Schema + Count + Optimized)")

# # ---------------- READ KAFKA ----------------
# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "broker1:9092") \
#     .option("subscribe", "raw-server-metrics") \
#     .option("startingOffsets", "latest") \
#     .option("kafka.group.id", "atlas-processor-streaming-group") \
#     .option("failOnDataLoss", "false") \
#     .load()

# # ---------------- PARSE ----------------
# json_df = df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json(col("json"), input_schema).alias("data")) \
#     .filter(col("data").isNotNull())

# # ---------------- FLATTEN ----------------
# flat = json_df.select("data.*") \
#     .withColumn("p", explode(col("data.PowerDetail")))

# # ---------------- PARALLELISM ----------------
# flat = flat.repartition(12, col("device_id"))

# # ---------------- TIMESTAMP ----------------
# flat = flat.withColumn(
#     "event_time",
#     to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
# ).filter(col("event_time").isNotNull())

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

#         first("report_id").alias("report_id"),
#         first("application_customer_id").alias("application_customer_id"),
#         first("platform_customer_id").alias("platform_customer_id"),
#         first("status").alias("status"),
#         first("report_type").alias("report_type"),
#         first("error_reason").alias("error_reason"),
#         first("model").alias("model"),
#         first("tags").alias("tags"),
#         first("location_state").alias("location_state"),
#         first("location_country").alias("location_country"),
#         first("processor_vendor").alias("processor_vendor"),
#         first("server_generation").alias("server_generation"),
#         first("location_id").alias("location_id"),
#         first("location_name").alias("location_name"),
#         first("location_city").alias("location_city"),
#         first("server_name").alias("server_name"),
#         first("inventory_data.socket_count").alias("socket_count")
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

# # ---------------- WRITE FUNCTION ----------------
# def write_batch(df, epoch_id):
#     # Cache to avoid recomputation
#     # df.persist()

#     # row_count = df.count()
#     # print(f"🚀 Batch {epoch_id} | rows received = {row_count}")
#     print(f"🚀 Batch {epoch_id} received")

#     if row_count > 0:
#         df.write \
#             .mode("append") \
#             .option("compression", "snappy") \
#             .parquet("/app/data/processed/stream")

#     df.unpersist()

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
from input_schema import input_schema

# ---------------- SPARK ----------------
spark = SparkSession.builder \
    .appName("Streaming-Final-Correct") \
    .master("local[6]") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.default.parallelism", "12") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("🚀 Streaming Started (FINAL CORRECT)")

# ---------------- READ KAFKA ----------------
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092") \
    .option("subscribe", "raw-server-metrics") \
    .option("startingOffsets", "latest") \
    .option("kafka.group.id", "atlas-processor-streaming-group") \
    .option("failOnDataLoss", "false") \
    .load()

# ---------------- PARSE ----------------
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), input_schema).alias("data")) \
    .filter(col("data").isNotNull())

# ---------------- FLATTEN (NO DROP, NO HACKS) ----------------
flat = json_df.select("data.*") \
    .withColumn("p", explode_outer(col("data.PowerDetail")))

# ---------------- OPTIONAL: FILTER FRESH (ONLY IF YOU WANT) ----------------
# COMMENT THIS IF YOU WANT ALL DATA
# flat = flat.filter(col("p.is_fresh") == True)

# ---------------- PARALLELISM ----------------
flat = flat.repartition(12, col("device_id"))

# ---------------- TIMESTAMP ----------------
flat = flat.withColumn(
    "event_time",
    to_timestamp(col("p.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
)

# fallback (safe)
flat = flat.withColumn(
    "event_time",
    when(col("event_time").isNull(), current_timestamp())
    .otherwise(col("event_time"))
)

# ---------------- AGGREGATION ----------------
agg = flat \
    .withWatermark("event_time", "20 seconds") \
    .groupBy(
        window("event_time", "20 seconds"),
        "device_id"
    ) \
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

# ---------------- FINAL SELECT (STRICT SCHEMA) ----------------
result = agg.select(
    col("report_id").cast("string"),
    col("device_id").cast("string"),
    col("application_customer_id").cast("string"),
    col("platform_customer_id").cast("string"),
    col("status").cast("boolean"),
    col("report_type").cast("string"),
    col("error_reason").cast("string"),
    col("avg_metric_value").cast("double").alias("MetricValue"),
    col("model").cast("string"),
    col("tags").cast("string"),
    col("location_state").cast("string"),
    col("location_country").cast("string"),
    col("processor_vendor").cast("string"),
    col("server_generation").cast("string"),
    col("location_id").cast("string"),
    col("location_name").cast("string"),
    col("location_city").cast("string"),
    col("server_name").cast("string"),
    lit("power").cast("string").alias("metric_id"),
    lit("cpu").cast("string").alias("cpu_inventory"),
    lit("memory").cast("string").alias("memory_inventory"),
    lit(0).cast("int").alias("pcie_devices_count"),
    col("socket_count").cast("int"),
    col("avg_metric_value").cast("double"),
    col("max_metric_value").cast("double"),
    col("min_metric_value").cast("double"),
    col("window.start").cast("string").alias("metric_time"),
    unix_timestamp("window.start").cast("double").alias("datetime"),
    unix_timestamp("window.end").cast("double").alias("timeRangeEnd"),
    lit(25.0).cast("double").alias("amb_temp"),
    unix_timestamp().cast("double").alias("Insertiontime"),
    lit(0.5).cast("double").alias("co2_factor"),
    lit(1.2).cast("double").alias("energy_cost_factor"),
    col("window.end").cast("string").alias("max_metric_time"),
    to_date("window.start").cast("string").alias("location_date"),
    to_date("window.start").cast("string").alias("inventory_date")
)

# ---------------- WRITE ----------------
def write_batch(df, epoch_id):
    print(f"🚀 Batch {epoch_id} received")

    if not df.rdd.isEmpty():
        df.coalesce(6) \
          .write \
          .mode("append") \
          .option("compression", "snappy") \
          .parquet("/app/data/processed/stream")

# ---------------- START STREAM ----------------
query = result.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "/app/checkpoint/stream") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()