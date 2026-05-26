import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================================================
# WORKER CONFIG
# =========================================================

WORKER_ID = os.getenv("WORKER_ID", "1")

print(f"🚀 STARTING WORKER {WORKER_ID}")

# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName(f"KafkaConsumerWorker-{WORKER_ID}")

    # optimized for local machine
    .master("local[1]")

    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.default.parallelism", "3")

    .config(
        "spark.streaming.stopGracefullyOnShutdown",
        "true"
    )

    .config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
    )

    .config(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# INPUT SCHEMA
# =========================================================

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

    StructField(
        "data",
        StructType([
            StructField(
                "PowerDetail",
                ArrayType(
                    StructType([
                        StructField("Average", DoubleType()),
                        StructField("Minimum", DoubleType()),
                        StructField("Peak", DoubleType()),
                        StructField("Time", StringType())
                    ])
                )
            )
        ])
    ),

    StructField(
        "inventory_data",
        StructType([
            StructField("socket_count", IntegerType())
        ])
    )
])

# =========================================================
# READ FROM KAFKA
# =========================================================

df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "broker1:9092"
    )

    .option(
        "subscribe",
        "raw-server-metrics"
    )

    .option(
        "startingOffsets",
        "latest"
    )

    # SAME GROUP FOR BOTH WORKERS
    .option(
        "kafka.group.id",
        "atlas-stream-group"
    )

    .option(
        "failOnDataLoss",
        "false"
    )

    # optimized microbatch size
    .option(
        "maxOffsetsPerTrigger",
        "3000"
    )

    # kafka has 12 partitions
    .option(
        "minPartitions",
        "12"
    )

    .load()
)

# =========================================================
# RAW JSON
# =========================================================

raw_df = df.selectExpr(
    "CAST(value AS STRING) as raw_json"
)

# =========================================================
# PARSE JSON
# =========================================================

parsed = raw_df.select(
    col("raw_json"),

    from_json(
        col("raw_json"),
        input_schema
    ).alias("data")
)

# =========================================================
# VALID / INVALID
# =========================================================

valid_df = parsed.filter(
    col("data").isNotNull() &
    col("data.data.PowerDetail").isNotNull()
)

invalid_df = parsed.filter(
    col("data").isNull() |
    col("data.data.PowerDetail").isNull()
)

# =========================================================
# DLQ STREAM
# =========================================================

invalid_kafka_df = invalid_df.selectExpr(
    "CAST(null AS STRING) AS key",
    "raw_json AS value"
)

dlq_query = (
    invalid_kafka_df.writeStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "broker1:9092"
    )

    .option(
        "topic",
        "raw-server-metrics-dlq"
    )

    .option(
        "checkpointLocation",
        f"/app/checkpoints/dlq_{WORKER_ID}"
    )

    .outputMode("append")

    .start()
)

# =========================================================
# PIPELINE
# =========================================================

parsed_clean = valid_df.select("data.*")

flat = (
    parsed_clean

    .withColumn(
        "p",
        explode(col("data.PowerDetail"))
    )

    .withColumn(
        "event_time",
        to_timestamp(col("p.Time"))
    )
)

flat = flat.filter(
    col("event_time").isNotNull()
)

# =========================================================
# REPARTITION
# =========================================================

flat = flat.repartition(
    4,
    col("device_id")
)

# =========================================================
# WATERMARK + AGGREGATION
# =========================================================

agg = (
    flat

    .withWatermark(
        "event_time",
        "1 hour"
    )

    .groupBy(
        col("device_id"),
        to_date("event_time").alias("location_date")
    )

    .agg(

        avg("p.Average")
        .alias("avg_metric_value"),

        max("p.Average")
        .alias("max_metric_value"),

        min("p.Average")
        .alias("min_metric_value"),

        first("report_id", True)
        .alias("report_id"),

        first(
            "application_customer_id",
            True
        ).alias("application_customer_id"),

        first(
            "platform_customer_id",
            True
        ).alias("platform_customer_id"),

        first("status", True)
        .alias("status"),

        first("report_type", True)
        .alias("report_type"),

        first("error_reason", True)
        .alias("error_reason"),

        first("model", True)
        .alias("model"),

        first("tags", True)
        .alias("tags"),

        first("location_state", True)
        .alias("location_state"),

        first("location_country", True)
        .alias("location_country"),

        first("processor_vendor", True)
        .alias("processor_vendor"),

        first("server_generation", True)
        .alias("server_generation"),

        first("location_id", True)
        .alias("location_id"),

        first("location_name", True)
        .alias("location_name"),

        first("location_city", True)
        .alias("location_city"),

        first("server_name", True)
        .alias("server_name"),

        first(
            "inventory_data.socket_count",
            True
        ).alias("socket_count")
    )
)

# =========================================================
# FINAL OUTPUT
# =========================================================

final_df = agg.select(

    col("report_id"),

    col("device_id"),

    col("application_customer_id"),

    col("platform_customer_id"),

    col("status"),

    col("report_type"),

    col("error_reason"),

    col("avg_metric_value")
    .cast("double")
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

    lit("power_metrics")
    .alias("metric_id"),

    lit(None)
    .cast("string")
    .alias("cpu_inventory"),

    lit(None)
    .cast("string")
    .alias("memory_inventory"),

    lit(None)
    .cast("int")
    .alias("pcie_devices_count"),

    col("socket_count"),

    col("avg_metric_value"),

    col("max_metric_value"),

    col("min_metric_value"),

    col("location_date")
    .cast("string")
    .alias("metric_time"),

    lit(None)
    .cast("double")
    .alias("datetime"),

    lit(None)
    .cast("double")
    .alias("timeRangeEnd"),

    lit(None)
    .cast("double")
    .alias("amb_temp"),

    unix_timestamp()
    .cast("double")
    .alias("Insertiontime"),

    lit(None)
    .cast("double")
    .alias("co2_factor"),

    lit(None)
    .cast("double")
    .alias("energy_cost_factor"),

    lit(None)
    .cast("string")
    .alias("max_metric_time"),

    col("location_date")
    .cast("string")
    .alias("location_date"),

    col("location_date")
    .cast("string")
    .alias("inventory_date")
)

# =========================================================
# WRITE FUNCTION
# =========================================================

def write_batch(df, epoch_id):

    start_time = time.time()

    print(
        f"🚀 WORKER {WORKER_ID} "
        f"| BATCH {epoch_id} START"
    )

    if not df.rdd.isEmpty():

        (
            df.write
            .mode("append")
            .option("compression", "snappy")
            .parquet(
                f"/app/data/processed/stream/worker_{WORKER_ID}"
            )
        )

    duration = time.time() - start_time

    print(
        f"✅ WORKER {WORKER_ID} "
        f"| BATCH {epoch_id} "
        f"| completed in {duration:.2f}s"
    )

# =========================================================
# MAIN STREAM
# =========================================================

query = (
    final_df.writeStream

    .foreachBatch(write_batch)

    .outputMode("update")

    .option(
        "checkpointLocation",
        f"/app/checkpoints/stream_{WORKER_ID}"
    )

    .trigger(
        processingTime="30 seconds"
    )

    .start()
)

# =========================================================
# WAIT
# =========================================================

query.awaitTermination()