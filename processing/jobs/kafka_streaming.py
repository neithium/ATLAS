import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================================================
# WORKER CONFIG
# =========================================================

WORKER_ID = os.getenv("WORKER_ID", "1")

print(f"STARTING WORKER {WORKER_ID}")

# =========================================================
# SPARK SESSION
# =========================================================

spark = (

    SparkSession.builder

    .appName(f"KafkaConsumerWorker-{WORKER_ID}")

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

                        StructField(
                            "Average",
                            DoubleType()
                        ),

                        StructField(
                            "Minimum",
                            DoubleType()
                        ),

                        StructField(
                            "Peak",
                            DoubleType()
                        ),

                        StructField(
                            "Time",
                            StringType()
                        )
                    ])
                )
            )
        ])
    ),

    StructField(

        "inventory_data",

        StructType([

            StructField(
                "socket_count",
                IntegerType()
            )
        ])
    )
])

# =========================================================
# READ FROM KAFKA
# =========================================================

KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

df = (

    spark.readStream

    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "broker1:9092"
    )

    .option(
        "subscribe",
        "raw-server-metrics,raw-server-metrics-retry"
    )

    .option(
        "startingOffsets",
        KAFKA_STARTING_OFFSETS
    )

    .option(
        "groupIdPrefix",
        "atlas-stream-group"
    )

    .option(
        "failOnDataLoss",
        "false"
    )

    .option(
        "maxOffsetsPerTrigger",
        "3000"
    )

    .option(
        "minPartitions",
        "12"
    )

    .load()
)

# =========================================================
# RAW JSON
# =========================================================

raw_df = df.select(

    col("topic"),

    col("partition"),

    col("offset"),

    col("timestamp").alias("kafka_timestamp"),

    expr("CAST(value AS STRING)")
    .alias("raw_json")
)

# =========================================================
# PARSE JSON
# =========================================================

parsed = raw_df.select(

    col("topic"),

    col("partition"),

    col("offset"),

    col("kafka_timestamp"),

    col("raw_json"),

    from_json(
        col("raw_json"),
        input_schema
    ).alias("data")
)

# =========================================================
# ERROR CLASSIFICATION ENGINE
# =========================================================

classified_df = parsed.withColumn(

    "error_type",

    when(
        col("data").isNull(),

        when(
            col("raw_json").contains("socket_count"),
            "INVALID_SOCKET_COUNT"
        ).otherwise("INVALID_SCHEMA")
    )
    .when(
        col("data.device_id").isNull(),
        "MISSING_DEVICE_ID"
    )

    .when(
        col("data.data.PowerDetail").isNull(),
        "MISSING_POWERDETAIL"
    )

    .when(
        col("data.inventory_data.socket_count")
        .cast("int")
        .isNull(),

        "INVALID_SOCKET_COUNT"
    )

    .otherwise("VALID")
)

# =========================================================
# VALID / INVALID SPLIT
# =========================================================

valid_df = classified_df.filter(
    col("error_type") == "VALID"
)

invalid_df = classified_df.filter(
    col("error_type") != "VALID"
)

# =========================================================
# DLQ PAYLOAD
# =========================================================

invalid_kafka_df = invalid_df.select(

    to_json(

        struct(

            col("raw_json"),

            col("error_type"),

            col("topic"),

            col("partition"),

            col("offset"),

            col("kafka_timestamp"),

            current_timestamp()
            .alias("failed_at"),

            lit(WORKER_ID)
            .alias("worker_id")
        )

    ).alias("value")

).selectExpr(

    "CAST(null AS STRING) AS key",

    "CAST(value AS STRING) AS value"
)

# =========================================================
# DLQ STREAM
# =========================================================

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

print("DLQ Stream Started")

# =========================================================
# CONTINUE VALID PIPELINE
# =========================================================

parsed_clean = valid_df.select("data.*")

# =========================================================
# EXPLODE POWER DETAIL
# =========================================================

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

# =========================================================
# REMOVE INVALID TIMESTAMPS
# =========================================================

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

        to_date("event_time")
        .alias("location_date")
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

    round(
        col("avg_metric_value"),
        2
    ).alias("avg_metric_value"),

    round(
        col("max_metric_value"),
        2
    ).alias("max_metric_value"),

    round(
        col("min_metric_value"),
        2
    ).alias("min_metric_value"),

    col("location_date")
    .cast("string")
    .alias("metric_time"),

    unix_timestamp(
        col("location_date")
    ).cast("double")
    .alias("datetime"),

    (
        unix_timestamp(
            col("location_date")
        ) + 60
    ).cast("double")
    .alias("timeRangeEnd"),

    lit(None)
    .cast("double")
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

    col("location_date")
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
        f"WORKER {WORKER_ID} "
        f"| BATCH {epoch_id} START"
    )

    row_count = df.count()

    print(
        f"WORKER {WORKER_ID} "
        # f"| RECORDS {row_count}"
    )

    if row_count > 0:

        (
            df.write

            .mode("append")

            .option(
                "compression",
                "snappy"
            )

            .parquet(
                f"/app/data/processed/stream/worker_{WORKER_ID}"
            )
        )

    duration = time.time() - start_time

    throughput = (
        row_count / duration
        if duration > 0 else 0
    )

    print(
        f"WORKER {WORKER_ID} "
        f"| BATCH {epoch_id} "
        f"| completed in {duration:.2f}s "
        f"| throughput={throughput:.2f} rec/sec"
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
# STARTED
# =========================================================

print("Main Streaming Query Started")

# =========================================================
# WAIT
# =========================================================

query.awaitTermination()


