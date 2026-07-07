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

    print("Starting Batch Job...")

    INPUT_PATH = "/app/data/raw/production"

    OUTPUT_PATH = "/app/data/processed/batch_out"

    # =====================================================
    # READ PARQUET
    # =====================================================

    df = (
        spark.read
        .schema(input_schema)
        .parquet(INPUT_PATH)
    )

    print("Raw parquet loaded")

    # =====================================================
    # EMPTY CHECK
    # =====================================================

    if df.limit(1).count() == 0:

        print("No parquet data found")

        spark.stop()

        return

    # =====================================================
    # REPARTITION
    # =====================================================

    df = df.repartition(8)

    print("Repartition completed")

    # =====================================================
    # EXPLODE POWER DETAIL
    # =====================================================

    flat = df.withColumn(

        "power",

        explode("data.PowerDetail")
    )

    print("PowerDetail exploded")

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

    print("Aggregations completed")

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

    print("Final output dataframe ready")

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

    print(f"Batch parquet written → {OUTPUT_PATH}")

    # =====================================================
    # STOP SPARK
    # =====================================================

    spark.stop()

    print("Batch Job Completed Successfully")

# =========================================================
# MAIN
# =====================================================

if __name__ == "__main__":

    run_batch()



















