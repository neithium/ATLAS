from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType, LongType, ArrayType

input_schema = StructType([
    StructField("data", StructType([
        StructField("Id", StringType(), True),
        StructField("Average", DoubleType(), True),
        StructField("Maximum", DoubleType(), True),
        StructField("Minimum", DoubleType(), True),
        StructField("Name", StringType(), True),
        StructField("PowerDetail", ArrayType(StructType([
            StructField("AmbTemp", DoubleType(), True),
            StructField("Average", DoubleType(), True),
            StructField("CpuAvgFreq", LongType(), True),
            StructField("CpuMax", LongType(), True),
            StructField("CpuPwrSavLim", LongType(), True),
            StructField("CpuUtil", LongType(), True),
            StructField("CpuWatts", LongType(), True),
            StructField("GpuWatts", LongType(), True),
            StructField("Minimum", LongType(), True),
            StructField("Peak", LongType(), True),
            StructField("Time", StringType(), True),
        ])), True)
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
    StructField("inventory_data", StructType([
        StructField("cpu_count", IntegerType(), True),
        StructField("socket_count", IntegerType(), True),
        StructField("cpu_inventory", ArrayType(StructType([
            StructField("model", StringType(), True),
            StructField("speed", IntegerType(), True),
            StructField("total_cores", IntegerType(), True)
        ])), True),
        StructField("memory_inventory", ArrayType(StructType([
            StructField("memory_size", IntegerType(), True),
            StructField("operating_freq", IntegerType(), True),
            StructField("memory_device_type", StringType(), True)
        ])), True)
    ]), True)
])