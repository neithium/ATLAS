"""
ATLAS Delta Lake Module - Mock Telemetry Data Generator
================================================================================
Simulates real-world 7-day rolling window telemetry data for deduplication testing.

Real-World Pattern:
- Each incoming file represents ONE DAY's telemetry batch
- Each file contains 2016 rows per device (7-day rolling window)
- Only 288 rows are new (that day's data at 5-min intervals)
- 1728 rows are duplicates from the previous 6 days
- Expected deduplication ratio: ~85.7% (1728/2016)

Data Generation Strategy:
- "file_date" = the date the file was received/processed
- "metric_time" = actual timestamp of the metric (spans 7 days)
- Each file contains: metric_time from (file_date - 6 days) to file_date
- Deduplication key: (device_id, metric_time, application_customer_id)

Schema (flattened from PowerDetail array):
- device_id: Server identifier (e.g., SRV-000001)
- metric_time: Timestamp of the metric reading
- application_customer_id: Customer app identifier
- platform_customer_id: Platform identifier
- report_type: Type of telemetry report
- file_date: Date file was received (partition column)
- MetricValue: The actual metric value

Example with 3 daily files:
- File Day 8:  metric_time spans Day 2-8, file_date=Day 8
- File Day 9:  metric_time spans Day 3-9, file_date=Day 9
- File Day 10: metric_time spans Day 4-10, file_date=Day 10
- Overlap: Days 3-8 appear in all three files → duplicates to be removed
"""

from datetime import datetime, timedelta
import argparse
import math
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    broadcast,
    col,
    dayofyear,
    element_at,
    explode,
    floor,
    format_string,
    expr,
    hour,
    lit,
    minute,
    pmod,
    sequence,
    sin,
    to_date,
    to_timestamp,
    date_format,
)


def create_spark_session() -> SparkSession:
    """Create Spark session tuned for container-constrained generation."""
    spark = (
        SparkSession.builder.appName("ATLAS-Benchmark-DataGenerator")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_metric_profile(spark: SparkSession):
    """
    Create a reusable 288-point daily profile (5-min slots) to reduce CPU load.

    Instead of generating a random value for every single row, we precompute one
    daily profile and reuse it across all 7 days and all batches with small
    deterministic adjustments.
    """
    return (
        spark.range(288)
        .withColumnRenamed("id", "slot_index")
        .withColumn(
            "base_metric",
            (lit(220.0) + lit(45.0) * sin(col("slot_index") / lit(14.0)) + (pmod(col("slot_index"), lit(17)) * lit(0.85))).cast("double"),
        )
    )


def build_daily_file_df(
    spark: SparkSession,
    batch_start: int,
    batch_end: int,
    file_date: datetime,
    profile_df,
):
    """
    Build a DataFrame simulating ONE DAY's incoming telemetry file.
    
    Each file contains a 7-day rolling window:
    - metric_time spans from (file_date - 6 days) to file_date
    - 2016 rows per device (288 rows/day × 7 days)
    - file_date column marks when this file was received
    
    Args:
        spark: SparkSession
        batch_start: Starting device index
        batch_end: Ending device index (exclusive)
        file_date: The date this "file" was received (determines the 7-day window)
        profile_df: Pre-computed metric profile for efficiency
    """
    # 7-day window: (file_date - 6 days) through file_date
    window_start = file_date - timedelta(days=6)
    window_end = file_date + timedelta(hours=23, minutes=55)  # End of file_date
    
    start_ts = window_start.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = window_end.strftime("%Y-%m-%d %H:%M:%S")
    
    # Build device dimension
    device_df = (
        spark.range(batch_start, batch_end)
        .withColumnRenamed("id", "device_num")
        .withColumn("device_id", format_string("SRV-%06d", col("device_num") + lit(1)))
        .withColumn(
            "application_customer_id",
            element_at(
                array(
                    lit("APP-001"),
                    lit("APP-017"),
                    lit("APP-113"),
                    lit("APP-226"),
                    lit("APP-67890"),
                ),
                (pmod(col("device_num"), lit(5)) + lit(1)).cast("int"),
            ),
        )
        .withColumn(
            "platform_customer_id",
            element_at(
                array(
                    lit("PLAT-001"),
                    lit("PLAT-021"),
                    lit("PLAT-101"),
                    lit("PLAT-12345"),
                    lit("PLAT-907"),
                ),
                (pmod(col("device_num"), lit(5)) + lit(1)).cast("int"),
            ),
        )
        .withColumn(
            "report_type",
            element_at(
                array(
                    lit("power_metrics"),
                    lit("thermal_metrics"),
                    lit("cpu_metrics"),
                    lit("sustainability_metrics"),
                ),
                (pmod(col("device_num"), lit(4)) + lit(1)).cast("int"),
            ),
        )
    )

    # Build time dimension - 7 days at 5-minute intervals = 2016 timestamps
    time_df = spark.range(1).select(
        explode(
            sequence(
                to_timestamp(lit(start_ts)),
                to_timestamp(lit(end_ts)),
                expr("INTERVAL 5 MINUTES"),
            )
        ).alias("metric_time")
    )

    # Cartesian product: devices × timestamps
    expanded_df = device_df.crossJoin(time_df)

    # Calculate slot index for metric profile lookup
    slot_index_expr = (hour(col("metric_time")) * lit(12) + floor(minute(col("metric_time")) / lit(5))).cast("long")

    # Join with metric profile and compute values
    with_values_df = (
        expanded_df.withColumn("slot_index", slot_index_expr)
        .join(broadcast(profile_df), on="slot_index", how="left")
        .withColumn(
            "MetricValue",
            (
                col("base_metric")
                + (pmod(col("device_num"), lit(19)) * lit(0.22))
                + (pmod(dayofyear(col("metric_time")), lit(7)) * lit(0.31))
            ).cast("double"),
        )
        # file_date = the day this file was "received" (partition key for incoming batches)
        .withColumn("file_date", lit(file_date.strftime("%Y-%m-%d")))
        # partition_date = actual date of the metric (for Delta Lake partitioning after dedup)
        .withColumn("partition_date", date_format(to_date(col("metric_time")), "yyyy-MM-dd"))
    )

    return with_values_df.select(
        "device_id",
        "metric_time",
        "application_customer_id",
        "platform_customer_id",
        "report_type",
        "file_date",
        "partition_date",
        "MetricValue",
    )


def generate_benchmark_data(
    spark: SparkSession,
    output_root: str,
    total_devices: int,
    batch_size: int,
    start_date: datetime,
    num_days: int = 7,
):
    """
    Generate benchmark data simulating real 7-day rolling window pattern.
    
    Creates N daily files, each containing 2016 rows/device (7-day windows).
    Consecutive files have 6/7 overlap → 85.7% deduplication expected.
    
    Args:
        spark: SparkSession
        output_root: Root directory for output
        total_devices: Number of devices to generate
        batch_size: Devices per write batch (for memory management)
        start_date: First file's date (the window will start 6 days before)
        num_days: Number of daily files to generate
    """
    output_path = f"{output_root.rstrip('/')}/benchmark_data"
    
    rows_per_device_per_file = 2016  # 7 days × 288 rows/day
    total_raw_rows = rows_per_device_per_file * total_devices * num_days
    
    # After dedup: first file = 2016 unique, subsequent files = 288 new each
    # Expected unique: 2016 + (num_days - 1) * 288
    expected_unique_per_device = 2016 + (num_days - 1) * 288
    expected_unique_rows = expected_unique_per_device * total_devices
    expected_duplicates = total_raw_rows - expected_unique_rows
    expected_dedup_ratio = (expected_duplicates / total_raw_rows) * 100 if total_raw_rows > 0 else 0

    print("=" * 80)
    print("ATLAS Benchmark Generator - 7-Day Rolling Window Simulation")
    print("=" * 80)
    print(f"\nOutput path:              {output_path}")
    print(f"Total devices:            {total_devices:,}")
    print(f"Device batch size:        {batch_size:,}")
    print(f"Number of daily files:    {num_days}")
    print(f"Rows per device per file: {rows_per_device_per_file:,} (7 days × 288)")
    print(f"\nReal-World Simulation:")
    print(f"- Each file contains 7-day rolling window")
    print(f"- File for Day N: metric_time spans (Day N-6) to Day N")
    print(f"- New rows per file: 288/device (1 day)")
    print(f"- Duplicate rows per file: 1,728/device (6 days)")
    print(f"\nExpected Results After Deduplication:")
    print(f"- Total raw input rows:   {total_raw_rows:,}")
    print(f"- Expected unique rows:   {expected_unique_rows:,}")
    print(f"- Expected duplicates:    {expected_duplicates:,}")
    print(f"- Expected dedup ratio:   {expected_dedup_ratio:.1f}%")
    print(f"\nFile date range: {start_date.strftime('%Y-%m-%d')} → {(start_date + timedelta(days=num_days-1)).strftime('%Y-%m-%d')}")

    pipeline_start = time.perf_counter()
    profile_df = build_metric_profile(spark)
    
    device_batch_count = int(math.ceil(total_devices / batch_size))

    # Generate each daily file
    for day_idx in range(num_days):
        file_date = start_date + timedelta(days=day_idx)
        file_date_str = file_date.strftime("%Y-%m-%d")
        
        print("\n" + "=" * 80)
        print(f"DAILY FILE {day_idx + 1}/{num_days}: file_date = {file_date_str}")
        print(f"  → metric_time window: {(file_date - timedelta(days=6)).strftime('%Y-%m-%d')} to {file_date_str}")
        print("=" * 80)
        
        # For each daily file, process devices in batches
        for batch_idx, dev_start in enumerate(range(0, total_devices, batch_size), start=1):
            dev_end = min(dev_start + batch_size, total_devices)
            current_batch_devices = dev_end - dev_start
            
            # Mode: overwrite for first batch of first file, append otherwise
            mode = "overwrite" if (day_idx == 0 and batch_idx == 1) else "append"
            
            batch_timer = time.perf_counter()
            print(f"\n  Batch {batch_idx}/{device_batch_count}: devices {dev_start + 1:,}..{dev_end:,}")
            
            batch_df = build_daily_file_df(
                spark=spark,
                batch_start=dev_start,
                batch_end=dev_end,
                file_date=file_date,
                profile_df=profile_df,
            )
            
            # Partition by file_date for pipeline processing
            (
                batch_df.repartition(4, col("file_date"))
                .write.mode(mode)
                .option("compression", "snappy")
                .partitionBy("file_date")
                .parquet(output_path)
            )
            
            rows_written = current_batch_devices * rows_per_device_per_file
            elapsed = time.perf_counter() - batch_timer
            throughput = rows_written / elapsed if elapsed > 0 else 0
            print(f"  Rows written: {rows_written:,} | Time: {elapsed:.2f}s | Throughput: {throughput:,.0f} rows/s")

    total_elapsed = time.perf_counter() - pipeline_start

    print("\n" + "=" * 80)
    print("Benchmark Generation Complete")
    print("=" * 80)
    print(f"Output path:            {output_path}")
    print(f"Total raw rows:         {total_raw_rows:,}")
    print(f"Expected unique after dedup: {expected_unique_rows:,}")
    print(f"Expected dedup ratio:   {expected_dedup_ratio:.1f}%")
    print(f"Total elapsed:          {total_elapsed:.2f}s")
    print("=" * 80)


def generate_legacy_demo_data(spark: SparkSession, output_root: str):
    """Compatibility mode for existing merge demo pipeline input layout."""
    output_root = output_root.rstrip("/")

    baseline_start = datetime(2026, 2, 26, 0, 0, 0)
    overlap_start = baseline_start + timedelta(days=1)

    profile_df = build_metric_profile(spark)

    # File 1: 7-day window ending on baseline_start + 6 days
    df1 = build_daily_file_df(
        spark=spark,
        batch_start=100,
        batch_end=101,
        file_date=baseline_start + timedelta(days=6),  # Window: Feb 26 - Mar 4
        profile_df=profile_df,
    )
    
    # File 2: 7-day window shifted by 1 day
    df2 = build_daily_file_df(
        spark=spark,
        batch_start=100,
        batch_end=101,
        file_date=baseline_start + timedelta(days=7),  # Window: Feb 27 - Mar 5
        profile_df=profile_df,
    )

    df1.drop("file_date").write.mode("overwrite").parquet(f"{output_root}/file1_baseline.parquet")
    df2.drop("file_date").write.mode("overwrite").parquet(f"{output_root}/file2_overlap.parquet")

    print("\nLegacy demo files generated:")
    print(f"- {output_root}/file1_baseline.parquet (2016 rows)")
    print(f"- {output_root}/file2_overlap.parquet (2016 rows, 1728 duplicates expected)")
    print(f"- Expected dedup ratio: 85.7% (1728/2016)")


def parse_args():
    parser = argparse.ArgumentParser(description="ATLAS Benchmark Data Generator")
    parser.add_argument("--output", type=str, default="/raw", help="Root output directory")
    parser.add_argument("--mode", type=str, choices=["benchmark", "legacy"], default="benchmark")
    parser.add_argument("--devices", type=int, default=100000, help="Number of devices")
    parser.add_argument("--batch-size", type=int, default=10000, help="Devices per batch")
    parser.add_argument("--num-days", type=int, default=7, help="Number of daily files to generate")
    parser.add_argument("--start-date", type=str, default="2026-03-01", help="First file date (YYYY-MM-DD)")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session()

    try:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")

        if args.mode == "benchmark":
            generate_benchmark_data(
                spark=spark,
                output_root=args.output,
                total_devices=args.devices,
                batch_size=args.batch_size,
                start_date=start_dt,
                num_days=args.num_days,
            )
        else:
            generate_legacy_demo_data(spark=spark, output_root=args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
