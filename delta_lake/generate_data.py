"""
ATLAS Delta Lake Module - Mock Telemetry Data Generator

Primary mode (benchmark):
- Generates scale-test data for up to 100,000 devices (or more)
- Memory-safe iterative writes in batches (default 10,000 devices/batch)
- 7 days at 5-minute cadence (2,016 rows/device)
- Output schema:
  device_id (string), metric_time (timestamp), application_customer_id (string),
  platform_customer_id (string), report_type (string), partition_date (date),
  MetricValue (double)
- Writes partitioned Parquet store at: /raw/benchmark_data/ (partitioned by partition_date)

Compatibility mode (legacy):
- Generates file1_baseline.parquet and file2_overlap.parquet for existing merge demo.
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


def build_device_batch_df(
    spark: SparkSession,
    batch_start: int,
    batch_end: int,
    start_ts: str,
    end_ts: str,
    profile_df,
):
    """Build one batch DataFrame using range + explode(sequence)."""
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

    time_df = spark.range(1).select(
        explode(
            sequence(
                to_timestamp(lit(start_ts)),
                to_timestamp(lit(end_ts)),
                expr("INTERVAL 5 MINUTES"),
            )
        ).alias("metric_time")
    )

    # Cartesian expansion done per batch only (bounded), avoiding OOM from a giant global join.
    expanded_df = device_df.crossJoin(time_df)

    slot_index_expr = (hour(col("metric_time")) * lit(12) + floor(minute(col("metric_time")) / lit(5))).cast("long")

    # Broadcast tiny 288-row profile and add deterministic per-device/day variation.
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
        .withColumn("partition_date", to_date(col("metric_time")))
    )

    return with_values_df.select(
        "device_id",
        "metric_time",
        "application_customer_id",
        "platform_customer_id",
        "report_type",
        "partition_date",
        "MetricValue",
    )


def generate_benchmark_data(
    spark: SparkSession,
    output_root: str,
    total_devices: int,
    batch_size: int,
    start_time: datetime,
):
    """Generate benchmark data iteratively and append in chunks."""
    output_path = f"{output_root.rstrip('/')}/benchmark_data"
    end_time = start_time + timedelta(days=7) - timedelta(minutes=5)

    start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")

    rows_per_device = (7 * 24 * 60) // 5
    expected_total_rows = rows_per_device * total_devices

    print("=" * 80)
    print("ATLAS Benchmark Generator")
    print("=" * 80)
    print(f"Output path:           {output_path}")
    print(f"Total devices:         {total_devices:,}")
    print(f"Batch size:            {batch_size:,}")
    print(f"Rows per device:       {rows_per_device:,}")
    print(f"Expected total rows:   {expected_total_rows:,}")
    print(f"Time window:           {start_ts} -> {end_ts}")
    print("Schema:                device_id, metric_time, application_customer_id,")
    print("                      platform_customer_id, report_type, partition_date, MetricValue")

    pipeline_start = time.perf_counter()
    profile_df = build_metric_profile(spark)

    batch_count = int(math.ceil(total_devices / batch_size))

    for batch_idx, batch_start in enumerate(range(0, total_devices, batch_size), start=1):
        batch_end = min(batch_start + batch_size, total_devices)
        current_batch_size = batch_end - batch_start
        mode = "overwrite" if batch_idx == 1 else "append"

        batch_timer = time.perf_counter()
        print("\n" + "-" * 80)
        print(f"Batch {batch_idx}/{batch_count}: devices {batch_start + 1:,}..{batch_end:,}")
        print("-" * 80)

        batch_df = build_device_batch_df(
            spark=spark,
            batch_start=batch_start,
            batch_end=batch_end,
            start_ts=start_ts,
            end_ts=end_ts,
            profile_df=profile_df,
        )

        # Keep output file sizes sane and partition writes by date.
        (
            batch_df.repartition(14, col("partition_date"))
            .write.mode(mode)
            .option("compression", "snappy")
            .partitionBy("partition_date")
            .parquet(output_path)
        )

        rows_written = current_batch_size * rows_per_device
        elapsed = time.perf_counter() - batch_timer
        print(f"Batch rows written:    {rows_written:,}")
        print(f"Batch elapsed:         {elapsed:.2f}s")

    total_elapsed = time.perf_counter() - pipeline_start

    print("\n" + "=" * 80)
    print("Benchmark Generation Complete")
    print("=" * 80)
    print(f"Output path:           {output_path}")
    print(f"Expected total rows:   {expected_total_rows:,}")
    print(f"Total elapsed:         {total_elapsed:.2f}s")
    print("=" * 80)


def generate_legacy_demo_data(spark: SparkSession, output_root: str):
    """Compatibility mode for existing merge demo pipeline input layout."""
    output_root = output_root.rstrip("/")
    benchmark_root = f"{output_root}/legacy_tmp"

    baseline_start = datetime(2026, 2, 26, 0, 0, 0)
    overlap_start = baseline_start + timedelta(days=1)

    profile_df = build_metric_profile(spark)

    # One-device, 7-day baseline and overlap datasets to preserve old demo behavior.
    df1 = build_device_batch_df(
        spark=spark,
        batch_start=100,
        batch_end=101,
        start_ts=baseline_start.strftime("%Y-%m-%d %H:%M:%S"),
        end_ts=(baseline_start + timedelta(days=7) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
        profile_df=profile_df,
    )
    df2 = build_device_batch_df(
        spark=spark,
        batch_start=100,
        batch_end=101,
        start_ts=overlap_start.strftime("%Y-%m-%d %H:%M:%S"),
        end_ts=(overlap_start + timedelta(days=7) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
        profile_df=profile_df,
    )

    df1.write.mode("overwrite").parquet(f"{output_root}/file1_baseline.parquet")
    df2.write.mode("overwrite").parquet(f"{output_root}/file2_overlap.parquet")

    print("\nLegacy demo files generated:")
    print(f"- {output_root}/file1_baseline.parquet")
    print(f"- {output_root}/file2_overlap.parquet")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=str, default="/raw", help="Root output directory")
    parser.add_argument("--mode", type=str, choices=["benchmark", "legacy"], default="benchmark")
    parser.add_argument("--devices", type=int, default=100000)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--start-time", type=str, default="2026-03-01 00:00:00")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session()

    try:
        start_dt = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")

        if args.mode == "benchmark":
            generate_benchmark_data(
                spark=spark,
                output_root=args.output,
                total_devices=args.devices,
                batch_size=args.batch_size,
                start_time=start_dt,
            )
        else:
            generate_legacy_demo_data(spark=spark, output_root=args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
