"""
ATLAS Delta Lake Module - Refined Layer Deduplication Pipeline
================================================================================
High-Performance 'Source of Truth' pipeline with Delta Lake MERGE deduplication.

Architecture: Lakehouse Pattern for 400,000+ Device Scale
- Triple-Hash Composite Primary Key: (device_id, metric_time, application_customer_id)
- 5-Level Deep Partitioning: metric_type/date/platform_customer_id/application_customer_id/device_id
- 7-Day Rolling Window Overlap Handling via MERGE deduplication
- Storage Optimization: Parquet columnar format with Z-ORDER clustering

Pipeline Steps:
1. Initialize SparkSession with Delta Lake + Parquet optimizations
2. Read pre-flattened Parquet files from Processing Layer
3. Prepare partition columns (extract date from metric_time)
4. Initialize/Load Delta Table with 5-level partitioning
5. Execute MERGE (upsert) with Triple-Hash composite key
6. Run OPTIMIZE + Z-ORDER for small file compaction
7. Verify deduplication and report metrics

NOTE: Input data is ALREADY FLATTENED by upstream Spark processing layer.
"""

import time
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, date_format, sha2, concat_ws, lit, when, coalesce
)
from pyspark.sql.types import StringType
from delta import DeltaTable
from delta.tables import DeltaMergeBuilder


# =============================================================================
# CONFIGURATION
# =============================================================================

class PipelineConfig:
    """Configuration for ATLAS Refined Layer Pipeline."""
    
    # Paths (now configurable via CLI args)
    RAW_DATA_PATH = "/raw"
    REFINED_PATH = "/refined"
    
    # Triple-Hash Composite Primary Key columns
    PRIMARY_KEY_COLUMNS = ["device_id", "metric_time", "application_customer_id"]
    
    # 5-Level Partition columns (order matters for directory structure)
    # Structure: /refined/report_type/date/platform_customer_id/application_customer_id/device_id/
    PARTITION_COLUMNS = [
        "report_type",
        "partition_date",
        "platform_customer_id",
        "application_customer_id",
        "device_id"
    ]
    
    # Z-ORDER clustering column for read optimization
    # Note: Cannot Z-ORDER on partition columns, using metric_time for time-series query locality
    ZORDER_COLUMN = "metric_time"
    
    # Delta Lake optimizations
    TARGET_FILE_SIZE_MB = 128  # Target file size for OPTIMIZE
    MAX_RECORDS_PER_FILE = 1_000_000


# =============================================================================
# SPARK SESSION FACTORY
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Create SparkSession with Delta Lake support optimized for ATLAS pipeline.
    
    Configurations:
    - Delta Lake 3.1.0 for Spark 3.5.x compatibility
    - Parquet as underlying storage format (Delta default)
    - Auto schema merge for evolving schemas
    - Adaptive query execution for dynamic optimization
    - Columnar read optimizations enabled
    """
    spark = (
        SparkSession.builder
        .appName("ATLAS-RefinedLayer-DeltaMerge")
        .master("local[*]")
        # Delta Lake core extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        # Schema evolution
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Parquet storage optimizations (Delta uses Parquet internally)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        # Adaptive Query Execution for partition pruning efficiency
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Delta optimizations for large-scale writes
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "false")  # Manual OPTIMIZE
        .config("spark.databricks.delta.properties.defaults.targetFileSize", 
                str(PipelineConfig.TARGET_FILE_SIZE_MB * 1024 * 1024))
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# DATA PREPARATION
# =============================================================================

def prepare_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add partition columns required for 5-level deep partitioning.
    
    Extracts partition_date from metric_time for date-based partitioning.
    Ensures all partition columns have non-null values for proper partitioning.
    
    Args:
        df: Input DataFrame with flattened telemetry data
        
    Returns:
        DataFrame with partition_date column added
    """
    return (
        df
        # Extract date from metric_time for date partitioning
        .withColumn(
            "partition_date",
            date_format(to_date(col("metric_time")), "yyyy-MM-dd")
        )
        # Handle potential nulls in partition columns with defaults
        .withColumn(
            "report_type",
            coalesce(col("report_type"), lit("unknown"))
        )
        .withColumn(
            "platform_customer_id",
            coalesce(col("platform_customer_id"), lit("unknown"))
        )
        .withColumn(
            "application_customer_id",
            coalesce(col("application_customer_id"), lit("unknown"))
        )
        .withColumn(
            "device_id",
            coalesce(col("device_id"), lit("unknown"))
        )
    )


def generate_composite_hash(df: DataFrame) -> DataFrame:
    """
    Generate SHA-256 hash of the Triple-Hash composite primary key.
    
    Creates a deterministic hash from (device_id, metric_time, application_customer_id)
    for efficient comparison during MERGE operations.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with _composite_key_hash column
    """
    return df.withColumn(
        "_composite_key_hash",
        sha2(
            concat_ws(
                "||",
                col("device_id"),
                col("metric_time"),
                col("application_customer_id")
            ),
            256
        )
    )


# =============================================================================
# DELTA TABLE OPERATIONS
# =============================================================================

def initialize_delta_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_cols: list
) -> None:
    """
    Initialize Delta table with 5-level partitioning and Parquet storage.
    
    Creates the initial Delta table structure with:
    - Explicit Parquet format (Delta's underlying storage)
    - 5-level partition scheme for partition pruning
    - Optimized write settings for columnar efficiency
    
    Args:
        spark: Active SparkSession
        df: Baseline DataFrame to initialize table
        path: Delta table path
        partition_cols: List of partition column names in hierarchy order
    """
    print(f"         Partition structure: /{'/'.join(partition_cols)}/")
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")  # Allow partition schema changes
        .partitionBy(*partition_cols)
        # Delta uses Parquet internally - these options ensure optimal columnar storage
        .option("parquet.compression", "snappy")
        .option("parquet.block.size", str(PipelineConfig.TARGET_FILE_SIZE_MB * 1024 * 1024))
        .save(path)
    )


def execute_merge_deduplication(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame
) -> dict:
    """
    Execute Delta MERGE for deduplication using Triple-Hash composite key.
    
    Implements the 7-day rolling overlap handling:
    - Records matching on (device_id, metric_time, application_customer_id) are IGNORED
    - Only truly new records are INSERTED
    - This strips the 6-day overlap from incoming hourly batches
    
    MERGE Logic:
    - WHEN MATCHED: Do nothing (record exists, ignore duplicate)
    - WHEN NOT MATCHED: Insert all columns (new unique record)
    
    Args:
        spark: Active SparkSession
        target_path: Path to target Delta table
        source_df: Incoming batch DataFrame
        
    Returns:
        Dictionary with merge operation metrics
    """
    delta_table = DeltaTable.forPath(spark, target_path)
    
    # Build merge condition using Triple-Hash composite key
    merge_condition = """
        target.device_id = source.device_id 
        AND target.metric_time = source.metric_time 
        AND target.application_customer_id = source.application_customer_id
    """
    
    # Execute MERGE with deduplication logic
    # whenMatchedUpdate is intentionally omitted - existing records are preserved
    # This implements "insert-only-if-new" deduplication pattern
    merge_builder = (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            merge_condition
        )
        # WHEN MATCHED: Intentionally no action (ignore duplicates from 7-day overlap)
        # WHEN NOT MATCHED: Insert new records only
        .whenNotMatchedInsertAll()
    )
    
    # Execute and capture metrics
    merge_builder.execute()
    
    # Get operation metrics from Delta history
    history_df = delta_table.history(1)
    history_row = history_df.collect()[0]
    
    metrics = {
        "operation": history_row["operation"],
        "operationMetrics": history_row["operationMetrics"],
        "timestamp": history_row["timestamp"]
    }
    
    return metrics


def optimize_delta_table(spark: SparkSession, path: str, zorder_col: str) -> dict:
    """
    Execute OPTIMIZE with Z-ORDER to resolve small file issues.
    
    This operation:
    1. Compacts small Parquet files into optimal ~128MB files
    2. Z-ORDERs by device_id for data locality in columnar scans
    3. Improves downstream query performance via data clustering
    
    Args:
        spark: Active SparkSession
        path: Delta table path
        zorder_col: Column to Z-ORDER by for query optimization
        
    Returns:
        Dictionary with optimization metrics
    """
    delta_table = DeltaTable.forPath(spark, path)
    
    # Execute OPTIMIZE with Z-ORDER using Delta Python API
    optimize_result = (
        delta_table
        .optimize()
        .executeZOrderBy(zorder_col)
    )
    
    # Extract metrics from result DataFrame
    if optimize_result.count() > 0:
        metrics_row = optimize_result.collect()[0]
        metrics = {
            "numFilesAdded": metrics_row["metrics"]["numFilesAdded"],
            "numFilesRemoved": metrics_row["metrics"]["numFilesRemoved"],
            "numBatches": metrics_row["metrics"]["numBatches"],
            "totalConsideredFiles": metrics_row["metrics"]["totalConsideredFiles"],
            "totalFilesSkipped": metrics_row["metrics"]["totalFilesSkipped"],
            "preserveInsertionOrder": metrics_row["metrics"]["preserveInsertionOrder"]
        }
    else:
        metrics = {"status": "no_files_to_optimize"}
    
    return metrics


def vacuum_old_files(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Vacuum old Delta log files beyond retention period.
    
    Default retention: 168 hours (7 days) to match the rolling window.
    
    Args:
        spark: Active SparkSession
        path: Delta table path
        retention_hours: Hours to retain old versions (default 7 days)
    """
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)


# =============================================================================
# VERIFICATION & REPORTING
# =============================================================================

def verify_deduplication(
    spark: SparkSession,
    path: str,
    input_counts: dict
) -> dict:
    """
    Verify deduplication results and generate metrics report.
    
    Args:
        spark: Active SparkSession
        path: Delta table path
        input_counts: Dictionary with file1_count and file2_count
        
    Returns:
        Dictionary with verification metrics
    """
    final_df = spark.read.format("delta").load(path)
    final_count = final_df.count()
    
    # Calculate deduplication efficiency
    total_input = input_counts["file1_count"] + input_counts["file2_count"]
    duplicates_dropped = total_input - final_count
    dedup_ratio = (duplicates_dropped / total_input * 100) if total_input > 0 else 0
    
    # Get partition statistics
    partition_stats = (
        final_df
        .groupBy("report_type", "partition_date")
        .count()
        .orderBy("report_type", "partition_date")
    )
    
    return {
        "final_count": final_count,
        "total_input": total_input,
        "duplicates_dropped": duplicates_dropped,
        "deduplication_ratio": dedup_ratio,
        "partition_count": partition_stats.count()
    }


def print_table_info(spark: SparkSession, path: str) -> None:
    """Print Delta table metadata and statistics."""
    delta_table = DeltaTable.forPath(spark, path)
    
    print("\n  Delta Table Details:")
    print(f"  - Location: {path}")
    
    # Get table detail
    detail_df = delta_table.detail()
    detail = detail_df.collect()[0]
    
    print(f"  - Format: {detail['format']}")
    print(f"  - Partitions: {detail['partitionColumns']}")
    print(f"  - Num Files: {detail['numFiles']}")
    print(f"  - Size (bytes): {detail['sizeInBytes']}")
    
    # Show recent history
    print("\n  Recent Operations:")
    history_df = delta_table.history(3)
    for row in history_df.collect():
        print(f"  - {row['timestamp']}: {row['operation']}")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main():
    """
    ATLAS Refined Layer - Delta Lake Deduplication Pipeline
    
    Implements:
    - Triple-Hash Composite Primary Key (device_id, metric_time, application_customer_id)
    - 5-Level Deep Partitioning for partition pruning
    - Delta MERGE for 7-day overlap deduplication
    - OPTIMIZE + Z-ORDER for storage efficiency
    """
    
    print("\n" + "=" * 80)
    print("  ATLAS - REFINED LAYER DELTA LAKE PIPELINE")
    print("  High-Performance Deduplication for 400K+ Device Scale")
    print("=" * 80)
    print("\n  Architecture:")
    print("  - Triple-Hash Key: (device_id, metric_time, application_customer_id)")
    print("  - 5-Level Partitioning: report_type/date/pcid/acid/device_id")
    print("  - Storage: Parquet columnar format with Snappy compression")
    print("  - Optimization: Z-ORDER by metric_time for query locality")
    
    # =========================================================================
    # STEP 1: INITIALIZE SPARK
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 1] Initializing Spark session with Delta Lake...")
    print("-" * 80)
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, default=PipelineConfig.RAW_DATA_PATH, help='Input raw parquet directory')
    parser.add_argument('--output', type=str, default=PipelineConfig.REFINED_PATH, help='Output refined delta directory')
    args = parser.parse_args()
    PipelineConfig.RAW_DATA_PATH = args.input
    PipelineConfig.REFINED_PATH = args.output

    pipeline_start = time.perf_counter()
    spark = create_spark_session()
    print("         ✓ SparkSession created with Delta Lake 3.1.0")
    print("         ✓ Parquet vectorized reader enabled")
    print("         ✓ Adaptive Query Execution enabled")
    
    # =========================================================================
    # STEP 2: READ BASELINE DATA
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 2] Reading baseline data (File 1 - pre-flattened)...")
    print("-" * 80)
    
    latency_start = time.perf_counter()
    df1_raw = spark.read.parquet(f"{PipelineConfig.RAW_DATA_PATH}/file1_baseline.parquet")
    df1_prepared = prepare_partition_columns(df1_raw)

    file1_count = df1_prepared.count()
    print(f"         ✓ File 1 rows: {file1_count:,}")
    print(f"         ✓ Partition columns added: {PipelineConfig.PARTITION_COLUMNS}")
    
    # =========================================================================
    # STEP 3: INITIALIZE DELTA TABLE WITH 5-LEVEL PARTITIONING
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 3] Creating Delta table with 5-level partitioning...")
    print("-" * 80)
    
    initialize_delta_table(
        spark=spark,
        df=df1_prepared,
        path=PipelineConfig.REFINED_PATH,
        partition_cols=PipelineConfig.PARTITION_COLUMNS
    )

    print(f"         ✓ Delta table created at {PipelineConfig.REFINED_PATH}")
    print(f"         ✓ Baseline records: {file1_count:,}")
    
    # =========================================================================
    # STEP 4: READ OVERLAP DATA
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 4] Reading overlap data (File 2 - 7-day rolling window)...")
    print("-" * 80)
    
    df2_raw = spark.read.parquet(f"{PipelineConfig.RAW_DATA_PATH}/file2_overlap.parquet")
    df2_prepared = prepare_partition_columns(df2_raw)

    file2_count = df2_prepared.count()
    print(f"         ✓ File 2 rows: {file2_count:,}")
    print(f"         ✓ Expected overlap with File 1 (6-day window)")
    
    # =========================================================================
    # STEP 5: DELTA MERGE DEDUPLICATION
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 5] Executing Delta MERGE deduplication...")
    print("-" * 80)
    print("         Triple-Hash Key: (device_id, metric_time, application_customer_id)")
    print("         Logic: WHEN MATCHED → Ignore | WHEN NOT MATCHED → Insert")
    
    merge_start = time.perf_counter()

    merge_metrics = execute_merge_deduplication(
        spark=spark,
        target_path=PipelineConfig.REFINED_PATH,
        source_df=df2_prepared
    )

    merge_elapsed = time.perf_counter() - merge_start
    print(f"         ✓ MERGE completed in {merge_elapsed:.2f}s")
    print(f"         ✓ Operation: {merge_metrics['operation']}")
    
    # =========================================================================
    # STEP 6: OPTIMIZE + Z-ORDER
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 6] Running OPTIMIZE with Z-ORDER BY metric_time...")
    print("-" * 80)
    print("         Compacting small files for columnar read efficiency...")
    
    optimize_start = time.perf_counter()

    optimize_metrics = optimize_delta_table(
        spark=spark,
        path=PipelineConfig.REFINED_PATH,
        zorder_col=PipelineConfig.ZORDER_COLUMN
    )

    optimize_elapsed = time.perf_counter() - optimize_start
    print(f"         ✓ OPTIMIZE completed in {optimize_elapsed:.2f}s")

    if "numFilesRemoved" in optimize_metrics:
        print(f"         ✓ Files removed: {optimize_metrics['numFilesRemoved']}")
        print(f"         ✓ Files added: {optimize_metrics['numFilesAdded']}")
    
    # =========================================================================
    # STEP 7: VERIFICATION
    # =========================================================================
    print("\n" + "-" * 80)
    print("[STEP 7] Verifying deduplication results...")
    print("-" * 80)
    
    verification = verify_deduplication(
        spark=spark,
        path=PipelineConfig.REFINED_PATH,
        input_counts={"file1_count": file1_count, "file2_count": file2_count}
    )

    # Print table metadata
    print_table_info(spark, PipelineConfig.REFINED_PATH)

    # =========================================================================
    # RESULTS SUMMARY
    # =========================================================================
    pipeline_elapsed = time.perf_counter() - pipeline_start
    latency_end = time.perf_counter()
    latency = latency_end - latency_start

    print("\n" + "=" * 80)
    print("  PIPELINE EXECUTION SUMMARY")
    print("=" * 80)
    print(f"\n  Input Statistics:")
    print(f"  - File 1 (baseline):     {file1_count:,} rows")
    print(f"  - File 2 (overlap):      {file2_count:,} rows")
    print(f"  - Total input:           {verification['total_input']:,} rows")

    print(f"\n  Deduplication Results:")
    print(f"  - Final table rows:      {verification['final_count']:,}")
    print(f"  - Duplicates dropped:    {verification['duplicates_dropped']:,}")
    print(f"  - Dedup ratio:           {verification['deduplication_ratio']:.1f}%")
    print(f"  - Partition count:       {verification['partition_count']}")

    print(f"\n  Performance Metrics:")
    print(f"  - MERGE time:            {merge_elapsed:.2f}s")
    print(f"  - OPTIMIZE time:         {optimize_elapsed:.2f}s")
    print(f"  - Total pipeline time:   {pipeline_elapsed:.2f}s")
    print(f"  - Delta Lake latency (read/write): {latency:.2f}s")

    print(f"\n  Architecture Verification:")
    print(f"  ✓ Triple-Hash Key: device_id + metric_time + application_customer_id")
    print(f"  ✓ 5-Level Partitioning: {'/'.join(PipelineConfig.PARTITION_COLUMNS)}")
    print(f"  ✓ Storage Format: Parquet (Delta Lake native)")
    print(f"  ✓ Z-ORDER Clustering: {PipelineConfig.ZORDER_COLUMN}")

    print("\n" + "=" * 80)
    print("  ATLAS REFINED LAYER PIPELINE COMPLETE")
    print("=" * 80 + "\n")

    # Cleanup
    spark.stop()

    return verification


if __name__ == "__main__":
    main()
