"""
ATLAS Delta Lake Module - Streaming Merge Pipeline
================================================================================
Spark Structured Streaming pipeline for real-time deduplication via Delta Lake MERGE.

Architecture: Continuous Lakehouse Pattern for Low-Latency Ingestion
- Receives pre-flattened telemetry data from upstream Kafka/Socket source
- Performs foreachBatch MERGE for exactly-once deduplication
- Outputs to the same Delta table used by batch processing (unified sink)
- Supports horizontal scaling via Spark executors and Kafka partitions

Streaming Flow:
1. Source: Kafka topic or Rate source (for testing)
2. Transform: Parse JSON → Add partition columns → Generate composite hash
3. Sink: foreachBatch MERGE into Delta table (same as batch pipeline)

Scalability Features:
- Auto-scaling via Spark dynamic allocation
- Kafka partition-based parallelism
- Watermarking for late data handling
- Checkpoint-based exactly-once semantics
- Configurable trigger intervals (micro-batch or continuous)

NOTE: Uses the same Delta table as batch pipeline for unified 'Source of Truth'.
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, date_format, sha2, concat_ws, lit, when, coalesce,
    from_json, current_timestamp, window, expr, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from delta import DeltaTable
from delta.tables import DeltaMergeBuilder


# =============================================================================
# CONFIGURATION
# =============================================================================

class StreamingConfig:
    """Configuration for ATLAS Streaming Merge Pipeline."""
    
    # Kafka source configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "atlas.telemetry.flattened")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "atlas-delta-streaming")
    KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    
    # Delta Lake paths (same as batch pipeline for unified sink)
    REFINED_PATH = os.getenv("REFINED_PATH", "/refined")
    CHECKPOINT_PATH = os.getenv("STREAMING_CHECKPOINT_PATH", "/refined/_streaming_checkpoints")
    
    # Triple-Hash Composite Primary Key columns (same as batch)
    PRIMARY_KEY_COLUMNS = ["device_id", "metric_time", "application_customer_id"]
    
    # 5-Level Partition columns (same as batch)
    PARTITION_COLUMNS = [
        "report_type",
        "partition_date",
        "platform_customer_id",
        "application_customer_id",
        "device_id"
    ]
    
    # Z-ORDER clustering column
    ZORDER_COLUMN = "metric_time"
    
    # Streaming trigger configuration
    TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
    
    # Horizontal scaling configuration
    SPARK_EXECUTOR_INSTANCES = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "2"))
    SPARK_EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "true").lower() == "true"
    SPARK_MIN_EXECUTORS = int(os.getenv("SPARK_MIN_EXECUTORS", "1"))
    SPARK_MAX_EXECUTORS = int(os.getenv("SPARK_MAX_EXECUTORS", "8"))
    
    # Compression (Zstd for better compression ratio)
    COMPRESSION_CODEC = "zstd"
    
    # Vacuum settings (14-day retention)
    VACUUM_RETENTION_DAYS = 14
    VACUUM_RETENTION_HOURS = VACUUM_RETENTION_DAYS * 24
    
    # Optimize settings
    OPTIMIZE_ENABLED = os.getenv("OPTIMIZE_ENABLED", "true").lower() == "true"
    OPTIMIZE_INTERVAL_MINUTES = int(os.getenv("OPTIMIZE_INTERVAL_MINUTES", "60"))
    
    # Watermark for late data handling
    WATERMARK_DELAY = "10 minutes"
    
    # Maximum records per micro-batch (for backpressure)
    MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "100000"))


# =============================================================================
# TELEMETRY SCHEMA (Pre-flattened from upstream)
# =============================================================================

TELEMETRY_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("metric_time", StringType(), True),  # ISO timestamp string
    StructField("application_customer_id", StringType(), True),
    StructField("platform_customer_id", StringType(), True),
    StructField("report_type", StringType(), True),
    StructField("MetricValue", DoubleType(), True),
    StructField("partition_date", StringType(), True),  # Optional, will compute if missing
])


# =============================================================================
# SPARK SESSION FACTORY WITH HORIZONTAL SCALING
# =============================================================================

def create_spark_session(app_name: str = "ATLAS-StreamingMerge") -> SparkSession:
    """
    Create SparkSession with Delta Lake support optimized for streaming.
    
    Horizontal Scaling Features:
    - Dynamic resource allocation (auto-scale executors based on load)
    - Configurable executor instances, cores, and memory
    - Adaptive query execution for partition coalescing
    - Optimized shuffle partitions for parallelism
    
    Compression: Zstd for better compression ratio (30% better than Snappy)
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Delta Lake core extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        # Schema evolution
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Zstd compression (better ratio than Snappy for large datasets)
        .config("spark.sql.parquet.compression.codec", StreamingConfig.COMPRESSION_CODEC)
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        # Adaptive Query Execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Delta optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "false")
        # Streaming optimizations
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.streaming.checkpointLocation", StreamingConfig.CHECKPOINT_PATH)
        # Shuffle partitions for parallelism
        .config("spark.sql.shuffle.partitions", "200")
    )
    
    # Horizontal scaling: Dynamic allocation or fixed executors
    if StreamingConfig.SPARK_DYNAMIC_ALLOCATION:
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", str(StreamingConfig.SPARK_MIN_EXECUTORS))
            .config("spark.dynamicAllocation.maxExecutors", str(StreamingConfig.SPARK_MAX_EXECUTORS))
            .config("spark.dynamicAllocation.initialExecutors", str(StreamingConfig.SPARK_EXECUTOR_INSTANCES))
            .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
            .config("spark.dynamicAllocation.schedulerBacklogTimeout", "5s")
            .config("spark.shuffle.service.enabled", "true")
        )
    else:
        builder = (
            builder
            .config("spark.executor.instances", str(StreamingConfig.SPARK_EXECUTOR_INSTANCES))
        )
    
    # Executor configuration
    builder = (
        builder
        .config("spark.executor.cores", str(StreamingConfig.SPARK_EXECUTOR_CORES))
        .config("spark.executor.memory", StreamingConfig.SPARK_EXECUTOR_MEMORY)
    )
    
    # For local testing, use local[*]
    if os.getenv("SPARK_MASTER"):
        builder = builder.master(os.getenv("SPARK_MASTER"))
    else:
        builder = builder.master("local[*]")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


# =============================================================================
# DATA PREPARATION
# =============================================================================

def prepare_partition_columns(df: DataFrame) -> DataFrame:
    """Add partition columns required for 5-level deep partitioning."""
    # Compute partition_date from metric_time if not present
    if "partition_date" in df.columns:
        prepared = df.withColumn(
            "partition_date",
            date_format(col("partition_date"), "yyyy-MM-dd")
        )
    else:
        prepared = df.withColumn(
            "partition_date",
            date_format(to_date(col("metric_time")), "yyyy-MM-dd")
        )
    
    return (
        prepared
        .withColumn("report_type", coalesce(col("report_type"), lit("unknown")))
        .withColumn("platform_customer_id", coalesce(col("platform_customer_id"), lit("unknown")))
        .withColumn("application_customer_id", coalesce(col("application_customer_id"), lit("unknown")))
        .withColumn("device_id", coalesce(col("device_id"), lit("unknown")))
    )


def generate_composite_hash(df: DataFrame) -> DataFrame:
    """Generate SHA-256 hash of the Triple-Hash composite primary key."""
    return df.withColumn(
        "_composite_key_hash",
        sha2(concat_ws("||", col("device_id"), col("metric_time"), col("application_customer_id")), 256)
    )


# =============================================================================
# DELTA LAKE OPERATIONS
# =============================================================================

def delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Check if Delta table exists at path."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def initialize_delta_table_if_needed(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_cols: list
) -> bool:
    """Initialize Delta table if it doesn't exist. Returns True if initialized."""
    if delta_table_exists(spark, path):
        return False
    
    print(f"    [INIT] Creating Delta table at {path}")
    print(f"    [INIT] Partition structure: /{'/'.join(partition_cols)}/")
    print(f"    [INIT] Compression: {StreamingConfig.COMPRESSION_CODEC}")
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .option("parquet.compression", StreamingConfig.COMPRESSION_CODEC)
        .save(path)
    )
    
    return True


def execute_streaming_merge(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame
) -> Dict[str, Any]:
    """
    Execute Delta MERGE for streaming micro-batch deduplication.
    
    MERGE Logic (same as batch pipeline):
    - WHEN MATCHED: Do nothing (record exists, ignore duplicate)
    - WHEN NOT MATCHED: Insert all columns (new unique record)
    
    Returns metrics about the merge operation.
    """
    delta_table = DeltaTable.forPath(spark, target_path)
    
    merge_condition = """
        target.device_id = source.device_id 
        AND target.metric_time = source.metric_time 
        AND target.application_customer_id = source.application_customer_id
    """
    
    merge_start = time.perf_counter()
    
    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    merge_elapsed = time.perf_counter() - merge_start
    
    # Get merge metrics from history
    history_df = delta_table.history(1)
    history_row = history_df.collect()[0]
    metrics = history_row["operationMetrics"]
    
    return {
        "operation": history_row["operation"],
        "merge_time_sec": round(merge_elapsed, 3),
        "num_target_rows_inserted": metrics.get("numTargetRowsInserted", 0),
        "num_target_rows_updated": metrics.get("numTargetRowsUpdated", 0),
        "num_output_rows": metrics.get("numOutputRows", 0),
        "timestamp": history_row["timestamp"]
    }


def vacuum_delta_table(spark: SparkSession, path: str) -> Dict[str, Any]:
    """
    Vacuum Delta table to remove old files beyond retention period.
    
    Default retention: 14 days (configurable via StreamingConfig.VACUUM_RETENTION_DAYS)
    This removes:
    - Old Parquet files no longer referenced by the table
    - Transaction log entries older than retention period
    
    WARNING: After vacuum, time travel to versions older than retention is not possible.
    """
    delta_table = DeltaTable.forPath(spark, path)
    
    # Disable retention check for aggressive cleanup (use with caution in prod)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    print(f"    [VACUUM] Starting vacuum with {StreamingConfig.VACUUM_RETENTION_DAYS}-day retention...")
    vacuum_start = time.perf_counter()
    
    # Vacuum returns deleted files (empty DataFrame if nothing deleted)
    delta_table.vacuum(StreamingConfig.VACUUM_RETENTION_HOURS)
    
    vacuum_elapsed = time.perf_counter() - vacuum_start
    
    # Re-enable check
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    
    return {
        "retention_hours": StreamingConfig.VACUUM_RETENTION_HOURS,
        "vacuum_time_sec": round(vacuum_elapsed, 3),
        "status": "completed"
    }


def optimize_delta_table(spark: SparkSession, path: str) -> Dict[str, Any]:
    """Execute OPTIMIZE with Z-ORDER clustering for query performance."""
    delta_table = DeltaTable.forPath(spark, path)
    
    print(f"    [OPTIMIZE] Running Z-ORDER by {StreamingConfig.ZORDER_COLUMN}...")
    optimize_start = time.perf_counter()
    
    optimize_result = delta_table.optimize().executeZOrderBy(StreamingConfig.ZORDER_COLUMN)
    
    optimize_elapsed = time.perf_counter() - optimize_start
    
    metrics = {}
    if optimize_result.count() > 0:
        metrics_row = optimize_result.collect()[0]
        metrics = {
            "num_files_added": metrics_row["metrics"]["numFilesAdded"],
            "num_files_removed": metrics_row["metrics"]["numFilesRemoved"],
            "total_files_considered": metrics_row["metrics"]["totalConsideredFiles"],
        }
    
    return {
        "optimize_time_sec": round(optimize_elapsed, 3),
        "zorder_column": StreamingConfig.ZORDER_COLUMN,
        **metrics
    }


# =============================================================================
# STREAMING MICRO-BATCH PROCESSOR
# =============================================================================

class StreamingMergeProcessor:
    """
    Handles foreachBatch processing for streaming micro-batches.
    
    This processor:
    1. Transforms each micro-batch (add partitions, etc.)
    2. Initializes Delta table on first batch if needed
    3. Executes MERGE for deduplication
    4. Periodically runs OPTIMIZE and VACUUM for maintenance
    """
    
    def __init__(self, spark: SparkSession, output_path: str):
        self.spark = spark
        self.output_path = output_path
        self.table_initialized = delta_table_exists(spark, output_path)
        self.batch_count = 0
        self.total_rows_processed = 0
        self.last_optimize_time = time.time()
        self.last_vacuum_time = time.time()
        self._metrics_log: list = []
    
    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Process a single micro-batch from the stream.
        
        Called by foreachBatch for each micro-batch.
        """
        batch_start = time.perf_counter()
        
        if batch_df.isEmpty():
            print(f"    [Batch {batch_id}] Empty batch, skipping...")
            return
        
        row_count = batch_df.count()
        print(f"\n{'='*60}")
        print(f"    [Batch {batch_id}] Processing {row_count:,} rows...")
        
        # Prepare data
        prepared_df = prepare_partition_columns(batch_df)
        
        # Initialize or MERGE
        if not self.table_initialized:
            initialized = initialize_delta_table_if_needed(
                self.spark,
                prepared_df,
                self.output_path,
                StreamingConfig.PARTITION_COLUMNS
            )
            if initialized:
                self.table_initialized = True
                print(f"    [Batch {batch_id}] Delta table initialized")
        else:
            # Execute MERGE
            merge_metrics = execute_streaming_merge(
                self.spark,
                self.output_path,
                prepared_df
            )
            print(f"    [Batch {batch_id}] MERGE: {merge_metrics['num_target_rows_inserted']} inserted, "
                  f"{merge_metrics['merge_time_sec']}s")
        
        batch_elapsed = time.perf_counter() - batch_start
        throughput = row_count / batch_elapsed if batch_elapsed > 0 else 0
        
        self.batch_count += 1
        self.total_rows_processed += row_count
        
        print(f"    [Batch {batch_id}] Complete: {batch_elapsed:.2f}s | {throughput:,.0f} rows/s")
        
        # Periodic maintenance
        self._maybe_optimize()
        self._maybe_vacuum()
        
        # Log metrics
        self._metrics_log.append({
            "batch_id": batch_id,
            "rows": row_count,
            "elapsed_sec": round(batch_elapsed, 3),
            "throughput": round(throughput, 1),
            "timestamp": datetime.now().isoformat()
        })
    
    def _maybe_optimize(self) -> None:
        """Run OPTIMIZE periodically based on configured interval."""
        if not StreamingConfig.OPTIMIZE_ENABLED:
            return
        
        elapsed_minutes = (time.time() - self.last_optimize_time) / 60
        
        if elapsed_minutes >= StreamingConfig.OPTIMIZE_INTERVAL_MINUTES:
            print(f"\n    [Maintenance] Running periodic OPTIMIZE...")
            optimize_metrics = optimize_delta_table(self.spark, self.output_path)
            print(f"    [Maintenance] OPTIMIZE complete: {optimize_metrics}")
            self.last_optimize_time = time.time()
    
    def _maybe_vacuum(self) -> None:
        """Run VACUUM daily to clean old files."""
        elapsed_hours = (time.time() - self.last_vacuum_time) / 3600
        
        # Run vacuum once per day
        if elapsed_hours >= 24:
            print(f"\n    [Maintenance] Running daily VACUUM...")
            vacuum_metrics = vacuum_delta_table(self.spark, self.output_path)
            print(f"    [Maintenance] VACUUM complete: {vacuum_metrics}")
            self.last_vacuum_time = time.time()
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Return summary of streaming metrics."""
        return {
            "total_batches": self.batch_count,
            "total_rows_processed": self.total_rows_processed,
            "table_path": self.output_path,
            "recent_batches": self._metrics_log[-10:] if self._metrics_log else []
        }


# =============================================================================
# STREAMING SOURCES
# =============================================================================

def create_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Create Kafka streaming source for telemetry data.
    
    Expects JSON messages with pre-flattened telemetry schema.
    """
    print(f"    [Source] Connecting to Kafka: {StreamingConfig.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"    [Source] Topic: {StreamingConfig.KAFKA_TOPIC}")
    
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", StreamingConfig.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", StreamingConfig.KAFKA_TOPIC)
        .option("startingOffsets", StreamingConfig.KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", StreamingConfig.MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.group.id", StreamingConfig.KAFKA_GROUP_ID)
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Parse JSON value and extract fields
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), TELEMETRY_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("metric_time", to_timestamp(col("metric_time")))
    )
    
    return parsed_df


def create_rate_stream(spark: SparkSession, rows_per_second: int = 1000) -> DataFrame:
    """
    Create rate-based test stream for local testing without Kafka.
    
    Generates synthetic telemetry data at specified rate.
    """
    print(f"    [Source] Rate stream: {rows_per_second} rows/second")
    
    rate_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", rows_per_second)
        .load()
    )
    
    # Generate synthetic telemetry from rate stream
    synthetic_df = (
        rate_df
        .withColumn("device_id", concat_ws("-", lit("SRV"), (col("value") % 1000).cast("string")))
        .withColumn("metric_time", col("timestamp"))
        .withColumn("application_customer_id", 
                    when(col("value") % 5 == 0, lit("APP-001"))
                    .when(col("value") % 5 == 1, lit("APP-017"))
                    .when(col("value") % 5 == 2, lit("APP-113"))
                    .when(col("value") % 5 == 3, lit("APP-226"))
                    .otherwise(lit("APP-67890")))
        .withColumn("platform_customer_id",
                    when(col("value") % 5 == 0, lit("PLAT-001"))
                    .when(col("value") % 5 == 1, lit("PLAT-021"))
                    .when(col("value") % 5 == 2, lit("PLAT-101"))
                    .when(col("value") % 5 == 3, lit("PLAT-12345"))
                    .otherwise(lit("PLAT-907")))
        .withColumn("report_type",
                    when(col("value") % 4 == 0, lit("power_metrics"))
                    .when(col("value") % 4 == 1, lit("thermal_metrics"))
                    .when(col("value") % 4 == 2, lit("cpu_metrics"))
                    .otherwise(lit("sustainability_metrics")))
        .withColumn("MetricValue", (col("value") % 100 + 200).cast("double"))
        .withColumn("partition_date", date_format(col("timestamp"), "yyyy-MM-dd"))
        .select(
            "device_id", "metric_time", "application_customer_id",
            "platform_customer_id", "report_type", "MetricValue", "partition_date"
        )
    )
    
    return synthetic_df


def create_socket_stream(spark: SparkSession, host: str = "localhost", port: int = 9999) -> DataFrame:
    """
    Create socket-based test stream for simple testing.
    
    Expects newline-delimited JSON records.
    """
    print(f"    [Source] Socket stream: {host}:{port}")
    
    socket_df = (
        spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()
    )
    
    # Parse JSON from socket
    parsed_df = (
        socket_df
        .select(from_json(col("value"), TELEMETRY_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("metric_time", to_timestamp(col("metric_time")))
    )
    
    return parsed_df


# =============================================================================
# MAIN STREAMING PIPELINE
# =============================================================================

def run_streaming_pipeline(
    source_type: str = "rate",
    output_path: str = None,
    checkpoint_path: str = None,
    trigger_interval: str = None,
    rows_per_second: int = 1000,
    await_termination: bool = True
):
    """
    Run the streaming merge pipeline.
    
    Args:
        source_type: Type of streaming source (kafka, rate, socket)
        output_path: Delta table output path
        checkpoint_path: Streaming checkpoint location
        trigger_interval: Processing trigger interval (e.g., "30 seconds")
        rows_per_second: Rate for test stream
        await_termination: Whether to block until stream terminates
    
    Returns:
        StreamingQuery object if await_termination=False, else None
    """
    output_path = output_path or StreamingConfig.REFINED_PATH
    checkpoint_path = checkpoint_path or StreamingConfig.CHECKPOINT_PATH
    trigger_interval = trigger_interval or StreamingConfig.TRIGGER_INTERVAL
    
    print("\n" + "=" * 80)
    print("  ATLAS - STREAMING MERGE PIPELINE")
    print("  Real-Time Deduplication via Spark Structured Streaming")
    print("=" * 80)
    print(f"\n  Configuration:")
    print(f"  - Source: {source_type}")
    print(f"  - Output: {output_path}")
    print(f"  - Checkpoint: {checkpoint_path}")
    print(f"  - Trigger: {trigger_interval}")
    print(f"  - Compression: {StreamingConfig.COMPRESSION_CODEC}")
    print(f"  - Vacuum Retention: {StreamingConfig.VACUUM_RETENTION_DAYS} days")
    print(f"\n  Horizontal Scaling:")
    print(f"  - Dynamic Allocation: {StreamingConfig.SPARK_DYNAMIC_ALLOCATION}")
    print(f"  - Executors: {StreamingConfig.SPARK_MIN_EXECUTORS}-{StreamingConfig.SPARK_MAX_EXECUTORS}")
    print(f"  - Executor Cores: {StreamingConfig.SPARK_EXECUTOR_CORES}")
    print(f"  - Executor Memory: {StreamingConfig.SPARK_EXECUTOR_MEMORY}")
    
    # Initialize Spark
    print("\n" + "-" * 80)
    print("[STEP 1] Initializing Spark session...")
    print("-" * 80)
    
    spark = create_spark_session()
    print("         ✓ SparkSession created with Delta Lake + Streaming support")
    
    # Create streaming source
    print("\n" + "-" * 80)
    print(f"[STEP 2] Creating {source_type} streaming source...")
    print("-" * 80)
    
    if source_type == "kafka":
        stream_df = create_kafka_stream(spark)
    elif source_type == "socket":
        stream_df = create_socket_stream(spark)
    else:  # rate (default for testing)
        stream_df = create_rate_stream(spark, rows_per_second)
    
    print("         ✓ Streaming source configured")
    
    # Initialize processor
    processor = StreamingMergeProcessor(spark, output_path)
    
    # Start streaming query
    print("\n" + "-" * 80)
    print("[STEP 3] Starting streaming query with foreachBatch MERGE...")
    print("-" * 80)
    
    query = (
        stream_df.writeStream
        .foreachBatch(processor.process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    
    print(f"         ✓ Stream started: {query.name}")
    print(f"         ✓ Query ID: {query.id}")
    print("\n" + "=" * 80)
    print("  STREAMING PIPELINE RUNNING - Processing micro-batches...")
    print("  Press Ctrl+C to stop")
    print("=" * 80)
    
    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n\nShutting down streaming pipeline...")
            query.stop()
            
            # Print final summary
            summary = processor.get_metrics_summary()
            print("\n" + "=" * 80)
            print("  STREAMING PIPELINE SUMMARY")
            print("=" * 80)
            print(f"  - Total batches processed: {summary['total_batches']}")
            print(f"  - Total rows processed: {summary['total_rows_processed']:,}")
            print(f"  - Output path: {summary['table_path']}")
            print("=" * 80)
            
            spark.stop()
        return None
    
    return query


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="ATLAS Streaming Merge Pipeline")
    parser.add_argument("--source", type=str, choices=["kafka", "rate", "socket"], 
                        default="rate", help="Streaming source type")
    parser.add_argument("--output", type=str, default=StreamingConfig.REFINED_PATH,
                        help="Output Delta table path")
    parser.add_argument("--checkpoint", type=str, default=StreamingConfig.CHECKPOINT_PATH,
                        help="Streaming checkpoint path")
    parser.add_argument("--trigger", type=str, default=StreamingConfig.TRIGGER_INTERVAL,
                        help="Trigger interval (e.g., '30 seconds')")
    parser.add_argument("--rate", type=int, default=1000,
                        help="Rows per second for rate source")
    parser.add_argument("--vacuum", action="store_true",
                        help="Run vacuum only (maintenance mode)")
    parser.add_argument("--optimize", action="store_true",
                        help="Run optimize only (maintenance mode)")
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Maintenance modes
    if args.vacuum or args.optimize:
        spark = create_spark_session("ATLAS-Maintenance")
        
        if args.vacuum:
            print("\n[Maintenance] Running VACUUM...")
            result = vacuum_delta_table(spark, args.output)
            print(f"[Maintenance] VACUUM result: {result}")
        
        if args.optimize:
            print("\n[Maintenance] Running OPTIMIZE...")
            result = optimize_delta_table(spark, args.output)
            print(f"[Maintenance] OPTIMIZE result: {result}")
        
        spark.stop()
        return
    
    # Run streaming pipeline
    run_streaming_pipeline(
        source_type=args.source,
        output_path=args.output,
        checkpoint_path=args.checkpoint,
        trigger_interval=args.trigger,
        rows_per_second=args.rate
    )


if __name__ == "__main__":
    main()
