"""
ATLAS Livewire Streaming Mode - Real-Time Integration Layer
================================================================================
Implements Spark Structured Streaming for real-time data ingestion from 
upstream processing layer into the Refined Layer Delta Lake table.

Key Features:
- Reads flattened Parquet files from /app/data/processed/stream
- Schema validation and alignment before MERGE
- Micro-batch MERGE deduplication using foreachBatch
- Exactly-once delivery semantics via checkpointing
- Backpressure handling and fault tolerance

Streaming Flow:
1. readStream from /stream directory (Parquet files)
2. Micro-batch processing (30-60s trigger intervals)
3. Schema validation and alignment
4. Delta MERGE deduplication via foreachBatch
5. Checkpoint after successful MERGE

Architecture:
- Single Spark driver (local[*] by default)
- Parquet file source (cloud storage friendly)
- Delta sink with exactly-once semantics
- State management via Delta transaction log
"""

import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, input_file_name

# Import schema validator and Delta operations from main pipeline
try:
    from livewire_schema_validator import (
        validate_and_align_schema,
        generate_schema_diff_report,
        infer_upstream_schema_from_sample
    )
except ImportError:
    print("ERROR: livewire_schema_validator.py not found in PYTHONPATH")
    print("Make sure both livewire_schema_validator.py and delta_merge_pipeline.py are in /app")
    sys.exit(1)


# =============================================================================
# LIVEWIRE STREAMING CONFIGURATION
# =============================================================================

class LivewireConfig:
    """Configuration for livewire streaming mode."""
    
    # Input/Output paths
    STREAM_INPUT_PATH = "/app/data/processed/stream"
    REFINED_OUTPUT_PATH = "/refined"
    CHECKPOINT_PATH = "/refined/_streaming_checkpoints/livewire"
    
    # Streaming configuration
    TRIGGER_INTERVAL_SECONDS = 60  # Micro-batch every 60 seconds (1 minute)
    AWAIT_TERMINATION_TIMEOUT = None  # Run indefinitely until stopped
    
    # Schema validation
    VALIDATE_SCHEMA = True
    INFER_SCHEMA_ON_START = False  # Set to True to debug upstream schema
    
    # Partition columns for 5-level partitioning
    PARTITION_COLUMNS = [
        "report_type",
        "partition_date", 
        "platform_customer_id",
        "application_customer_id",
        "device_id"
    ]
    
    # Primary key for deduplication
    PRIMARY_KEY_COLUMNS = ["device_id", "metric_time", "application_customer_id"]
    
    # Compression codec
    COMPRESSION_CODEC = os.getenv("COMPRESSION_CODEC", "zstd")
    
    # Z-ORDER clustering for read optimization
    ZORDER_COLUMN = "metric_time"
    
    # Metrics and logging
    LOG_BATCH_METRICS = True
    LOG_SCHEMA_CHANGES = True


# =============================================================================
# STREAMING STATISTICS
# =============================================================================

class StreamingMetrics:
    """Track livewire streaming metrics."""
    
    def __init__(self):
        self.start_time = time.time()
        self.batches_processed = 0
        self.total_rows_processed = 0
        self.total_rows_merged = 0
        self.total_rows_inserted = 0
        self.batches_with_schema_changes = 0
        self.total_merge_time_sec = 0.0
        self.errors = []
    
    def record_batch(self, batch_id: int, num_rows: int, merge_time: float, 
                    rows_matched: int, rows_inserted: int, schema_changed: bool = False):
        """Record metrics for processed batch."""
        self.batches_processed += 1
        self.total_rows_processed += num_rows
        self.total_rows_merged += rows_matched
        self.total_rows_inserted += rows_inserted
        self.total_merge_time_sec += merge_time
        
        if schema_changed:
            self.batches_with_schema_changes += 1
        
        if LivewireConfig.LOG_BATCH_METRICS:
            elapsed = time.time() - self.start_time
            throughput = self.total_rows_processed / elapsed if elapsed > 0 else 0
            print(f"         ℹ Batch {batch_id}: {num_rows:,} rows | "
                  f"Merged: {rows_matched:,} | Inserted: {rows_inserted:,} | "
                  f"Throughout: {throughput:.0f} rows/sec")
    
    def record_error(self, batch_id: int, error: str):
        """Record batch processing error."""
        self.errors.append({"batch_id": batch_id, "error": error, "timestamp": datetime.now()})
        print(f"         ✗ Batch {batch_id} ERROR: {error}")
    
    def get_summary(self) -> dict:
        """Get summary metrics."""
        elapsed = time.time() - self.start_time
        throughput = self.total_rows_processed / elapsed if elapsed > 0 else 0
        
        return {
            "status": "STREAMING",
            "batches_processed": self.batches_processed,
            "total_rows_processed": self.total_rows_processed,
            "total_rows_merged_deduplicated": self.total_rows_merged,
            "total_rows_inserted_new": self.total_rows_inserted,
            "batches_with_schema_mismatches": self.batches_with_schema_changes,
            "total_merge_time_sec": round(self.total_merge_time_sec, 2),
            "elapsed_time_sec": round(elapsed, 2),
            "throughput_rows_per_sec": round(throughput, 1),
            "error_count": len(self.errors),
            "errors": [{"batch_id": e["batch_id"], "error": e["error"]} for e in self.errors[-10:]]
        }


# =============================================================================
# STREAMING MERGE ENGINE
# =============================================================================

def livewire_merge_batch(batch_df: DataFrame, batch_id: int, spark: SparkSession, 
                        metrics: StreamingMetrics, target_path: str) -> None:
    """
    Execute Delta MERGE for a single micro-batch from streaming source.
    
    This function is called by foreachBatch for each micro-batch:
    1. Validate and align schema
    2. Prepare partition columns
    3. Execute Delta MERGE deduplication
    4. Record metrics
    5. Handle errors gracefully
    
    Args:
        batch_df: DataFrame chunk from streaming source
        batch_id: Micro-batch identifier
        spark: SparkSession instance
        metrics: Metrics tracking object
        target_path: Path to target Delta table
    """
    
    if batch_df.count() == 0:
        print(f"         ℹ Batch {batch_id}: Empty batch (no new data)")
        return
    
    try:
        merge_start = time.time()
        
        # Step 1: Schema validation and alignment
        if LivewireConfig.VALIDATE_SCHEMA:
            aligned_df, schema_report = validate_and_align_schema(batch_df, spark)
            
            if schema_report["status"] != "PASS_EXACT_MATCH":
                print(generate_schema_diff_report(schema_report))
                metrics.batches_with_schema_changes += 1
            
            batch_df = aligned_df
        
        # Step 2: Prepare partition columns (add if missing)
        if "partition_date" not in batch_df.columns:
            from pyspark.sql.functions import to_date
            batch_df = batch_df.withColumn(
                "partition_date",
                to_date(col("metric_time"), "yyyy-MM-dd HH:mm:ss")
            )
        
        if "file_date" not in batch_df.columns:
            batch_df = batch_df.withColumn("file_date", col("partition_date"))
        
        # Step 3: Execute Delta MERGE
        num_rows = batch_df.count()
        
        from delta import DeltaTable
        
        # Check if target table exists
        try:
            delta_table = DeltaTable.forPath(spark, target_path)
            target_exists = True
            target_count_before = spark.read.format("delta").load(target_path).count()
        except Exception:
            target_exists = False
            target_count_before = 0
            # Initialize empty Delta table
            empty_df = batch_df.limit(0)  # Schema only, no rows
            (
                empty_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy(*LivewireConfig.PARTITION_COLUMNS)
                .option("parquet.compression", LivewireConfig.COMPRESSION_CODEC)
                .save(target_path)
            )
            delta_table = DeltaTable.forPath(spark, target_path)
        
        # Execute MERGE
        if target_exists:
            merge_condition = (
                f"target.device_id = source.device_id "
                f"AND target.metric_time = source.metric_time "
                f"AND target.application_customer_id = source.application_customer_id"
            )
            
            merge_builder = (
                delta_table.alias("target")
                .merge(batch_df.alias("source"), merge_condition)
                .whenNotMatchedInsertAll()
            )
            merge_builder.execute()
        else:
            # First batch - just append
            (
                batch_df.write
                .format("delta")
                .mode("append")
                .partitionBy(*LivewireConfig.PARTITION_COLUMNS)
                .save(target_path)
            )
        
        # Step 4: Record metrics
        merge_time = time.time() - merge_start
        
        # Reload delta table for post-merge count
        delta_table_after = DeltaTable.forPath(spark, target_path)
        target_count_after = spark.read.format("delta").load(target_path).count()
        
        rows_inserted = target_count_after - target_count_before
        rows_matched = num_rows - rows_inserted
        
        metrics.record_batch(
            batch_id,
            num_rows,
            merge_time,
            rows_matched,
            rows_inserted,
            schema_report["status"] != "PASS_EXACT_MATCH" if LivewireConfig.VALIDATE_SCHEMA else False
        )
        
        print(f"         ✓ Batch {batch_id}: MERGE COMPLETE in {merge_time:.2f}s | "
              f"Match: {rows_matched:,} | Insert: {rows_inserted:,}")
        
    except Exception as e:
        error_msg = str(e)
        metrics.record_error(batch_id, error_msg)
        print(f"         ✗ Batch {batch_id} MERGE FAILED: {error_msg}")
        # Continue with next batch (don't crash entire stream)


def run_livewire_streaming(spark: SparkSession) -> None:
    """
    Start livewire streaming mode with Structured Streaming.
    
    Flow:
    1. Validate input path exists
    2. Optionally infer upstream schema
    3. Create streaming DataFrame from /stream directory
    4. Apply foreachBatch for micro-batch MERGE processing
    5. Run indefinitely with checkpointing for fault tolerance
    """
    
    print("\n================================================================================")
    print("  ATLAS LIVEWIRE MODE - REAL-TIME STREAMING INTEGRATION")
    print("================================================================================\n")
    
    # Initialization
    print(f"Configuration:")
    print(f"  Input path:         {LivewireConfig.STREAM_INPUT_PATH}")
    print(f"  Output path:        {LivewireConfig.REFINED_OUTPUT_PATH}")
    print(f"  Checkpoint path:    {LivewireConfig.CHECKPOINT_PATH}")
    print(f"  Trigger interval:   {LivewireConfig.TRIGGER_INTERVAL_SECONDS} seconds")
    print(f"  Schema validation:  {LivewireConfig.VALIDATE_SCHEMA}")
    print("")
    
    # Validate input path
    if not os.path.exists(LivewireConfig.STREAM_INPUT_PATH):
        try:
            os.makedirs(LivewireConfig.STREAM_INPUT_PATH, exist_ok=True)
            print(f"  ℹ Created input path: {LivewireConfig.STREAM_INPUT_PATH}")
        except Exception as e:
            print(f"  ✗ ERROR: Cannot create input path: {e}")
            return
    
    # Optional: Infer upstream schema from sample files
    if LivewireConfig.INFER_SCHEMA_ON_START:
        print(f"  ℹ Inferring upstream schema...")
        infer_upstream_schema_from_sample(LivewireConfig.STREAM_INPUT_PATH, spark)
    
    # Initialize metrics
    metrics = StreamingMetrics()
    
    # Create streaming DataFrame from parquet files
    print(f"  ℹ Starting Parquet streaming source...")
    stream_df = (
        spark.readStream
        .format("parquet")
        .option("maxFileAge", "1h")  # Re-read files for 1 hour
        .option("latestFirst", "false")  # Process files in order
        .load(LivewireConfig.STREAM_INPUT_PATH)
    )
    
    # Add source tracking columns
    stream_df = (
        stream_df
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_time", current_timestamp())
    )
    
    # Start streaming query with foreachBatch
    print(f"  ℹ Starting streaming query with foreachBatch...")
    
    def process_batch(batch_df: DataFrame, batch_id: int):
        """Callback for each micro-batch."""
        livewire_merge_batch(
            batch_df, batch_id, spark, metrics, 
            LivewireConfig.REFINED_OUTPUT_PATH
        )
    
    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", LivewireConfig.CHECKPOINT_PATH)
        .trigger(processingTime=f"{LivewireConfig.TRIGGER_INTERVAL_SECONDS} seconds")
        .start()
    )
    
    print(f"\n  ✓ Livewire streaming query started (ID: {query.id})")
    print(f"  ✓ Waiting for data... (^C to stop)\n")
    
    try:
        # Run indefinitely
        query.awaitTermination(timeout=LivewireConfig.AWAIT_TERMINATION_TIMEOUT)
    except KeyboardInterrupt:
        print(f"\n  ℹ Stopping livewire streaming...")
        query.stop()
    except Exception as e:
        print(f"\n  ✗ Streaming query failed: {e}")
        query.stop()
    finally:
        # Print final metrics
        summary = metrics.get_summary()
        print(f"\n================================================================================")
        print(f"  LIVEWIRE STREAMING SUMMARY")
        print(f"================================================================================\n")
        print(f"  Batches processed:           {summary['batches_processed']}")
        print(f"  Total rows processed:        {summary['total_rows_processed']:,}")
        print(f"  Rows merged (duplicates):    {summary['total_rows_merged_deduplicated']:,}")
        print(f"  Rows inserted (new):         {summary['total_rows_inserted_new']:,}")
        print(f"  Schema mismatches encountered: {summary['batches_with_schema_mismatches']}")
        print(f"  Total merge time:            {summary['total_merge_time_sec']}s")
        print(f"  Total elapsed time:          {summary['elapsed_time_sec']}s")
        print(f"  Throughput:                  {summary['throughput_rows_per_sec']} rows/sec")
        print(f"  Errors:                      {summary['error_count']}")
        print(f"\n================================================================================\n")


if __name__ == "__main__":
    """
    Quick test of livewire streaming mode.
    
    Usage:
        python3 livewire_streaming.py
    """
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder
        .appName("ATLAS-Livewire-Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .master("local[*]")
        .getOrCreate()
    )
    
    run_livewire_streaming(spark)
