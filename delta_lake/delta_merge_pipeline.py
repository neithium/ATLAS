"""
ATLAS Delta Lake Module - Refined Layer Deduplication Pipeline
================================================================================
High-Performance 'Source of Truth' pipeline with Delta Lake MERGE deduplication.

Architecture: Lakehouse Pattern for 400,000+ Device Scale
- Triple-Hash Composite Primary Key: (device_id, metric_time, application_customer_id)
- 5-Level Deep Partitioning: metric_type/date/platform_customer_id/application_customer_id/device_id
- 7-Day Rolling Window Overlap Handling via MERGE deduplication
- Storage Optimization: Parquet columnar format with Z-ORDER clustering

Pipeline Modes:
1. legacy   - Process 2 static files (file1_baseline + file2_overlap)
2. benchmark - Process partitioned benchmark data with incremental date-batch MERGE

Scalability Features:
- Date-partitioned batch processing (avoids OOM for 400K+ devices)
- Checkpoint-based fault tolerance (resume from last successful batch)
- Comprehensive latency metrics (per-batch, P50/P95/P99, throughput)
- Adaptive OPTIMIZE frequency (every N batches to balance latency vs compaction)

NOTE: Input data is ALREADY FLATTENED by upstream Spark processing layer.
"""

import json
import os
import shutil
import statistics
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, date_format, sha2, concat_ws, lit, when, coalesce, count
)
from pyspark.sql.types import StringType
from delta import DeltaTable
from delta.tables import DeltaMergeBuilder
from py4j.protocol import Py4JJavaError


# =============================================================================
# CONFIGURATION
# =============================================================================

class PipelineConfig:
    """Configuration for ATLAS Refined Layer Pipeline."""
    
    # Paths (configurable via CLI args)
    RAW_DATA_PATH = "/raw"
    REFINED_PATH = "/refined"
    CHECKPOINT_PATH = "/refined/_checkpoints"
    
    # Mode: legacy | benchmark
    MODE = "legacy"
    
    # Triple-Hash Composite Primary Key columns
    PRIMARY_KEY_COLUMNS = ["device_id", "metric_time", "application_customer_id"]
    
    # 5-Level Partition columns (order matters for directory structure)
    PARTITION_COLUMNS = [
        "report_type",
        "partition_date",
        "platform_customer_id",
        "application_customer_id",
        "device_id"
    ]
    
    # Z-ORDER clustering column for read optimization
    ZORDER_COLUMN = "metric_time"
    
    # Delta Lake optimizations
    TARGET_FILE_SIZE_MB = 128
    MAX_RECORDS_PER_FILE = 1_000_000
    
    # Benchmark mode settings
    OPTIMIZE_EVERY_N_BATCHES = 3  # Run OPTIMIZE after every N date batches
    ENABLE_CHECKPOINTING = True


# =============================================================================
# LATENCY TRACKER
# =============================================================================

class LatencyTracker:
    """Track and compute latency statistics across batch operations."""
    
    def __init__(self):
        self.batch_latencies: List[float] = []
        self.merge_latencies: List[float] = []
        self.read_latencies: List[float] = []
        self.rows_processed: List[int] = []
        self.pipeline_start: float = 0
        self.total_rows: int = 0
        
    def start_pipeline(self):
        self.pipeline_start = time.perf_counter()
        
    def record_batch(self, batch_time: float, merge_time: float, read_time: float, rows: int):
        self.batch_latencies.append(batch_time)
        self.merge_latencies.append(merge_time)
        self.read_latencies.append(read_time)
        self.rows_processed.append(rows)
        self.total_rows += rows
        
    def _percentile(self, data: List[float], p: float) -> float:
        if not data:
            return 0.0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * p / 100
        f = int(k)
        c = f + 1 if f + 1 < len(sorted_data) else f
        return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])
    
    def get_summary(self) -> Dict:
        elapsed = time.perf_counter() - self.pipeline_start
        throughput = self.total_rows / elapsed if elapsed > 0 else 0
        
        return {
            "total_batches": len(self.batch_latencies),
            "total_rows": self.total_rows,
            "total_elapsed_sec": round(elapsed, 2),
            "throughput_rows_per_sec": round(throughput, 1),
            "batch_latency": {
                "min": round(min(self.batch_latencies), 3) if self.batch_latencies else 0,
                "max": round(max(self.batch_latencies), 3) if self.batch_latencies else 0,
                "mean": round(statistics.mean(self.batch_latencies), 3) if self.batch_latencies else 0,
                "p50": round(self._percentile(self.batch_latencies, 50), 3),
                "p95": round(self._percentile(self.batch_latencies, 95), 3),
                "p99": round(self._percentile(self.batch_latencies, 99), 3),
            },
            "merge_latency": {
                "min": round(min(self.merge_latencies), 3) if self.merge_latencies else 0,
                "max": round(max(self.merge_latencies), 3) if self.merge_latencies else 0,
                "mean": round(statistics.mean(self.merge_latencies), 3) if self.merge_latencies else 0,
                "p50": round(self._percentile(self.merge_latencies, 50), 3),
                "p95": round(self._percentile(self.merge_latencies, 95), 3),
                "p99": round(self._percentile(self.merge_latencies, 99), 3),
            },
            "read_latency": {
                "min": round(min(self.read_latencies), 3) if self.read_latencies else 0,
                "max": round(max(self.read_latencies), 3) if self.read_latencies else 0,
                "mean": round(statistics.mean(self.read_latencies), 3) if self.read_latencies else 0,
            }
        }


# =============================================================================
# CHECKPOINT MANAGER
# =============================================================================

class CheckpointManager:
    """Manage checkpoint state for fault-tolerant batch processing."""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = checkpoint_dir
        self.state_file = f"{checkpoint_dir}/pipeline_state.json"
        
    def ensure_dir(self):
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
    def load_state(self) -> Dict:
        """Load checkpoint state or return empty state."""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"         ⚠ Could not load checkpoint: {e}")
        return {"completed_batches": [], "last_batch": None, "total_rows_processed": 0}
    
    def save_state(self, state: Dict):
        """Persist checkpoint state."""
        self.ensure_dir()
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2, default=str)
        except Exception as e:
            print(f"         ⚠ Could not save checkpoint: {e}")
            
    def mark_batch_complete(self, batch_id: str, rows: int, state: Dict) -> Dict:
        """Mark a batch as completed."""
        state["completed_batches"].append(batch_id)
        state["last_batch"] = batch_id
        state["total_rows_processed"] += rows
        state["last_updated"] = datetime.now().isoformat()
        self.save_state(state)
        return state
    
    def reset(self):
        """Clear checkpoint state for fresh run."""
        if os.path.exists(self.state_file):
            os.remove(self.state_file)


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
    """
    # Check if partition_date already exists (benchmark data has it)
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
# DELTA TABLE OPERATIONS
# =============================================================================

def delta_table_exists(spark: SparkSession, path: str) -> bool:
    """Check if Delta table exists at path."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def initialize_delta_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_cols: list
) -> None:
    """Initialize Delta table with 5-level partitioning and Parquet storage."""
    print(f"         Partition structure: /{'/'.join(partition_cols)}/")
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .option("parquet.compression", "snappy")
        .option("parquet.block.size", str(PipelineConfig.TARGET_FILE_SIZE_MB * 1024 * 1024))
        .save(path)
    )


def append_to_delta_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_cols: list
) -> None:
    """Append data to existing Delta table (for initial batch in benchmark mode)."""
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy(*partition_cols)
        .save(path)
    )


def execute_merge_deduplication(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame
) -> dict:
    """
    Execute Delta MERGE for deduplication using Triple-Hash composite key.
    
    MERGE Logic:
    - WHEN MATCHED: Do nothing (record exists, ignore duplicate)
    - WHEN NOT MATCHED: Insert all columns (new unique record)
    """
    delta_table = DeltaTable.forPath(spark, target_path)
    
    merge_condition = """
        target.device_id = source.device_id 
        AND target.metric_time = source.metric_time 
        AND target.application_customer_id = source.application_customer_id
    """
    
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            merge_builder = (
                delta_table.alias("target")
                .merge(source_df.alias("source"), merge_condition)
                .whenNotMatchedInsertAll()
            )
            merge_builder.execute()
            break
        except Py4JJavaError as exc:
            msg = str(exc)
            is_missing_file_error = (
                "SparkFileNotFoundException" in msg
                or "does not exist" in msg
            )
            if not is_missing_file_error or attempt == max_retries:
                raise
            print(f"         ⚠ MERGE retry {attempt + 1}/{max_retries} after file metadata refresh...")
            spark.catalog.clearCache()
            delta_table = DeltaTable.forPath(spark, target_path)
            time.sleep(1)
    
    history_df = delta_table.history(1)
    history_row = history_df.collect()[0]
    
    return {
        "operation": history_row["operation"],
        "operationMetrics": history_row["operationMetrics"],
        "timestamp": history_row["timestamp"]
    }


def optimize_delta_table(spark: SparkSession, path: str, zorder_col: str) -> dict:
    """Execute OPTIMIZE with Z-ORDER to resolve small file issues."""
    delta_table = DeltaTable.forPath(spark, path)
    
    optimize_result = delta_table.optimize().executeZOrderBy(zorder_col)
    
    if optimize_result.count() > 0:
        metrics_row = optimize_result.collect()[0]
        return {
            "numFilesAdded": metrics_row["metrics"]["numFilesAdded"],
            "numFilesRemoved": metrics_row["metrics"]["numFilesRemoved"],
            "numBatches": metrics_row["metrics"]["numBatches"],
            "totalConsideredFiles": metrics_row["metrics"]["totalConsideredFiles"],
            "totalFilesSkipped": metrics_row["metrics"]["totalFilesSkipped"],
            "preserveInsertionOrder": metrics_row["metrics"]["preserveInsertionOrder"]
        }
    return {"status": "no_files_to_optimize"}


def vacuum_old_files(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """Vacuum old Delta log files beyond retention period."""
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)


# =============================================================================
# VERIFICATION & REPORTING
# =============================================================================

def verify_deduplication(spark: SparkSession, path: str, total_input: int) -> dict:
    """Verify deduplication results and generate metrics report."""
    final_df = spark.read.format("delta").load(path)
    final_count = final_df.count()
    
    duplicates_dropped = total_input - final_count
    dedup_ratio = (duplicates_dropped / total_input * 100) if total_input > 0 else 0
    
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
    
    detail_df = delta_table.detail()
    detail = detail_df.collect()[0]
    
    print(f"  - Format: {detail['format']}")
    print(f"  - Partitions: {detail['partitionColumns']}")
    print(f"  - Num Files: {detail['numFiles']}")
    print(f"  - Size (bytes): {detail['sizeInBytes']}")
    
    print("\n  Recent Operations:")
    history_df = delta_table.history(5)
    for row in history_df.collect():
        print(f"  - {row['timestamp']}: {row['operation']}")


def print_latency_report(tracker: LatencyTracker):
    """Print comprehensive latency report with percentiles."""
    summary = tracker.get_summary()
    
    print("\n" + "=" * 80)
    print("  LATENCY & THROUGHPUT REPORT")
    print("=" * 80)
    
    print(f"\n  Overall Performance:")
    print(f"  - Total batches processed: {summary['total_batches']}")
    print(f"  - Total rows processed:    {summary['total_rows']:,}")
    print(f"  - Total elapsed time:      {summary['total_elapsed_sec']:.2f}s")
    print(f"  - Throughput:              {summary['throughput_rows_per_sec']:,.1f} rows/sec")
    
    batch = summary['batch_latency']
    print(f"\n  Batch Latency (seconds):")
    print(f"  - Min:  {batch['min']:.3f}s")
    print(f"  - Max:  {batch['max']:.3f}s")
    print(f"  - Mean: {batch['mean']:.3f}s")
    print(f"  - P50:  {batch['p50']:.3f}s")
    print(f"  - P95:  {batch['p95']:.3f}s")
    print(f"  - P99:  {batch['p99']:.3f}s")
    
    merge = summary['merge_latency']
    print(f"\n  MERGE Operation Latency (seconds):")
    print(f"  - Min:  {merge['min']:.3f}s")
    print(f"  - Max:  {merge['max']:.3f}s")
    print(f"  - Mean: {merge['mean']:.3f}s")
    print(f"  - P50:  {merge['p50']:.3f}s")
    print(f"  - P95:  {merge['p95']:.3f}s")
    print(f"  - P99:  {merge['p99']:.3f}s")
    
    read = summary['read_latency']
    print(f"\n  Read Latency (seconds):")
    print(f"  - Min:  {read['min']:.3f}s")
    print(f"  - Max:  {read['max']:.3f}s")
    print(f"  - Mean: {read['mean']:.3f}s")


# =============================================================================
# LEGACY MODE PIPELINE
# =============================================================================

def run_legacy_pipeline(spark: SparkSession, tracker: LatencyTracker) -> dict:
    """Run legacy 2-file pipeline (original behavior)."""
    print("\n" + "-" * 80)
    print("[STEP 2] Reading baseline data (File 1 - pre-flattened)...")
    print("-" * 80)
    
    read_start = time.perf_counter()
    df1_raw = spark.read.parquet(f"{PipelineConfig.RAW_DATA_PATH}/file1_baseline.parquet")
    df1_prepared = prepare_partition_columns(df1_raw)
    file1_count = df1_prepared.count()
    read_elapsed = time.perf_counter() - read_start
    
    print(f"         ✓ File 1 rows: {file1_count:,}")
    print(f"         ✓ Read time: {read_elapsed:.2f}s")
    
    print("\n" + "-" * 80)
    print("[STEP 3] Creating Delta table with 5-level partitioning...")
    print("-" * 80)
    
    batch_start = time.perf_counter()
    initialize_delta_table(
        spark=spark,
        df=df1_prepared,
        path=PipelineConfig.REFINED_PATH,
        partition_cols=PipelineConfig.PARTITION_COLUMNS
    )
    batch_elapsed = time.perf_counter() - batch_start
    
    print(f"         ✓ Delta table created at {PipelineConfig.REFINED_PATH}")
    print(f"         ✓ Baseline records: {file1_count:,}")
    
    tracker.record_batch(batch_elapsed, 0, read_elapsed, file1_count)
    
    print("\n" + "-" * 80)
    print("[STEP 4] Reading overlap data (File 2 - 7-day rolling window)...")
    print("-" * 80)
    
    read_start = time.perf_counter()
    df2_raw = spark.read.parquet(f"{PipelineConfig.RAW_DATA_PATH}/file2_overlap.parquet")
    df2_prepared = prepare_partition_columns(df2_raw)
    file2_count = df2_prepared.count()
    read_elapsed = time.perf_counter() - read_start
    
    print(f"         ✓ File 2 rows: {file2_count:,}")
    print(f"         ✓ Expected overlap with File 1 (6-day window)")
    
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
    batch_elapsed = time.perf_counter() - read_start
    
    print(f"         ✓ MERGE completed in {merge_elapsed:.2f}s")
    print(f"         ✓ Operation: {merge_metrics['operation']}")
    
    tracker.record_batch(batch_elapsed, merge_elapsed, read_elapsed, file2_count)
    
    return {"file1_count": file1_count, "file2_count": file2_count}


# =============================================================================
# BENCHMARK MODE PIPELINE
# =============================================================================

def get_file_dates(spark: SparkSession, data_path: str) -> List[str]:
    """
    Discover available file_date partitions from benchmark data.
    
    file_date represents the date each batch was "received" - this is the
    partition key for incoming batches. Each file_date contains a 7-day
    rolling window of metric_time data.
    """
    try:
        df = spark.read.parquet(data_path)
        dates = (
            df.select("file_date")
            .distinct()
            .orderBy("file_date")
            .collect()
        )
        return [str(row["file_date"]) for row in dates]
    except Exception as e:
        print(f"         ⚠ Could not read file_dates: {e}")
        return []


def run_benchmark_pipeline(
    spark: SparkSession,
    tracker: LatencyTracker,
    checkpoint_mgr: CheckpointManager,
    resume: bool = False
) -> dict:
    """
    Run benchmark pipeline with incremental date-batch MERGE.
    
    Process flow:
    1. Discover all date partitions in benchmark_data
    2. For first partition: Initialize Delta table
    3. For subsequent partitions: MERGE incrementally
    4. Run OPTIMIZE every N batches
    5. Track latency per batch with checkpointing
    """
    benchmark_path = f"{PipelineConfig.RAW_DATA_PATH}/benchmark_data"
    
    print("\n" + "-" * 80)
    print("[STEP 2] Discovering file_date batches in benchmark data...")
    print("-" * 80)
    print("         ℹ file_date = date batch was received (each contains 7-day rolling window)")
    
    file_dates = get_file_dates(spark, benchmark_path)
    
    if not file_dates:
        print("         ✗ No file_date partitions found in benchmark data!")
        print(f"         ✗ Expected path: {benchmark_path}")
        return {"error": "no_data"}
    
    print(f"         ✓ Found {len(file_dates)} daily file batches")
    print(f"         ✓ File date range: {file_dates[0]} → {file_dates[-1]}")
    
    # Load checkpoint state
    state = checkpoint_mgr.load_state() if resume else {"completed_batches": [], "last_batch": None, "total_rows_processed": 0}
    completed = set(state.get("completed_batches", []))
    
    if completed:
        print(f"         ✓ Resuming from checkpoint: {len(completed)} batches already processed")
    
    # Filter file_dates to process
    file_dates_to_process = [d for d in file_dates if d not in completed]
    
    if not file_dates_to_process:
        print("         ✓ All file batches already processed!")
        return {"total_rows": state.get("total_rows_processed", 0)}
    
    print(f"         ✓ File batches to process: {len(file_dates_to_process)}")
    
    total_rows_processed = state.get("total_rows_processed", 0)

    if resume:
        table_initialized = delta_table_exists(spark, PipelineConfig.REFINED_PATH)
    else:
        table_initialized = False
        if os.path.exists(PipelineConfig.REFINED_PATH):
            print(f"         ℹ Fresh benchmark run detected (resume=False): clearing existing output at {PipelineConfig.REFINED_PATH}")
            shutil.rmtree(PipelineConfig.REFINED_PATH, ignore_errors=True)

    optimize_counter = 0
    
    print("\n" + "-" * 80)
    print("[STEP 3] Processing file batches with incremental MERGE...")
    print("-" * 80)
    print("         ℹ Each batch = 2016 rows/device (7-day window), only 288 new/device")
    
    for idx, file_date in enumerate(file_dates_to_process, start=1):
        batch_start = time.perf_counter()
        
        print(f"\n  ┌─ Batch {idx}/{len(file_dates_to_process)}: file_date={file_date}")
        
        # Read file_date batch (contains 7-day rolling window of metric_time)
        read_start = time.perf_counter()
        batch_df = (
            spark.read.parquet(benchmark_path)
            .filter(col("file_date") == file_date)
            .drop("file_date")  # Not needed in refined layer
        )
        prepared_df = prepare_partition_columns(batch_df)
        row_count = prepared_df.count()
        read_elapsed = time.perf_counter() - read_start
        
        print(f"  │  Rows: {row_count:,} | Read: {read_elapsed:.2f}s")
        
        # Initialize or MERGE
        merge_elapsed = 0
        if not table_initialized:
            # First batch: Initialize Delta table
            init_start = time.perf_counter()
            initialize_delta_table(
                spark=spark,
                df=prepared_df,
                path=PipelineConfig.REFINED_PATH,
                partition_cols=PipelineConfig.PARTITION_COLUMNS
            )
            merge_elapsed = time.perf_counter() - init_start
            table_initialized = True
            print(f"  │  Action: INIT | Time: {merge_elapsed:.2f}s")
        else:
            # Subsequent batches: MERGE
            merge_start = time.perf_counter()
            execute_merge_deduplication(
                spark=spark,
                target_path=PipelineConfig.REFINED_PATH,
                source_df=prepared_df
            )
            merge_elapsed = time.perf_counter() - merge_start
            print(f"  │  Action: MERGE | Time: {merge_elapsed:.2f}s")
        
        batch_elapsed = time.perf_counter() - batch_start
        throughput = row_count / batch_elapsed if batch_elapsed > 0 else 0
        
        print(f"  │  Batch total: {batch_elapsed:.2f}s | Throughput: {throughput:,.0f} rows/s")
        
        # Track metrics
        tracker.record_batch(batch_elapsed, merge_elapsed, read_elapsed, row_count)
        total_rows_processed += row_count
        
        # Checkpoint
        if PipelineConfig.ENABLE_CHECKPOINTING:
            state = checkpoint_mgr.mark_batch_complete(file_date, row_count, state)
        
        # Periodic OPTIMIZE
        optimize_counter += 1
        if optimize_counter >= PipelineConfig.OPTIMIZE_EVERY_N_BATCHES:
            print(f"  │  Running OPTIMIZE (every {PipelineConfig.OPTIMIZE_EVERY_N_BATCHES} batches)...")
            opt_start = time.perf_counter()
            optimize_delta_table(spark, PipelineConfig.REFINED_PATH, PipelineConfig.ZORDER_COLUMN)
            opt_elapsed = time.perf_counter() - opt_start
            print(f"  │  OPTIMIZE completed in {opt_elapsed:.2f}s")
            optimize_counter = 0
        
        print(f"  └─ ✓ Batch complete")
    
    # Final OPTIMIZE if needed
    if optimize_counter > 0:
        print("\n  Running final OPTIMIZE...")
        opt_start = time.perf_counter()
        optimize_delta_table(spark, PipelineConfig.REFINED_PATH, PipelineConfig.ZORDER_COLUMN)
        print(f"  ✓ Final OPTIMIZE completed in {time.perf_counter() - opt_start:.2f}s")
    
    return {"total_rows": total_rows_processed}


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="ATLAS Delta Lake Deduplication Pipeline")
    parser.add_argument('--input', type=str, default=PipelineConfig.RAW_DATA_PATH, 
                        help='Input raw parquet directory')
    parser.add_argument('--output', type=str, default=PipelineConfig.REFINED_PATH, 
                        help='Output refined delta directory')
    parser.add_argument('--mode', type=str, choices=['legacy', 'benchmark'], default='legacy',
                        help='Pipeline mode: legacy (2 files) or benchmark (partitioned data)')
    parser.add_argument('--resume', action='store_true', 
                        help='Resume from last checkpoint (benchmark mode only)')
    parser.add_argument('--reset', action='store_true',
                        help='Reset checkpoints and start fresh')
    parser.add_argument('--optimize-every', type=int, default=3,
                        help='Run OPTIMIZE after every N batches (benchmark mode)')
    return parser.parse_args()


def main():
    """
    ATLAS Refined Layer - Delta Lake Deduplication Pipeline
    
    Modes:
    - legacy: Process 2 static files (file1_baseline + file2_overlap)
    - benchmark: Process partitioned benchmark data with incremental MERGE
    """
    args = parse_args()
    
    # Update config from args
    PipelineConfig.RAW_DATA_PATH = args.input
    PipelineConfig.REFINED_PATH = args.output
    PipelineConfig.MODE = args.mode
    PipelineConfig.OPTIMIZE_EVERY_N_BATCHES = args.optimize_every
    PipelineConfig.CHECKPOINT_PATH = f"{args.output}/_checkpoints"
    
    print("\n" + "=" * 80)
    print("  ATLAS - REFINED LAYER DELTA LAKE PIPELINE")
    print("  High-Performance Deduplication for 400K+ Device Scale")
    print("=" * 80)
    print(f"\n  Mode: {args.mode.upper()}")
    print("  Architecture:")
    print("  - Triple-Hash Key: (device_id, metric_time, application_customer_id)")
    print("  - 5-Level Partitioning: report_type/date/pcid/acid/device_id")
    print("  - Storage: Parquet columnar format with Snappy compression")
    print("  - Optimization: Z-ORDER by metric_time for query locality")
    
    if args.mode == "benchmark":
        print(f"  - Batch Processing: OPTIMIZE every {args.optimize_every} batches")
        print(f"  - Fault Tolerance: Checkpointing {'enabled' if PipelineConfig.ENABLE_CHECKPOINTING else 'disabled'}")
    
    # Initialize components
    print("\n" + "-" * 80)
    print("[STEP 1] Initializing Spark session with Delta Lake...")
    print("-" * 80)
    
    tracker = LatencyTracker()
    tracker.start_pipeline()
    
    checkpoint_mgr = CheckpointManager(PipelineConfig.CHECKPOINT_PATH)
    if args.reset:
        checkpoint_mgr.reset()
        print("         ✓ Checkpoints reset")
    
    spark = create_spark_session()
    print("         ✓ SparkSession created with Delta Lake 3.1.0")
    print("         ✓ Parquet vectorized reader enabled")
    print("         ✓ Adaptive Query Execution enabled")
    
    # Run appropriate pipeline
    if args.mode == "legacy":
        result = run_legacy_pipeline(spark, tracker)
        total_input = result.get("file1_count", 0) + result.get("file2_count", 0)
    else:
        result = run_benchmark_pipeline(spark, tracker, checkpoint_mgr, resume=args.resume)
        if "error" in result:
            spark.stop()
            return
        total_input = result.get("total_rows", 0)
    
    # Final verification
    print("\n" + "-" * 80)
    print("[FINAL] Verifying deduplication results...")
    print("-" * 80)
    
    verification = verify_deduplication(spark, PipelineConfig.REFINED_PATH, total_input)
    print_table_info(spark, PipelineConfig.REFINED_PATH)
    
    # Print results
    print("\n" + "=" * 80)
    print("  PIPELINE EXECUTION SUMMARY")
    print("=" * 80)
    
    print(f"\n  Mode: {args.mode.upper()}")
    print(f"\n  Input Statistics:")
    print(f"  - Total input rows:      {verification['total_input']:,}")
    
    print(f"\n  Deduplication Results:")
    print(f"  - Final table rows:      {verification['final_count']:,}")
    print(f"  - Duplicates dropped:    {verification['duplicates_dropped']:,}")
    print(f"  - Dedup ratio:           {verification['deduplication_ratio']:.1f}%")
    print(f"  - Partition count:       {verification['partition_count']}")
    
    # Print latency report
    print_latency_report(tracker)
    
    print(f"\n  Architecture Verification:")
    print(f"  ✓ Triple-Hash Key: device_id + metric_time + application_customer_id")
    print(f"  ✓ 5-Level Partitioning: {'/'.join(PipelineConfig.PARTITION_COLUMNS)}")
    print(f"  ✓ Storage Format: Parquet (Delta Lake native)")
    print(f"  ✓ Z-ORDER Clustering: {PipelineConfig.ZORDER_COLUMN}")
    
    print("\n" + "=" * 80)
    print("  ATLAS REFINED LAYER PIPELINE COMPLETE")
    print("=" * 80 + "\n")
    
    spark.stop()
    return verification


if __name__ == "__main__":
    main()
