import os
import time
import json
import statistics
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, date_format, sha2, concat_ws, lit, coalesce
from delta import DeltaTable
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
    
    # Mode: legacy | benchmark | dataframe | livewire
    MODE = " benchmark"
    
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
    
    # Compression: Zstd provides ~30% better compression than Snappy
    COMPRESSION_CODEC = "zstd"
    
    # Vacuum settings
    VACUUM_RETENTION_DAYS = 14
    VACUUM_RETENTION_HOURS = VACUUM_RETENTION_DAYS * 24
    VACUUM_ENABLED = True
    
    # Benchmark mode settings
    OPTIMIZE_EVERY_N_BATCHES = 5
    ENABLE_CHECKPOINTING = True
    
    # Horizontal Scaling Configuration
    SPARK_EXECUTOR_INSTANCES = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
    SPARK_EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "6"))
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower() == "true"
    SPARK_MIN_EXECUTORS = int(os.getenv("SPARK_MIN_EXECUTORS", "2"))
    SPARK_MAX_EXECUTORS = int(os.getenv("SPARK_MAX_EXECUTORS", "8"))
    SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "12"))


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
# DATA PREPARATION
# =============================================================================

def prepare_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add partition columns required for 5-level deep partitioning.
    
    Extracts partition_date from metric_time for date-based partitioning.
    Ensures all partition columns have non-null values for proper partitioning.
    """
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
    """Initialize Delta table with 5-level partitioning and Zstd compression."""
    print(f"         Partition structure: /{'/'.join(partition_cols)}/")
    print(f"         Compression: {PipelineConfig.COMPRESSION_CODEC}")
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .option("parquet.compression", PipelineConfig.COMPRESSION_CODEC)
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
    """
    source_count = source_df.count()
    
    delta_table = DeltaTable.forPath(spark, target_path)
    target_count = spark.read.format("delta").load(target_path).count()
    
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
    
    delta_table_after = DeltaTable.forPath(spark, target_path)
    target_count_after = spark.read.format("delta").load(target_path).count()
    
    rows_inserted = target_count_after - target_count
    rows_matched = source_count - rows_inserted
    match_ratio = (rows_matched / source_count * 100) if source_count > 0 else 0
    insert_ratio = (rows_inserted / source_count * 100) if source_count > 0 else 0
    
    history_df = delta_table_after.history(1)
    history_row = history_df.collect()[0]
    
    print(f"         ✓ MERGE Results (Day i vs Days 0..i-1):")
    print(f"           - Source rows (day i): {source_count:,}")
    print(f"           - Target rows before: {target_count:,}")
    print(f"           - Matched/Duplicates: {rows_matched:,} ({match_ratio:.1f}%)")
    print(f"           - Inserted/New: {rows_inserted:,} ({insert_ratio:.1f}%)")
    print(f"           - Target rows after: {target_count_after:,}")
    
    return {
        "operation": history_row["operation"],
        "operationMetrics": history_row["operationMetrics"],
        "timestamp": history_row["timestamp"],
        "source_count": source_count,
        "target_count_before": target_count,
        "target_count_after": target_count_after,
        "rows_matched": rows_matched,
        "rows_inserted": rows_inserted,
        "match_ratio_pct": match_ratio,
        "insert_ratio_pct": insert_ratio
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


def vacuum_old_files(spark: SparkSession, path: str, retention_hours: int = None) -> Dict:
    """Vacuum Delta table to remove old files beyond retention period."""
    retention_hours = retention_hours or PipelineConfig.VACUUM_RETENTION_HOURS
    
    delta_table = DeltaTable.forPath(spark, path)
    
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    print(f"         Running VACUUM with {retention_hours}h ({retention_hours // 24}d) retention...")
    vacuum_start = time.perf_counter()
    
    delta_table.vacuum(retention_hours)
    
    vacuum_elapsed = time.perf_counter() - vacuum_start
    
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    
    return {
        "retention_hours": retention_hours,
        "retention_days": retention_hours // 24,
        "vacuum_time_sec": round(vacuum_elapsed, 3),
        "status": "completed"
    }