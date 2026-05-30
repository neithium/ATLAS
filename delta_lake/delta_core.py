import os
import time
import json
import statistics
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, date_format, sha2, concat_ws, lit, coalesce, row_number
import pyspark.sql.functions as F
from pyspark.sql.window import Window
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
    
    # 1-Level Partition to fix small file problem
    PARTITION_COLUMNS = ["partition_date"]
    
    # Z-ORDER clustering column for read optimization
    ZORDER_COLUMN = "application_customer_id, device_id"
    
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
    SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "8g")
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "8g")
    
    SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower() == "true"
    SPARK_MIN_EXECUTORS = int(os.getenv("SPARK_MIN_EXECUTORS", "2"))
    SPARK_MAX_EXECUTORS = int(os.getenv("SPARK_MAX_EXECUTORS", "8"))
    SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")) #earlier 12    

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
    Prepares partition columns defensively.
    Extracts partition_date from metric_time for date-based partitioning.
    Ensures all partition columns have non-null values for proper partitioning.
    """
    from pyspark.sql.functions import date_format, to_date, col, lit, coalesce

    batch_cols = df.columns
    
    # Defensively create partition_date
    if "partition_date" in batch_cols:
        prepared = df.withColumn(
            "partition_date",
            date_format(col("partition_date"), "yyyy-MM-dd")
        )
    elif "metric_time" in batch_cols:
        prepared = df.withColumn(
            "partition_date",
            date_format(to_date(col("metric_time")), "yyyy-MM-dd")
        )
    else:
        # If no date source, create a null column that will be filled later
        prepared = df.withColumn("partition_date", lit(None).cast("string"))

    # Defensively apply coalesce to known columns if they exist
    columns_to_coalesce = {
        "report_type": "unknown",
        "platform_customer_id": "unknown",
        "application_customer_id": "unknown",
        "device_id": "unknown"
    }

    for col_name, default_value in columns_to_coalesce.items():
        if col_name in batch_cols:
            prepared = prepared.withColumn(col_name, coalesce(col(col_name), lit(default_value)))
        else:
            # If the column doesn't exist, create it with the default value
            prepared = prepared.withColumn(col_name, lit(default_value))
            
    return prepared


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
    """Check if a Delta table exists at the given path."""
    return DeltaTable.isDeltaTable(spark, path)

def initialize_delta_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_cols: List[str]
) -> None:
    """Create a new Delta table with partitioning."""
    print(f"        Initializing Delta table at: {path}")
    (
        df.write
        .format("delta")
        .partitionBy(*partition_cols)
        .mode("overwrite")
        .option("maxRecordsPerFile", PipelineConfig.MAX_RECORDS_PER_FILE)
        .save(path)
    )
    # Inject advanced Delta Lake table properties
    print("        Applying Auto-Compaction and Optimized Writes...")
    spark.sql(f"""
        ALTER TABLE delta.`{path}` SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)

def execute_merge_deduplication(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    max_retries: int = 3
) -> dict:
    """
    Execute MERGE operation with deduplication logic and resilience.
    
    - Inserts new records.
    - Updates existing records if source has a more recent timestamp.
    - Deduplicates source data to prevent duplicate insertions.
    - Implements retry logic and Delta Log consistency checks.
    """
    import time
    
    # Deduplicate source data based on PK, keeping the latest record
    # This is critical for idempotent writes when re-processing batches
    source_deduped = (
        source_df
        .withColumn("row_num", 
            F.row_number().over(
                Window.partitionBy(*PipelineConfig.PRIMARY_KEY_COLUMNS)
                      .orderBy(F.col("metric_time").desc())
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    merge_condition = """
        target.partition_date = source.partition_date
        AND target.device_id = source.device_id 
        AND target.metric_time = source.metric_time 
        AND target.application_customer_id = source.application_customer_id
    """
    
    # Retry logic with exponential backoff for transient failures
    for attempt in range(max_retries):
        try:
            # Refresh Delta table metadata to ensure log consistency
            delta_table = DeltaTable.forPath(spark, target_path)
            
            # Force invalidate the plan cache to ensure fresh file resolution
            spark.catalog.refreshByPath(target_path)
            
            # Execute merge with materialized source to avoid reference errors
            (
                delta_table.alias("target")
                .merge(
                    source_deduped.alias("source"),
                    condition=merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            
            return {"status": "success", "merge_executed": True}
            
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a transient file-not-found error
            if "does not exist" in error_msg or "SparkFileNotFoundException" in error_msg:
                if attempt < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 2 ** attempt
                    print(f"        ⚠ Transient file error on attempt {attempt + 1}/{max_retries}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"        ❌ MERGE failed after {max_retries} attempts: {error_msg}")
                    raise
            else:
                # Non-transient error - fail immediately
                print(f"        ❌ MERGE failed with non-transient error: {error_msg}")
                raise
    
    return {"status": "failed", "error": "Max retries exceeded"}

def optimize_delta_table(spark: SparkSession, path: str, zorder_col: Optional[str] = None):
    """Run OPTIMIZE and Z-ORDER on the Delta table."""
    delta_table = DeltaTable.forPath(spark, path)
    if zorder_col:
        print(f"        Running OPTIMIZE and Z-ORDER by {zorder_col}...")
        delta_table.optimize().executeZOrderBy(zorder_col)
    else:
        print("        Running OPTIMIZE...")
        delta_table.optimize().executeCompaction()

def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int):
    """Vacuum old files from the Delta table."""
    if not PipelineConfig.VACUUM_ENABLED:
        print("        VACUUM is disabled.")
        return
        
    print(f"        Vacuuming files older than {retention_hours} hours...")
    delta_table = DeltaTable.forPath(spark, path)
    try:
        delta_table.vacuum(retentionHours=retention_hours)
    except Py4JJavaError as e:
        # Gracefully handle error if retention period is not met
        if "retention period" in str(e.java_exception):
            print(f"        ⚠ VACUUM skipped: {e.java_exception}")
        else:
            raise e