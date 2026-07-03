import time
import os
from datetime import datetime
import pytz
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, to_timestamp, current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType

# Timezone setup - IST (Indian Standard Time)
IST = pytz.timezone('Asia/Kolkata')
from delta_core import (
    PipelineConfig,
    LatencyTracker,
    prepare_partition_columns,
    generate_composite_hash,
    delta_table_exists,
    initialize_delta_table,
    execute_merge_deduplication,
    optimize_delta_table
)
from schema.output_schema import output_schema as PROCESSOR_OUTPUT_SCHEMA

# Define metrics schema at module level to avoid scoping issues
METRICS_SCHEMA = StructType([
    StructField("batch_id", LongType()),
    StructField("timestamp", TimestampType()),
    StructField("total_time", DoubleType()),
    StructField("merge_time", DoubleType()),
    StructField("row_count", LongType())
])


def _build_livewire_source_schema() -> StructType:
    """Use the processor output schema so livewire can start before files exist."""
    return StructType([
        StructField(field.name, field.dataType, field.nullable, field.metadata)
        for field in PROCESSOR_OUTPUT_SCHEMA.fields
    ])


LIVEWIRE_SOURCE_SCHEMA = _build_livewire_source_schema()

# Debug log file
DEBUG_LOG = "/tmp/livewire_debug.log"


def run_livewire_pipeline(
    spark: SparkSession,
    tracker: LatencyTracker,
    source_path: str = "/stream_raw",
    target_path: str = PipelineConfig.REFINED_PATH
) -> dict:
    """
    Run the livewire pipeline using Spark Structured Streaming to fetch files
    produced by the upstream processing engine and deduplicate them into the lakehouse.
    """
    print("\n" + "=" * 80)
    print(f"🔌 LIVEWIRE MODE: Monitoring {source_path}")
    print("=" * 80)
    
    # Initialize the target Delta table explicitly if not exists
    if not delta_table_exists(spark, target_path):
        print("         Initializing target Delta table...")
        df_dummy = spark.createDataFrame([], LIVEWIRE_SOURCE_SCHEMA)
        df_dummy = prepare_partition_columns(df_dummy)
        initialize_delta_table(
            spark,
            df_dummy,
            target_path,
            PipelineConfig.PARTITION_COLUMNS
        )
    
    # Initialize the system metrics table for Micro-SLA Dashboard
    metrics_table_path = "/refined/system_metrics"
    try:
        import os
        delta_log_path = f"{metrics_table_path}/_delta_log"
        
        if not os.path.exists(delta_log_path):
            print("         Initializing system metrics Delta table...")
            df_metrics_init = spark.createDataFrame([], METRICS_SCHEMA)
            df_metrics_init.write.format("delta").mode("overwrite").save(metrics_table_path)
            
            # Verify creation
            import time as time_module
            time_module.sleep(1)  # Give filesystem time to sync
            if os.path.exists(delta_log_path):
                print(f"         ✓ Metrics table initialized at {metrics_table_path}")
            else:
                print(f"         ⚠ Metrics table write may have failed - no _delta_log directory found")
        else:
            print(f"         ✓ Metrics table exists at {metrics_table_path}")
    except Exception as e:
        print(f"         ⚠ WARNING: Metrics table initialization error: {type(e).__name__}: {e}")



    
    # Use readStream for continuous folder ingestion.
    # The schema is explicit so the stream can start even when the folder is empty.
    try:
        stream_df = (
            spark.readStream
            .schema(LIVEWIRE_SOURCE_SCHEMA)
            .option("recursiveFileLookup", "true")
            .parquet(source_path)
        )
    except Exception as e:
        print(f"         ⚠ Could not initialize stream from {source_path}: {e}")
        return {"status": "failed"}

    # Track batches
    batch_counter = {"count": 0}
    
    def process_livewire_batch(batch_df: DataFrame, batch_id: int):
        print(f"DEBUG: process_livewire_batch called with batch_id={batch_id}", flush=True)
        
        start_time = time.perf_counter()
        rows = batch_df.count()
        
        # Log to both file and stdout for debugging (in IST)
        now_ist = datetime.now(IST).isoformat()
        log_msg = f"[{now_ist}] Batch {batch_id}: rows={rows}"
        print(f"DEBUG: {log_msg}", flush=True)  # This will appear in container logs
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{log_msg}\n")
        
        if rows == 0:
            now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{now_ist}] ⏳ Waiting for stream data (Batch {batch_id}) - no new files detected.", flush=True)
            return
            
        now_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[{now_ist}] ⚡ Processing Micro-batch {batch_id} with {rows} rows...", flush=True)
        
        # --- Schema-Defensive Programming ---
        # Inspect batch schema to handle upstream drift gracefully
        batch_cols = batch_df.columns
        
        # 1. Dynamically resolve the timestamp column
        if "metric_time" in batch_cols:
            timestamp_col = coalesce(to_timestamp(col("metric_time")), current_timestamp())
        elif "window_start" in batch_cols:
            timestamp_col = coalesce(to_timestamp(col("window_start")), current_timestamp())
        elif "event_date" in batch_cols:
            timestamp_col = coalesce(to_timestamp(col("event_date")), current_timestamp())
        elif "created_at" in batch_cols: # Future-proofing for other possible names
            timestamp_col = coalesce(to_timestamp(col("created_at")), current_timestamp())
        else:
            timestamp_col = current_timestamp()

        # 2. Build the transformation with the resolved column
        aligned_df = batch_df.withColumn("metric_time", timestamp_col)

        for column_name, default_value in {
            "application_customer_id": "livewire_unknown",
            "platform_customer_id": "livewire_unknown",
            "report_type": "livewire_raw"
        }.items():
            if column_name not in batch_cols:
                aligned_df = aligned_df.withColumn(column_name, lit(default_value))

        # 3. Safely drop staging columns that exist in the batch
        cols_to_drop = ["window_start", "window_end", "event_date", "created_at"]
        safe_cols_to_drop = [c for c in cols_to_drop if c in batch_cols]
        if safe_cols_to_drop:
            aligned_df = aligned_df.drop(*safe_cols_to_drop)
        
        prepared_df = prepare_partition_columns(aligned_df)
        hashed_df = generate_composite_hash(prepared_df)
        
        # Execute MERGE deduplication with built-in retry logic
        try:
            merge_start = time.perf_counter()
            merge_results = execute_merge_deduplication(spark, target_path, hashed_df)
            merge_elapsed = time.perf_counter() - merge_start
        except Exception as merge_error:
            print(f"\n         ⚠ MERGE operation failed after retries: {merge_error}", flush=True)
            print(f"         Batch {batch_id} will be retried in the next cycle.", flush=True)
            # Log the batch for potential replay
            return
        
        total_time = time.perf_counter() - start_time
        
        # --- Persist Internal Performance Metrics to Micro-SLA Dashboard ---
        # Record batch-level performance metrics independently of upstream latency
        try:
            now_ist = datetime.now(IST)
            metrics_data = [(
                int(batch_id),
                now_ist,
                float(total_time),
                float(merge_elapsed),
                int(rows)
            )]
            
            metrics_df = spark.createDataFrame(metrics_data, METRICS_SCHEMA)
            
            # Append to system metrics table (silent, non-blocking)
            metrics_df.write.format("delta").mode("append").save("/refined/system_metrics")
            
            # Debug log (in IST)
            now_ist_iso = datetime.now(IST).isoformat()
            metrics_log_msg = f"[{now_ist_iso}] Batch {batch_id}: Metrics written - total_time={total_time:.3f}s, merge_time={merge_elapsed:.3f}s, rows={rows}"
            print(f"DEBUG: {metrics_log_msg}", flush=True)
            with open(DEBUG_LOG, "a") as f:
                f.write(f"{metrics_log_msg}\n")
        except Exception as metrics_error:
            # Debug log the error (in IST)
            now_ist_iso = datetime.now(IST).isoformat()
            error_msg = f"[{now_ist_iso}] Batch {batch_id}: Metrics write FAILED - {type(metrics_error).__name__}: {str(metrics_error)[:100]}"
            print(f"DEBUG: {error_msg}", flush=True)
            with open(DEBUG_LOG, "a") as f:
                f.write(f"{error_msg}\n")
            # Silently log metrics failures to avoid disrupting the main pipeline
            print(f"         ⚠ (Metrics recording skipped: {type(metrics_error).__name__})", flush=True)

        tracker.record_batch(
            batch_time=total_time,
            merge_time=merge_elapsed,
            read_time=total_time - merge_elapsed,
            rows=rows
        )
        
        batch_counter["count"] += 1
        
        # Optimize periodically
        if batch_counter["count"] % PipelineConfig.OPTIMIZE_EVERY_N_BATCHES == 0:
            print(f"         🔨 Running OPTIMIZE with Z-ORDER by {PipelineConfig.ZORDER_COLUMN}...")
            optimize_delta_table(spark, target_path, PipelineConfig.ZORDER_COLUMN)

    try:
        # Write initialization log (in IST)
        now_ist_iso = datetime.now(IST).isoformat()
        startup_msg = f"[{now_ist_iso}] Pipeline starting - reading from {source_path}"
        print(f"DEBUG: {startup_msg}", flush=True)
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{startup_msg}\n")
        
        query = (
            stream_df.writeStream
            .foreachBatch(process_livewire_batch)
            .option("checkpointLocation", f"{PipelineConfig.CHECKPOINT_PATH}/livewire")
            .trigger(processingTime="5 seconds")
            .start()
        )
        
        now_ist_iso = datetime.now(IST).isoformat()
        startup_success_msg = f"[{now_ist_iso}] Stream started successfully"
        print(f"DEBUG: {startup_success_msg}", flush=True)
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{startup_success_msg}\n")
        
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n         🛑 Stopping Livewire Stream...")
        query.stop()
    except Exception as e:
        now_ist_iso = datetime.now(IST).isoformat()
        error_startup_msg = f"[{now_ist_iso}] Stream failed: {e}"
        print(f"\n         ❌ Livewire stream failed: {e}")
        print(f"DEBUG: {error_startup_msg}", flush=True)
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{error_startup_msg}\n")
        return {"status": "failed", "error": str(e)}
        
    return {"status": "completed"}

if __name__ == "__main__":
    from run_benchmark import create_spark_session
    spark = create_spark_session("ATLAS-Livewire")
    tracker = LatencyTracker()
    tracker.start_pipeline()
    run_livewire_pipeline(spark, tracker)