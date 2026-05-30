import time
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, to_timestamp, current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType
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

# Define metrics schema at module level to avoid scoping issues
METRICS_SCHEMA = StructType([
    StructField("batch_id", LongType()),
    StructField("timestamp", TimestampType()),
    StructField("total_time", DoubleType()),
    StructField("merge_time", DoubleType()),
    StructField("row_count", LongType())
])

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
        # create a dummy dataframe matching the expected schema to init the table
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
        dummy_schema = StructType([
            StructField("device_id", StringType()),
            StructField("metric_time", TimestampType()),
            StructField("application_customer_id", StringType()),
            StructField("report_type", StringType()),
            StructField("platform_customer_id", StringType()),
            StructField("avg_cpu", DoubleType()),
            StructField("avg_mem", DoubleType()),
            StructField("num_records", LongType()),
            StructField("partition_date", StringType())
        ])
        df_dummy = spark.createDataFrame([], dummy_schema)
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
    # The schema is inferred from the Parquet files, making it schema-agnostic.
    try:
        # Enable Spark streaming schema inference
        spark.conf.set("spark.sql.streaming.schemaInference", "true")
        
        # Recursive reading from /stream_raw allows parsing both /batch and /stream partitions
        stream_df = spark.readStream.parquet(f"{source_path}/*/*.parquet")
    except Exception as e:
        print(f"         ⚠ Could not initialize stream from {source_path}: {e}")
        return {"status": "failed"}

    # Track batches
    batch_counter = {"count": 0}
    
    def process_livewire_batch(batch_df: DataFrame, batch_id: int):
        print(f"DEBUG: process_livewire_batch called with batch_id={batch_id}", flush=True)
        
        start_time = time.perf_counter()
        rows = batch_df.count()
        
        # Log to both file and stdout for debugging
        log_msg = f"[{datetime.now().isoformat()}] Batch {batch_id}: rows={rows}"
        print(f"DEBUG: {log_msg}", flush=True)  # This will appear in container logs
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{log_msg}\n")
        
        if rows == 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏳ Waiting for stream data (Batch {batch_id}) - no new files detected.", flush=True)
            return
            
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚡ Processing Micro-batch {batch_id} with {rows} rows...", flush=True)
        
        # --- Schema-Defensive Programming ---
        # Inspect batch schema to handle upstream drift gracefully
        batch_cols = batch_df.columns
        
        # 1. Dynamically resolve the timestamp column
        if "window_start" in batch_cols:
            timestamp_col = coalesce(col("window_start"), current_timestamp())
        elif "event_date" in batch_cols:
            timestamp_col = coalesce(to_timestamp(col("event_date")), current_timestamp())
        elif "created_at" in batch_cols: # Future-proofing for other possible names
            timestamp_col = coalesce(to_timestamp(col("created_at")), current_timestamp())
        else:
            timestamp_col = current_timestamp()

        # 2. Build the transformation with the resolved column
        aligned_df = (
            batch_df
            .withColumn("metric_time", timestamp_col)
            .withColumn("application_customer_id", lit("livewire_unknown"))
            .withColumn("platform_customer_id", lit("livewire_unknown"))
            .withColumn("report_type", lit("livewire_raw"))
        )

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
            metrics_data = [(
                int(batch_id),
                datetime.now(),
                float(total_time),
                float(merge_elapsed),
                int(rows)
            )]
            
            metrics_df = spark.createDataFrame(metrics_data, METRICS_SCHEMA)
            
            # Append to system metrics table (silent, non-blocking)
            metrics_df.write.format("delta").mode("append").save("/refined/system_metrics")
            
            # Debug log
            metrics_log_msg = f"[{datetime.now().isoformat()}] Batch {batch_id}: Metrics written - total_time={total_time:.3f}s, merge_time={merge_elapsed:.3f}s, rows={rows}"
            print(f"DEBUG: {metrics_log_msg}", flush=True)
            with open(DEBUG_LOG, "a") as f:
                f.write(f"{metrics_log_msg}\n")
        except Exception as metrics_error:
            # Debug log the error
            error_msg = f"[{datetime.now().isoformat()}] Batch {batch_id}: Metrics write FAILED - {type(metrics_error).__name__}: {str(metrics_error)[:100]}"
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
        # Write initialization log
        startup_msg = f"[{datetime.now().isoformat()}] Pipeline starting - reading from {source_path}"
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
        
        startup_success_msg = f"[{datetime.now().isoformat()}] Stream started successfully"
        print(f"DEBUG: {startup_success_msg}", flush=True)
        with open(DEBUG_LOG, "a") as f:
            f.write(f"{startup_success_msg}\n")
        
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n         🛑 Stopping Livewire Stream...")
        query.stop()
    except Exception as e:
        error_startup_msg = f"[{datetime.now().isoformat()}] Stream failed: {e}"
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