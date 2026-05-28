import time
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, to_timestamp, current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
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

def run_livewire_pipeline(
    spark: SparkSession,
    tracker: LatencyTracker,
    source_path: str = "/app/data/processed",
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
    
    # Use readStream for continuous folder ingestion.
    # The schema is inferred from the Parquet files, making it schema-agnostic.
    
    # Check if source directory has any parquet files
    has_data = False
    try:
        for root, dirs, files in os.walk(source_path):
            if any(f.endswith('.parquet') for f in files):
                has_data = True
                break
    except Exception:
        pass
    
    if not has_data:
        print(f"\n        No parquet files found in {source_path}")
        print(f"          Waiting for processor to generate data from Kafka...")
        print(f"          If using test mode, enable RUN_GENERATOR=y to pre-populate test data")
        print(f"          Will retry schema inference when data arrives...\n")
    
    try:
        # Enable Spark streaming schema inference
        spark.conf.set("spark.sql.streaming.schemaInference", "true")
        
        # Recursive reading from both /app/data/processed/stream/ and /app/data/processed/batch/
        # Pattern matches: stream/worker_1/*.parquet, batch/*.parquet, etc.
        stream_df = spark.readStream.parquet(f"{source_path}")
    except Exception as e:
        error_msg = str(e)
        if "Unable to infer schema" in error_msg or "Path does not exist" in error_msg:
            print(f"\n           Cannot initialize streaming: {error_msg}")
            print(f"\n           TROUBLESHOOTING:")
            print(f"            1. Check if Kafka has telemetry data:")
            print(f"               docker exec atlas-ingestion curl -s http://localhost:8001/health | jq .")
            print(f"            2. Check processor logs:")
            print(f"               docker logs atlas-processor | grep -i 'worker\\|batch'")
            print(f"            3. Enable test data generation:")
            print(f"               RUN_GENERATOR=y RUN_PIPELINE=y docker compose up atlas-lakehouse")
            print(f"            4. Check that docker volumes exist:")
            print(f"               docker volume ls | grep delta")
            return {"status": "failed", "reason": "no_input_data"}
        else:
            print(f"         ⚠ Error initializing stream: {e}")
            return {"status": "failed", "reason": str(e)}

    # Track batches
    batch_counter = {"count": 0}
    
    def process_livewire_batch(batch_df: DataFrame, batch_id: int):
        start_time = time.perf_counter()
        rows = batch_df.count()
        if rows == 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏳ Waiting for stream data (Batch {batch_id}) - no new files detected.", flush=True)
            return
            
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚡ Processing Micro-batch {batch_id} with {rows} rows...", flush=True)
        
        # Schema alignment mapping streaming and batch outputs (schema-agnostic)
        from pyspark.sql.functions import coalesce, to_timestamp, current_timestamp
        
        # Make the metric_time derivation schema-agnostic
        if "window_start" in batch_df.columns:
            time_col = col("window_start")
        elif "event_date" in batch_df.columns:
            time_col = to_timestamp(col("event_date"))
        else:
            time_col = current_timestamp()
            
        aligned_df = (
            batch_df
            .withColumn("metric_time", time_col)
            .withColumn("application_customer_id", lit("livewire_unknown"))
            .withColumn("platform_customer_id", lit("livewire_unknown"))
            .withColumn("report_type", lit("livewire_raw"))
        )
        
        # Drop staging columns only if they exist
        drop_cols = [c for c in ["window_start", "window_end", "event_date"] if c in aligned_df.columns]
        if drop_cols:
            aligned_df = aligned_df.drop(*drop_cols)
        
        prepared_df = prepare_partition_columns(aligned_df)
        hashed_df = generate_composite_hash(prepared_df)
        
        # Execute MERGE deduplication
        merge_start = time.perf_counter()
        merge_results = execute_merge_deduplication(spark, target_path, hashed_df)
        merge_elapsed = time.perf_counter() - merge_start
        
        total_time = time.perf_counter() - start_time
        
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
        query = (
            stream_df.writeStream
            .foreachBatch(process_livewire_batch)
            .option("checkpointLocation", f"{PipelineConfig.CHECKPOINT_PATH}/livewire")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n         🛑 Stopping Livewire Stream...")
        query.stop()
    except Exception as e:
        print(f"\n         ❌ Livewire stream failed: {e}")
        return {"status": "failed", "error": str(e)}
        
    return {"status": "completed"}

if __name__ == "__main__":
    from run_benchmark import create_spark_session
    spark = create_spark_session("ATLAS-Livewire")
    tracker = LatencyTracker()
    tracker.start_pipeline()
    run_livewire_pipeline(spark, tracker)