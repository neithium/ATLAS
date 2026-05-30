import os
import time
import shutil
from typing import List
from delta_core import (
    PipelineConfig,
    LatencyTracker,
    CheckpointManager,
    prepare_partition_columns,
    delta_table_exists,
    initialize_delta_table,
    execute_merge_deduplication,
    optimize_delta_table
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session(app_name: str = "ATLAS-RefinedLayer-DeltaMerge-Benchmark") -> SparkSession:
    """
    Create SparkSession for local execution
    """
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Delta Lake core extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        # Schema evolution
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Zstd compression
        .config("spark.sql.parquet.compression.codec", PipelineConfig.COMPRESSION_CODEC)
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        # Disable Dynamic Allocation for consistent benchmark
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.dynamicAllocation.enabled", "false")
        # Delta optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "false")
        .config("spark.databricks.delta.properties.defaults.targetFileSize", str(PipelineConfig.TARGET_FILE_SIZE_MB * 1024 * 1024))
        .config("spark.sql.shuffle.partitions", str(PipelineConfig.SPARK_SHUFFLE_PARTITIONS))
        # Local Profile (Default): Vertical Monolith inside Lakehouse container
        .master("local[*]")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "6")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
    )
    
    if os.getenv("SPARK_MASTER"):
        # Override local if explicit master passed via env var
        builder = builder.master(os.getenv("SPARK_MASTER"))
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_file_dates(spark: SparkSession, data_path: str) -> List[str]:
    """Discover available file_date partitions from benchmark data."""
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
    benchmark_path = f"{PipelineConfig.RAW_DATA_PATH}/benchmark_data"
    
    print("\n" + "-" * 80)
    print("[STEP 2] Discovering file_date batches in benchmark data...")
    print("-" * 80)
    
    file_dates = get_file_dates(spark, benchmark_path)
    
    if not file_dates:
        print("         ✗ No file_date partitions found in benchmark data!")
        return {"error": "no_data"}
    
    print(f"         ✓ Found {len(file_dates)} daily file batches")
    
    state = checkpoint_mgr.load_state() if resume else {"completed_batches": [], "last_batch": None, "total_rows_processed": 0}
    completed = set(state.get("completed_batches", []))
    
    file_dates_to_process = [d for d in file_dates if d not in completed]
    
    if not file_dates_to_process:
        print("         ✓ All file batches already processed!")
        return {"total_rows": state.get("total_rows_processed", 0)}
        
    total_rows_processed = state.get("total_rows_processed", 0)

    if resume:
        table_initialized = delta_table_exists(spark, PipelineConfig.REFINED_PATH)
    else:
        table_initialized = False
        if os.path.exists(PipelineConfig.REFINED_PATH):
            shutil.rmtree(PipelineConfig.REFINED_PATH, ignore_errors=True)

    optimize_counter = 0                            
    
    print("\n" + "-" * 80)
    print("[STEP 3] Processing file batches with incremental MERGE...")
    print("-" * 80)
    
    for idx, file_date in enumerate(file_dates_to_process, start=1):
        batch_start = time.perf_counter()
        
        print(f"\n  ┌─ Batch {idx}/{len(file_dates_to_process)}: file_date={file_date}", flush=True)
        
        read_start = time.perf_counter()
        batch_df = (
            spark.read.parquet(benchmark_path)
            .filter(col("file_date") == file_date)
            .drop("file_date")
        )
        prepared_df = prepare_partition_columns(batch_df)
        row_count = prepared_df.count()
        read_elapsed = time.perf_counter() - read_start
        
        print(f"  │  Rows: {row_count:,} | Read: {read_elapsed:.2f}s", flush=True)
        
        merge_elapsed = 0
        if not table_initialized:
            init_start = time.perf_counter()
            initialize_delta_table(
                spark=spark,
                df=prepared_df,
                path=PipelineConfig.REFINED_PATH,
                partition_cols=PipelineConfig.PARTITION_COLUMNS
            )
            merge_elapsed = time.perf_counter() - init_start
            table_initialized = True
            print(f"  │  Action: INIT | Time: {merge_elapsed:.2f}s", flush=True)
        else:
            print(f"  │  Merging day i against accumulated days 0..i-1...", flush=True)
            merge_start = time.perf_counter()
            execute_merge_deduplication(
                spark=spark,
                target_path=PipelineConfig.REFINED_PATH,
                source_df=prepared_df
            )
            merge_elapsed = time.perf_counter() - merge_start
            print(f"  │  MERGE Time: {merge_elapsed:.2f}s", flush=True)
        
        batch_elapsed = time.perf_counter() - batch_start
        throughput = row_count / batch_elapsed if batch_elapsed > 0 else 0
        
        print(f"  │  Batch total: {batch_elapsed:.2f}s | Throughput: {throughput:,.0f} rows/s", flush=True)
        
        tracker.record_batch(batch_elapsed, merge_elapsed, read_elapsed, row_count)
        total_rows_processed += row_count
        
        if PipelineConfig.ENABLE_CHECKPOINTING:
            state = checkpoint_mgr.mark_batch_complete(file_date, row_count, state)
        
        optimize_counter += 1
        # Deprecated: Replaced by Delta Auto-Compaction
        # if optimize_counter >= PipelineConfig.OPTIMIZE_EVERY_N_BATCHES:
        #     print(f"  │  Running OPTIMIZE...", flush=True)
        #     opt_start = time.perf_counter()
        #     optimize_delta_table(spark, PipelineConfig.REFINED_PATH, PipelineConfig.ZORDER_COLUMN)
        #     print(f"  │  OPTIMIZE completed in {time.perf_counter() - opt_start:.2f}s", flush=True)
        #     optimize_counter = 0
        
        print(f"  └─ ✓ Batch complete", flush=True)
    
    
    ##Disable final optimize for faster benchmark runs, can be enabled for more realistic performance numbers on larger datasets
    #if optimize_counter > 0:
    #    print("\n  Running final OPTIMIZE...")
    #    optimize_delta_table(spark, PipelineConfig.REFINED_PATH, PipelineConfig.ZORDER_COLUMN)
        
    return {
        "total_rows": total_rows_processed,
        "num_days": len(file_dates_to_process),
        "all_file_dates": file_dates_to_process
    }

if __name__ == "__main__":
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description="ATLAS Benchmark Pipeline")
    parser.add_argument("--generate-data", action="store_true", help="Generate benchmark data before running pipeline")
    parser.add_argument("--devices", type=int, default=1000, help="Number of devices for data generation")
    parser.add_argument("--days", type=int, default=4, help="Number of daily batches to generate")
    parser.add_argument("--batch-size", type=int, default=1000, help="Device batch size")
    args = parser.parse_args()

    spark = create_spark_session()
    
    if args.generate_data:
        from generate_data import generate_benchmark_data
        print(f"\n[INIT] Generating benchmark data for {args.devices} devices over {args.days} days...")
        # Since it runs in container, /raw is the correct output root
        generate_benchmark_data(
            spark=spark,
            output_root=PipelineConfig.RAW_DATA_PATH,
            total_devices=args.devices,
            batch_size=args.batch_size,
            start_date=datetime(2026, 2, 20),
            num_days=args.days
        )
    
    tracker = LatencyTracker()
    tracker.start_pipeline()
    checkpoint_mgr = CheckpointManager(PipelineConfig.CHECKPOINT_PATH)
    
    result = run_benchmark_pipeline(spark, tracker, checkpoint_mgr, resume=False)
    print(result)
    spark.stop()