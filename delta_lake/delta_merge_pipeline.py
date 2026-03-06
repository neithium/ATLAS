"""
ATLAS Delta Lake Module - Deduplication Pipeline
Performs high-performance deduplication using Delta Lake MERGE on time-series telemetry data.

This module is part of the ATLAS (Advanced Telemetry Logging & Analytics System) pipeline.

Pipeline Steps:
1. Read pre-flattened Parquet files (from Processing Layer)
2. Initialize Delta Table with baseline data
3. Execute MERGE (upsert) with composite key (device_id, metric_time)
4. Verify deduplication and report timing

NOTE: Input data is ALREADY FLATTENED by upstream Spark processing.
      This pipeline only handles deduplication via Delta MERGE.
"""

import time
from pyspark.sql import SparkSession
from delta import DeltaTable


def create_spark_session():
    """Create SparkSession with Delta Lake support for ATLAS pipeline."""
    # Use explicit package version compatible with Spark 3.5.x
    spark = SparkSession.builder \
        .appName("DeltaLakeMergeDemo") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("\n" + "=" * 70)
    print("  ATLAS - DELTA LAKE DEDUPLICATION PIPELINE")
    print("  High-Performance Time-Series Telemetry Processing")
    print("=" * 70)
    print("\n  NOTE: Input data is PRE-FLATTENED (processed by Sanjula layer)")
    
     
    # STEP 1: INITIALIZE SPARK WITH DELTA
     
    print("\n[STEP 1] Initializing Spark session with Delta Lake...")
    spark = create_spark_session()
    
    # Paths
    raw_data_path = "/app/data/raw"
    refined_path = "/app/data/refined"
    
     
    # STEP 2: READ PRE-FLATTENED FILE 1 (BASELINE)
     
    print("[STEP 2] Reading File 1 (baseline - already flattened)...")
    
    df1_flattened = spark.read.parquet(f"{raw_data_path}/file1_baseline.parquet")
    
    file1_count = df1_flattened.count()
    print(f"         ✓ File 1 rows: {file1_count}")
    
     
    # STEP 3: CREATE BASELINE DELTA TABLE
     
    print("[STEP 3] Creating baseline Delta table at /refined...")
    
    df1_flattened.write \
        .format("delta") \
        .mode("overwrite") \
        .save(refined_path)
    
    print(f"         ✓ Delta table initialized with {file1_count} rows")
    
     
    # STEP 4: READ PRE-FLATTENED FILE 2 (OVERLAP)
     
    print("[STEP 4] Reading File 2 (overlap - already flattened)...")
    
    df2_flattened = spark.read.parquet(f"{raw_data_path}/file2_overlap.parquet")
    
    file2_count = df2_flattened.count()
    print(f"         ✓ File 2 rows: {file2_count}")
    
     
    # STEP 5: DELTA MERGE (DEDUPLICATION LOGIC)
     
    print("[STEP 5] Executing Delta MERGE (deduplication)...")
    print("         Using composite key: (device_id, metric_time)")
    print("\n  Starting pipeline timer...")
    start_time = time.perf_counter()
    start_time_ms = time.time() * 1000
    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, refined_path)
    
    # Execute MERGE - only insert non-matching records
    # Composite primary key: device_id + metric_time #application customer id 
    merge_result = delta_table.alias("target").merge(
        df2_flattened.alias("source"),
        "target.device_id = source.device_id AND target.metric_time = source.metric_time"
    ).whenNotMatchedInsertAll().execute()
    
    print("          MERGE operation completed")
    
     
    # TIMER END
     
    end_time = time.perf_counter()
    end_time_ms = time.time() * 1000
    
    elapsed_seconds = end_time - start_time
    elapsed_ms = end_time_ms - start_time_ms
    
     
    # STEP 6: VERIFICATION
     
    print("[STEP 6] Verifying deduplication results...")
    
    # Read final Delta table
    final_df = spark.read.format("delta").load(refined_path)
    final_count = final_df.count()
    
    # Calculate expected values
    expected_count = 2016 + 288  # baseline + new unique records
    duplicates_dropped = file1_count + file2_count - final_count
    
    print(f"\n" + "-" * 50)
    print("  DEDUPLICATION RESULTS")
    print("-" * 50)
    print(f"  File 1 (baseline) rows:    {file1_count}")
    print(f"  File 2 (overlap) rows:     {file2_count}")
    print(f"  Total input rows:          {file1_count + file2_count}")
    print(f"  Final Delta table rows:    {final_count}")
    print(f"  Duplicates dropped:        {duplicates_dropped}")
    print(f"  Expected final count:      {expected_count}")
    print("-" * 50)
    
    # Validation
    if final_count == expected_count:
        print("   SUCCESS: Deduplication verified!")
    else:
        print(f"  WARNING: Expected {expected_count}, got {final_count}")
    
  
    print(f"\n" + "=" * 70)
    print("  PIPELINE EXECUTION COMPLETE")
    print("=" * 70)
    print(f"  Total execution time: {elapsed_ms:.2f} ms ({elapsed_seconds:.3f} seconds)")
    print("=" * 70)
    
     


if __name__ == "__main__":
    main()
