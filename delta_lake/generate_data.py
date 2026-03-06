"""
ATLAS Delta Lake Module - Test Data Generator
Generates FLATTENED Parquet files simulating pre-processed telemetry data for testing.

This simulates data that has ALREADY been processed by the ATLAS Processing Layer (Sanjula):
- Data is already exploded (one row per reading)
- Aggregates (min, max, avg) are pre-computed
- Data matches the project output_schema format (../schema/output_schema.py)

File 1 (Baseline): 2016 flattened rows (7 days of 5-min intervals)
File 2 (Overlap): 2016 flattened rows with 1728 overlapping + 288 new timestamps
"""

import os
import json
import random
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    IntegerType
)

# Output schema  
flattened_schema = StructType([
    StructField("report_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("application_customer_id", StringType(), True),
    StructField("platform_customer_id", StringType(), True),
    StructField("status", BooleanType(), True),
    StructField("report_type", StringType(), True),
    StructField("error_reason", StringType(), True),
    StructField("MetricValue", DoubleType(), True),
    StructField("model", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("location_state", StringType(), True),
    StructField("location_country", StringType(), True),
    StructField("processor_vendor", StringType(), True),
    StructField("server_generation", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("location_name", StringType(), True),
    StructField("location_city", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("metric_id", StringType(), True),
    StructField("cpu_inventory", StringType(), True),
    StructField("memory_inventory", StringType(), True),
    StructField("pcie_devices_count", IntegerType(), True),
    StructField("socket_count", IntegerType(), True),
    StructField("avg_metric_value", DoubleType(), True),
    StructField("max_metric_value", DoubleType(), True),
    StructField("min_metric_value", DoubleType(), True),
    StructField("metric_time", StringType(), True),
    StructField("datetime", DoubleType(), True),
    StructField("timeRangeEnd", DoubleType(), True),
    StructField("amb_temp", DoubleType(), True),
    StructField("Insertiontime", DoubleType(), True),
    StructField("co2_factor", DoubleType(), True),
    StructField("energy_cost_factor", DoubleType(), True),
    StructField("max_metric_time", StringType(), True),
    StructField("location_date", StringType(), True),
    StructField("inventory_date", StringType(), True)
])


def generate_flattened_rows(device_id: str, report_id: str, start_time: datetime, num_readings: int) -> list:
    """
    Generate FLATTENED telemetry rows (already processed/exploded format).
    Each row represents one 5-minute reading - no nested arrays.
    """
    rows = []
    current_time = start_time
    insertion_time = time.time()
    
    # Pre-compute aggregates for the batch (simulating upstream calculation)
    all_values = [round(random.uniform(150.0, 400.0), 2) for _ in range(num_readings)]
    avg_metric = round(sum(all_values) / len(all_values), 2)
    max_metric = max(all_values)
    min_metric = min(all_values)
    
    for i in range(num_readings):
        metric_time_str = current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        metric_timestamp = current_time.timestamp()
        
        row = {
            "report_id": report_id,
            "device_id": device_id,
            "application_customer_id": "APP-67890",
            "platform_customer_id": "PLAT-12345",
            "status": True,
            "report_type": "power_metrics",
            "error_reason": None,
            "MetricValue": all_values[i],
            "model": "PowerEdge R750",
            "tags": json.dumps({"environment": "production", "rack": "A1"}),
            "location_state": "Texas",
            "location_country": "USA",
            "processor_vendor": "Intel",
            "server_generation": "Gen15",
            "location_id": "LOC-001",
            "location_name": "Austin Data Center",
            "location_city": "Austin",
            "server_name": f"server-{device_id}",
            "metric_id": "power",
            "cpu_inventory": "2",
            "memory_inventory": "DDR4-64GB",
            "pcie_devices_count": 4,
            "socket_count": 2,
            "avg_metric_value": avg_metric,
            "max_metric_value": max_metric,
            "min_metric_value": min_metric,
            "metric_time": metric_time_str,
            "datetime": metric_timestamp,
            "timeRangeEnd": metric_timestamp + 300,  # 5 minutes later
            "amb_temp": round(random.uniform(20.0, 35.0), 2),
            "Insertiontime": insertion_time,
            "co2_factor": 0.5,
            "energy_cost_factor": 0.12,
            "max_metric_time": metric_time_str,
            "location_date": current_time.strftime("%Y-%m-%d"),
            "inventory_date": current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
        rows.append(row)
        current_time += timedelta(minutes=5)
    
    return rows


def main():
    print("=" * 60)
    print("Delta Lake MERGE Demo - Flattened Data Generator")
    print("=" * 60)
    print("\n📋 Generating PRE-PROCESSED (flattened) telemetry data")
    print("   This simulates data already transformed by upstream pipeline")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("FlattenedDataGenerator") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Create output directory
    output_dir = "/app/data/raw"
    os.makedirs(output_dir, exist_ok=True)
    
    # ========================================
    # FILE 1: Baseline (7 days = 2016 rows, already flattened)
    # ========================================
    print("\n[1/2] Generating File 1 (Baseline - Flattened)...")
    
    # Start time: 7 days ago
    baseline_start = datetime(2026, 2, 26, 0, 0, 0)
    
    # Generate 2016 FLATTENED rows (one row per reading)
    rows_file1 = generate_flattened_rows("SRV-101", "RPT-001", baseline_start, 2016)
    
    df1 = spark.createDataFrame(rows_file1, schema=flattened_schema)
    df1.write.mode("overwrite").parquet(f"{output_dir}/file1_baseline.parquet")
    
    print(f"   ✓ Created: {output_dir}/file1_baseline.parquet")
    print(f"   ✓ Device ID: SRV-101")
    print(f"   ✓ Flattened rows: {len(rows_file1)}")
    print(f"   ✓ Time range: {rows_file1[0]['metric_time']} to {rows_file1[-1]['metric_time']}")
    
    # ========================================
    # FILE 2: Overlap (1728 overlap + 288 new, already flattened)
    # ========================================
    print("\n[2/2] Generating File 2 (Overlap - Flattened)...")
    
    # File 2 starts 1 day after File 1 starts (6 days overlap)
    overlap_start = baseline_start + timedelta(days=1)
    
    # Generate 2016 FLATTENED rows
    rows_file2 = generate_flattened_rows("SRV-101", "RPT-002", overlap_start, 2016)
    
    df2 = spark.createDataFrame(rows_file2, schema=flattened_schema)
    df2.write.mode("overwrite").parquet(f"{output_dir}/file2_overlap.parquet")
    
    print(f"   ✓ Created: {output_dir}/file2_overlap.parquet")
    print(f"   ✓ Device ID: SRV-101")
    print(f"   ✓ Flattened rows: {len(rows_file2)}")
    print(f"   ✓ Time range: {rows_file2[0]['metric_time']} to {rows_file2[-1]['metric_time']}")
    
    # ========================================
    # VERIFICATION
    # ========================================
    print("\n" + "=" * 60)
    print("OVERLAP ANALYSIS")
    print("=" * 60)
    
    # Calculate overlapping timestamps
    file1_timestamps = set(r["metric_time"] for r in rows_file1)
    file2_timestamps = set(r["metric_time"] for r in rows_file2)
    
    overlap_count = len(file1_timestamps & file2_timestamps)
    unique_file1 = len(file1_timestamps - file2_timestamps)
    unique_file2 = len(file2_timestamps - file1_timestamps)
    
    print(f"   File 1 unique timestamps: {unique_file1} (Day 1 only)")
    print(f"   File 2 unique timestamps: {unique_file2} (Day 8 only)")
    print(f"   Overlapping timestamps:   {overlap_count} (Days 2-7)")
    print(f"\n   Expected after MERGE:")
    print(f"   - File 1 total:    {len(rows_file1)} rows")
    print(f"   - New from File 2: {unique_file2} rows")
    print(f"   - Final count:     {len(rows_file1) + unique_file2} rows")
    print(f"   - Duplicates dropped: {overlap_count}")
    
    # Show sample data structure
    print("\n" + "=" * 60)
    print("SAMPLE DATA STRUCTURE (First row)")
    print("=" * 60)
    sample = rows_file1[0]
    key_fields = ["device_id", "metric_time", "MetricValue", "amb_temp", 
                  "avg_metric_value", "max_metric_value", "min_metric_value"]
    for field in key_fields:
        print(f"   {field}: {sample[field]}")
    
    print("\n" + "=" * 60)
    print("Data generation complete!")
    print("Input files are ALREADY FLATTENED (no nested arrays)")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
