#!/usr/bin/env python3
"""
Generate dummy parquet data for /data/refined to test delta_loader.py
without needing the full Spark pipeline.

This creates parquet files with Hive partitioning that match the schema
expected by delta_loader.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

def generate_dummy_data(output_dir="/data/refined", num_devices=100, num_days=3):
    """
    Generate dummy telemetry data matching delta_loader.py schema.
    
    Creates Hive-partitioned parquet files:
        /data/refined/report_type=telemetry/partition_date=YYYY-MM-DD/.../device_id=xxx/
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"[*] Generating dummy data in {output_dir}")
    
    # Schema columns expected by ClickHouse (from delta_loader.py)
    schema = [
        "report_id", "device_id", "application_customer_id", "platform_customer_id",
        "status", "report_type", "error_reason", "MetricValue", "model", "tags",
        "location_state", "location_country", "processor_vendor", "server_generation",
        "location_id", "location_name", "location_city", "server_name", "metric_id",
        "cpu_inventory", "memory_inventory", "pcie_devices_count", "socket_count",
        "avg_metric_value", "max_metric_value", "min_metric_value", "metric_time",
        "datetime", "timeRangeEnd", "amb_temp", "Insertiontime", "co2_factor",
        "energy_cost_factor", "max_metric_time", "location_date", "inventory_date"
    ]
    
    # Generate data
    base_time = datetime.now(timezone.utc) - timedelta(days=num_days)
    rows = []
    
    for day_offset in range(num_days):
        current_date = base_time + timedelta(days=day_offset)
        partition_date = current_date.strftime("%Y-%m-%d")
        
        for device_idx in range(num_devices):
            device_id = f"SRV-{device_idx:06d}"
            pcid = f"PLATCUST{(device_idx % 5) + 1:04d}"
            acid = f"{pcid}_APPCUST{(device_idx % 2) + 1:02d}"
            
            metric_time = current_date + timedelta(hours=device_idx % 24)
            
            row = {
                "report_id": f"RPT-{device_idx}-{day_offset}",
                "device_id": device_id,
                "application_customer_id": acid,
                "platform_customer_id": pcid,
                "status": True,
                "report_type": "telemetry",
                "error_reason": "",
                "MetricValue": np.random.uniform(10, 100),
                "model": "PowerEdge R750",
                "tags": "production,critical",
                "location_state": "Karnataka",
                "location_country": "India",
                "processor_vendor": "Intel",
                "server_generation": "15G",
                "location_id": f"LOC-{(device_idx % 5) + 1:02d}",
                "location_name": "Atlas-DC-01",
                "location_city": "Bangalore",
                "server_name": f"srv-{device_idx:04d}.prod",
                "metric_id": f"MET-{device_idx}",
                "cpu_inventory": "2x Intel Xeon Gold 6230",
                "memory_inventory": "384GB RDIMM",
                "pcie_devices_count": 2,
                "socket_count": 2,
                "avg_metric_value": np.random.uniform(20, 80),
                "max_metric_value": np.random.uniform(80, 100),
                "min_metric_value": np.random.uniform(5, 20),
                "metric_time": metric_time,
                "datetime": metric_time,
                "timeRangeEnd": metric_time + timedelta(hours=1),
                "amb_temp": np.random.uniform(20, 28),
                "Insertiontime": datetime.now(timezone.utc),
                "co2_factor": 0.45,
                "energy_cost_factor": 8.5,
                "max_metric_time": metric_time,
                "location_date": partition_date,
                "inventory_date": partition_date,
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # Write with Hive partitioning
    # partition_date is required for delta_loader's partition pruning
    print(f"[*] Writing {len(df)} rows with Hive partitioning...")
    
    # Use pyarrow to write partitioned parquet
    import pyarrow.parquet as pq
    import pyarrow as pa
    
    table = pa.Table.from_pandas(df)
    pq.write_table(
        table,
        str(output_path),
        partition_cols=["report_type", "partition_date"],
        coerce_timestamps="us",
    )
    
    print(f"[✓] Generated dummy data:")
    print(f"    Rows: {len(df)}")
    print(f"    Devices: {df['device_id'].nunique()}")
    print(f"    Date range: {df['partition_date'].min()} to {df['partition_date'].max()}")
    print(f"    Output: {output_path}")
    
    # List generated files
    parquet_files = list(output_path.rglob("*.parquet"))
    print(f"    Files created: {len(parquet_files)}")
    for pf in parquet_files[:5]:
        print(f"      - {pf.relative_to(output_path)}")
    if len(parquet_files) > 5:
        print(f"      ... and {len(parquet_files) - 5} more")
    
    return len(df)


if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "/data/refined"
    num_devices = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    num_days = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    
    try:
        rows = generate_dummy_data(output_dir, num_devices, num_days)
        print(f"\n[SUCCESS] Dummy data generation complete!")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
