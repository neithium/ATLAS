"""
ATLAS Test Data Generator — Creates sample Parquet files matching output_schema.py
Writes to a local directory with the 5-level Hive partition structure that
the Delta Loader expects:
    report_type / partition_date / platform_customer_id /
    application_customer_id / device_id
"""

import os
import sys
import json
import uuid
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEVICE_COUNT = 50          # Number of unique devices
READINGS_PER_DEVICE = 24   # 24 × 5-min readings = 2 hours of data
OUTPUT_DIR = os.getenv("TEST_DATA_DIR", str(Path(__file__).parent.parent.parent / "data" / "test_refined"))

# Realistic-ish value pools
PLATFORMS = ["hpe_prod_01", "hpe_prod_02", "dell_staging_03"]
APPLICATIONS = ["acct_alpha_99", "acct_beta_42", "acct_gamma_17"]
LOCATIONS = [
    {"id": "LOC-BLR-01", "name": "Bangalore DC-1", "city": "Bangalore", "state": "Karnataka", "country": "India"},
    {"id": "LOC-MUM-02", "name": "Mumbai DC-2",    "city": "Mumbai",    "state": "Maharashtra", "country": "India"},
    {"id": "LOC-HYD-03", "name": "Hyderabad DC-3", "city": "Hyderabad", "state": "Telangana",   "country": "India"},
]
MODELS = ["ProLiant DL380 Gen10", "ProLiant DL360 Gen11", "PowerEdge R750"]
VENDORS = ["Intel", "AMD"]
GENERATIONS = ["Gen10", "Gen11", "Gen12"]
REPORT_TYPES = ["PowerMetrics", "ThermalMetrics"]
METRIC_IDS = ["CpuWatts", "AmbTemp", "GpuWatts"]


def generate_test_data():
    """Generate test Parquet files with realistic telemetry data."""
    output_path = Path(OUTPUT_DIR)

    # Clean previous test data
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) + timedelta(days=1)
    rows = []

    print(f"Generating test data: {DEVICE_COUNT} devices × {READINGS_PER_DEVICE} readings × {len(METRIC_IDS)} metrics")
    print(f"Output: {output_path}")

    for dev_idx in range(DEVICE_COUNT):
        device_id = f"device-{dev_idx:04d}"
        pcid = random.choice(PLATFORMS)
        acid = random.choice(APPLICATIONS)
        loc = random.choice(LOCATIONS)
        model = random.choice(MODELS)
        vendor = random.choice(VENDORS)
        gen = random.choice(GENERATIONS)
        report_type = random.choice(REPORT_TYPES)

        for reading_idx in range(READINGS_PER_DEVICE):
            metric_time = base_time + timedelta(minutes=5 * reading_idx)

            for metric_id in METRIC_IDS:
                base_value = {"CpuWatts": 150.0, "AmbTemp": 24.0, "GpuWatts": 80.0}[metric_id]
                value = base_value + random.gauss(0, base_value * 0.1)

                rows.append({
                    "report_id": str(uuid.uuid4()),
                    "device_id": device_id,
                    "application_customer_id": acid,
                    "platform_customer_id": pcid,
                    "status": random.choice([True, False]),
                    "report_type": report_type,
                    "error_reason": None if random.random() > 0.05 else "Sensor timeout",
                    "MetricValue": round(value, 2),
                    "model": model,
                    "tags": json.dumps({"env": "test", "gen": gen}),
                    "location_state": loc["state"],
                    "location_country": loc["country"],
                    "processor_vendor": vendor,
                    "server_generation": gen,
                    "location_id": loc["id"],
                    "location_name": loc["name"],
                    "location_city": loc["city"],
                    "server_name": f"srv-{device_id}",
                    "metric_id": metric_id,
                    "cpu_inventory": json.dumps([{"model": "Xeon 8380", "speed": 2300, "total_cores": 40}]),
                    "memory_inventory": json.dumps([{"memory_size": 64, "operating_freq": 3200, "memory_device_type": "DDR5"}]),
                    "pcie_devices_count": random.randint(2, 8),
                    "socket_count": random.choice([1, 2, 4]),
                    "avg_metric_value": round(value * 0.98, 2),
                    "max_metric_value": round(value * 1.15, 2),
                    "min_metric_value": round(value * 0.85, 2),
                    "metric_time": metric_time.isoformat() + "Z",
                    "datetime": metric_time.timestamp(),
                    "timeRangeEnd": (metric_time + timedelta(minutes=5)).timestamp(),
                    "amb_temp": round(24.0 + random.gauss(0, 2), 1),
                    "Insertiontime": datetime.utcnow().timestamp(),
                    "co2_factor": 0.42,
                    "energy_cost_factor": 0.12,
                    "max_metric_time": metric_time.isoformat() + "Z",
                    "location_date": metric_time.strftime("%Y-%m-%d"),
                    "inventory_date": metric_time.strftime("%Y-%m-%d"),
                    # Partition columns (used for Hive directory structure)
                    "partition_date": metric_time.strftime("%Y-%m-%d"),
                })

    df = pd.DataFrame(rows)
    total_rows = len(df)
    print(f"Generated {total_rows:,} rows")

    # Write with Hive partitioning: report_type / partition_date
    # (We write a simpler 2-level partition for testing — the loader
    #  handles both deep and shallow partition layouts)
    partition_cols = ["report_type", "partition_date"]

    table = pa.Table.from_pandas(df.drop(columns=partition_cols))

    # Group by partitions and write separate parquet files
    files_written = 0
    for (rtype, pdate), group in df.groupby(partition_cols):
        part_dir = output_path / f"report_type={rtype}" / f"partition_date={pdate}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = part_dir / f"part-{files_written:05d}.parquet"

        # Drop partition columns from data (they're encoded in the path)
        write_df = group.drop(columns=partition_cols)
        pq.write_table(pa.Table.from_pandas(write_df), str(part_file))
        files_written += 1

    print(f"Wrote {files_written} parquet file(s) to {output_path}")
    print(f"Partition structure: report_type=X/partition_date=YYYY-MM-DD/")

    # Verify
    parquet_files = list(output_path.rglob("*.parquet"))
    total_size = sum(f.stat().st_size for f in parquet_files)
    print(f"Total size: {total_size / 1024:.1f} KB across {len(parquet_files)} file(s)")
    print("Done!")
    return str(output_path)


if __name__ == "__main__":
    generate_test_data()
