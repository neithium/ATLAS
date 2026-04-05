"""
prefill_tsdb.py — Production-Scale V2 Prefill (Hot/Cold Optimization)
──────────────────────────────────────────────────────────────────
Generates n-days of historical telemetry for all 50,000 devices.
- Hot Path: Bulk load into TimescaleDB via COPY.
- Cold Path: Daily Mega-Compaction (one Parquet file per day) for MinIO.

Schema: 28-column Fleet Production Schema
"""

import argparse
import csv
import io
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import psycopg2
import pandas as pd
from minio import Minio

# Adjust path to find core.config
V2_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(V2_ROOT))

# Configuration
TSDB_HOST = os.getenv("TSDB_HOST", "localhost")
TSDB_PORT = os.getenv("TSDB_PORT", "5432")
TSDB_USER = os.getenv("TSDB_USER", "postgres")
TSDB_PASS = os.getenv("TSDB_PASS", "postgres")
TSDB_NAME = os.getenv("TSDB_NAME", "postgres")

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "telemetry-raw")

READINGS_PER_HOUR = 12        # 5-minute intervals
INTERVAL_SEC = 300            # 5 minutes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("prefill-v2")

from core.config import load_device_registry
DEVICE_REGISTRY = load_device_registry()

COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "server_name", "model", "processor_vendor", "server_generation",
    "report_type", "metric_type", "status", "error_reason", "tags",
    "location_id", "location_city", "location_state", "location_country", "location_name"
]

def generate_slot_rows(dt: datetime):
    """Generate one 5-min slot of telemetry for 50k devices."""
    n = len(DEVICE_REGISTRY)
    ts_iso = dt.isoformat()
    
    # Simulation Logic
    cycle_factor = float(np.sin((dt.hour - 8) * np.pi / 12))
    cpu_util = np.clip(40 + (cycle_factor * 30) + np.random.uniform(-10, 10, n), 5, 95).astype(np.int32)
    cpu_watts = (200 + (cpu_util * 2.5) + np.random.uniform(-5, 5, n)).astype(np.int32)
    gpu_watts = (50 + (cycle_factor * 50) + np.random.uniform(-5, 5, n)).astype(np.int32)
    avg_watts = np.round((cpu_watts + gpu_watts) / 2.0, 2)
    min_watts = (avg_watts * 0.8).astype(np.int32)
    peak_watts = (avg_watts * 1.4).astype(np.int32)
    amb_temp = np.round(22.0 + (cycle_factor * 5.0) + np.random.uniform(-0.5, 0.5, n), 1)
    cpu_freq = ((2800 + (cycle_factor * 1000) + np.random.randint(-100, 100, n)) * 1000).astype(np.int64)

    rows = []
    for i in range(n):
        dev = DEVICE_REGISTRY[i]
        rows.append([
            ts_iso, dev[0], dev[1], dev[2],
            float(amb_temp[i]), float(avg_watts[i]), int(cpu_freq[i]),
            4200000, 250, int(cpu_util[i]), int(cpu_watts[i]), int(gpu_watts[i]),
            int(min_watts[i]), int(peak_watts[i]),
            dev[3], dev[4], dev[5], dev[6],
            "telemetry_live", "power_metrics", "t", "", "production,critical",
            dev[7], dev[8], dev[9], dev[10], dev[11]
        ])
    return rows

def push_to_tsdb(rows):
    """Hot Path push."""
    conn = psycopg2.connect(host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, dbname=TSDB_NAME)
    cur = conn.cursor()
    f = io.StringIO()
    writer = csv.writer(f, delimiter='\t')
    writer.writerows(rows)
    f.seek(0)
    copy_sql = f"COPY telemetry_live ({','.join(COLUMNS)}) FROM STDIN WITH DELIMITER E'\\t' NULL '' CSV"
    cur.copy_expert(sql=copy_sql, file=f)
    conn.commit()
    cur.close()
    conn.close()

def run_prefill(days: int = 7):
    now = datetime.now(timezone.utc)
    log.info("=" * 70)
    log.info(f"V2 DUAL PREFILL (HOT/COLD OPTIMIZED) - {days} Days")
    log.info("=" * 70)

    start_time = time.time()
    for d in range(days):
        day_start = now - timedelta(days=d+1)
        date_str = day_start.strftime("%Y-%m-%d")
        log.info(f"📅 [prefill] Processing Day {d+1}/{days} ({date_str})...")
        
        day_rows = []
        for s in range(READINGS_PER_HOUR * 24):
            slot_dt = day_start + timedelta(seconds=INTERVAL_SEC * s)
            slot_rows = generate_slot_rows(slot_dt)
            
            # 1. Hot Path: Push to TSDB immediately
            push_to_tsdb(slot_rows)
            # 2. Collect for Cold Path Compaction
            day_rows.extend(slot_rows)
            
            if s % 12 == 0:
                log.info(f"  -> Hour {s//12} completed...")

        # 3. Cold Path: Daily Mega-Compaction Upload
        if day_rows:
            log.info(f"❄️ [cold-path] Saving Day {date_str} to MinIO (Compacted)...")
            df = pd.DataFrame(day_rows, columns=COLUMNS)
            pq_buf = io.BytesIO()
            df.to_parquet(pq_buf, engine='pyarrow', index=False)
            
            client = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
            if not client.bucket_exists(MINIO_BUCKET):
                client.make_bucket(MINIO_BUCKET)
                
            obj_name = f"date={date_str}/daily_compacted.parquet"
            client.put_object(
                MINIO_BUCKET, obj_name, 
                data=io.BytesIO(pq_buf.getvalue()), length=len(pq_buf.getvalue()), 
                content_type="application/octet-stream"
            )
            log.info(f"✅ [cold-path] Day {date_str} Archived.")

    log.info(f"🔥 Prefill Complete in {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()
    run_prefill(days=args.days)
