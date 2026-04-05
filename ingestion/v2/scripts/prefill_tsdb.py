"""
prefill_tsdb.py — Production-Scale V3 Prefill (80k Scale Enabled)
──────────────────────────────────────────────────────────────────
Generates n-days of historical telemetry for 80,000 devices.
- Hot Path: Bulk load into TimescaleDB via COPY.
- Cold Path: Daily Mega-Compaction (one Parquet file per day) for MinIO.
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
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import psycopg2
import pandas as pd
from minio import Minio

# Adjust path for V2/V3 structure
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

READINGS_PER_HOUR = 12        
INTERVAL_SEC = 300            

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("prefill-v3")

# Load Registry (80k devices)
import json
REGISTRY_PATH = os.path.join(os.getcwd(), "device_configs.json")
with open(REGISTRY_PATH, "r") as f:
    DEVICES = list(json.load(f).items())

COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "server_name", "model", "processor_vendor", "server_generation",
    "report_type", "metric_type", "status", "error_reason", "tags",
    "location_id", "location_city", "location_state", "location_country", "location_name"
]

def generate_slot_rows(dt: datetime):
    n = len(DEVICES)
    ts_iso = dt.isoformat()
    
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
        dev_id, meta = DEVICES[i]
        rows.append([
            ts_iso, dev_id, meta["platform_customer_id"], meta["application_customer_id"],
            float(amb_temp[i]), float(avg_watts[i]), int(cpu_freq[i]),
            4200000, 250, int(cpu_util[i]), int(cpu_watts[i]), int(gpu_watts[i]),
            int(min_watts[i]), int(peak_watts[i]),
            meta["server_name"], meta["model"], meta["processor_vendor"], meta["server_generation"],
            "telemetry_live", "power_metrics", "t", "", "production,critical",
            meta["location_id"], meta["location_city"], meta["location_state"], 
            meta["location_country"], meta["location_name"]
        ])
    return rows

def push_to_tsdb(rows):
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

def process_day(d: int, days_total: int):
    now = datetime.now(timezone.utc)
    day_start = now - timedelta(days=d+1)
    date_str = day_start.strftime("%Y-%m-%d")
    log.info(f"📅 [prefill] Day {d+1}/{days_total} Start ({date_str})...")
    
    day_rows = []
    for s in range(READINGS_PER_HOUR * 24):
        slot_dt = day_start + timedelta(seconds=INTERVAL_SEC * s)
        slot_rows = generate_slot_rows(slot_dt)
        push_to_tsdb(slot_rows)
        day_rows.extend(slot_rows)
        if s % 12 == 11:
            log.info(f"  -> {date_str} hour {(s+1)//12}/{24} synced.")

    # Cold Path Compaction
    if day_rows:
        df = pd.DataFrame(day_rows, columns=COLUMNS)
        pq_buf = io.BytesIO()
        df.to_parquet(pq_buf, engine='pyarrow', index=False)
        client = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        if not client.bucket_exists(MINIO_BUCKET): client.make_bucket(MINIO_BUCKET)
        obj_name = f"date={date_str}/daily_compacted.parquet"
        client.put_object(MINIO_BUCKET, obj_name, data=io.BytesIO(pq_buf.getvalue()), length=len(pq_buf.getvalue()), content_type="application/octet-stream")
        log.info(f"✅ [cold-path] Day {date_str} Archived to MinIO.")

def run_prefill(days: int = 7, workers: int = 4):
    log.info(f"🚀 V3 HIGH-SCALE PREFILL - {days} Days | {len(DEVICES):,} Devices | {workers} Workers")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for d in range(days):
            executor.submit(process_day, d, days)

    log.info(f"🔥 Prefill Complete in {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--scale", type=int, default=80000)
    args = parser.parse_args()
    run_prefill(days=args.days, workers=args.workers)
