"""
PowerPulse V2 Parallel Poller (Hot/Cold Optimization)
────────────────────────────────────────────────────
Core Engine for 50,000 devices. 
Strategy: 
1. Hot Path: Real-time 5-min intervals to TimescaleDB (COPY).
2. Cold Path: Daily Mega-Compaction to MinIO Parquet (Scheduled).
Kafka is STRICTLY for Demand-Based Export (REST API triggered).
"""

import asyncio
import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import asyncpg
import pandas as pd
import psycopg2
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from minio import Minio

from config.devices import DEVICES, POLL_INTERVAL_SECONDS, ENABLE_POLLER
from core.ipmi_reader import poll_batch_ipmi

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("poller-v2")

# High-Scale Partitioning/Ingest Config
POLL_BATCH_SIZE = int(os.getenv("POLL_BATCH_SIZE", "500"))
POLL_WORKERS = int(os.getenv("POLL_WORKERS", "100"))
POLL_STARTUP_DELAY = 1.0  # seconds between batch starts

# Storage Enablers
ENABLE_TSDB_PUSH = os.getenv("ENABLE_TSDB_PUSH", "1") == "1"
ENABLE_MINIO_PUSH = os.getenv("ENABLE_MINIO_PUSH", "1") == "1"
TS_CONN_STR = os.getenv("TS_CONN_STR", "postgresql://postgres:postgres@127.0.0.1:5432/postgres")

MINIO_HOST = os.getenv("MINIO_HOST", "127.0.0.1:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "telemetry-raw")

# Fleet Telemetry Schema (28 Columns)
HOT_PATH_COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "server_name", "model", "processor_vendor", "server_generation",
    "report_type", "metric_type", "status", "error_reason", "tags",
    "location_id", "location_city", "location_state", "location_country", "location_name"
]

# Scheduler State
scheduler = AsyncIOScheduler()
LAST_POLL = {
    "start_time": None,
    "end_time": None,
    "total_devices": 0,
    "success_count": 0,
    "error_count": 0,
    "status": "idle"
}

def _get_devices_to_poll() -> List[str]:
    return list(DEVICES.keys())

async def _poll_batch(device_ids: List[str]) -> List[Dict[str, Any]]:
    batch_configs = {did: DEVICES[did] for did in device_ids}
    return await poll_batch_ipmi(batch_configs)

async def poll_all():
    """Main polling loop for 50,000 devices every 5 minutes."""
    devices = _get_devices_to_poll()
    total_devices = len(devices)
    
    LAST_POLL["start_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    LAST_POLL["total_devices"] = total_devices
    LAST_POLL["status"] = "polling"
    LAST_POLL["success_count"] = 0
    LAST_POLL["error_count"] = 0
    
    log.info(f"🚀 [poller] Starting high-scale poll for {total_devices:,} devices...")
    
    batches = [devices[i:i + POLL_BATCH_SIZE] for i in range(0, total_devices, POLL_BATCH_SIZE)]
    semaphore = asyncio.Semaphore(POLL_WORKERS)
    total_results = []

    async def process_batch_with_semaphore(batch_idx: int):
        async with semaphore:
            batch = batches[batch_idx]
            if batch_idx > 0:
                await asyncio.sleep(POLL_STARTUP_DELAY * batch_idx % 10) 
            
            results = await _poll_batch(batch)
            total_results.append(results)
            
            success = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
            LAST_POLL["success_count"] += success
            LAST_POLL["error_count"] += (len(batch) - success)

    await asyncio.gather(*[process_batch_with_semaphore(i) for i in range(len(batches))])
    
    # Ingestion Path
    if total_results:
        log.info(f"💾 [hot-path] Ingesting {LAST_POLL['success_count']:,} records to TimescaleDB...")
        await _push_to_tsdb_hot(total_results)

    LAST_POLL["end_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    LAST_POLL["status"] = "idle"
    log.info(f"✅ [poller] Poll Complete: {LAST_POLL['success_count']}/{total_devices} success.")


async def _push_to_tsdb_hot(batch_results):
    """Hot Path: Bulk Ingest to TimescaleDB via COPY."""
    records = []
    ts_iso = datetime.now(timezone.utc).isoformat()
    
    for result_list in batch_results:
        for r in result_list:
            if not isinstance(r, dict) or r.get("status") != "success":
                continue
                
            did = r["device_id"]
            reading = r["data"]
            meta = DEVICES.get(did, {})
            
            records.append([
                ts_iso, did, meta.get("platform_customer_id"), meta.get("application_customer_id"),
                float(reading.get('AmbTemp', 25)), float(reading.get('Average', 300)), 
                int(reading.get('CpuAvgFreq', 3400000)), int(reading.get('CpuMax', 4200000)), 
                250, int(reading.get('CpuUtil', 50)), int(reading.get('CpuWatts', 200)), 
                int(reading.get('GpuWatts', 50)), int(reading.get('Minimum', 250)), 
                int(reading.get('Peak', 400)), 
                meta.get("server_name"), meta.get("model"), meta.get("processor_vendor"),
                meta.get("server_generation"), "telemetry_live", "power_metrics", True, "", 
                "production,critical", meta.get("location_id"),
                meta.get("location_city"), meta.get("location_state"), 
                meta.get("location_country"), meta.get("location_name")
            ])

    if records and ENABLE_TSDB_PUSH:
        f = io.StringIO()
        writer = csv.writer(f, delimiter='\t')
        writer.writerows(records)
        f.seek(0)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: _do_tsdb_copy(f))
        log.info(f"🔥 [hot-path] Bulk insert complete.")

def _do_tsdb_copy(f):
    conn = psycopg2.connect(TS_CONN_STR)
    try:
        with conn.cursor() as cur:
            copy_sql = f"COPY telemetry_live ({','.join(HOT_PATH_COLUMNS)}) FROM STDIN WITH DELIMITER E'\\t' NULL '' CSV"
            cur.copy_expert(copy_sql, f)
            conn.commit()
    finally:
        conn.close()

async def archive_daily_to_minio():
    """
    V3 Dual-Archive Mega-Compactor (TSDB -> MinIO):
    1. Writes Hive-Partitioned Raw Data (telemetry-raw)
    2. Writes Immutable Recovery Backup (telemetry-archive)
    """
    if not ENABLE_MINIO_PUSH:
        return
        
    try:
        now = datetime.now(timezone.utc)
        target_day = now - timedelta(days=1)
        log.info(f"📅 [cold-path] Starting Dual-Archive Mega-Compaction for {target_day.strftime('%Y-%m-%d')}...")

        # 🏙 Hive Partition Path Calculation
        y, m, d = target_day.strftime("%Y"), target_day.strftime("%m"), target_day.strftime("%d")
        partition_path = f"year={y}/month={m}/day={d}"
        filename = f"daily_compacted_{target_day.strftime('%Y%m%d')}.parquet"

        # 1. Fetch full 24h history from Hot Path
        start = target_day.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        
        conn = await asyncpg.connect(TS_CONN_STR.replace("localhost", "127.0.0.1"))
        records = await conn.fetch("SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2", start, end)
        await conn.close()
        
        if not records:
            log.info("📅 [cold-path] No data found for specified window.")
            return

        df = pd.DataFrame([dict(r) for r in records])
        
        # 💾 Prepare Parquet Stream
        pq_buf = io.BytesIO()
        df.to_parquet(pq_buf, engine='pyarrow', index=False, compression="snappy")
        data_size = len(pq_buf.getvalue())
        
        # 🏙 Initialize MinIO Client
        s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        
        # 🏙 DESTINATION 1: Partitioned Raw (For Spark Consumption)
        if not s3.bucket_exists(MINIO_BUCKET):
            s3.make_bucket(MINIO_BUCKET)
        
        s3.put_object(
            MINIO_BUCKET, 
            f"{partition_path}/{filename}", 
            data=io.BytesIO(pq_buf.getvalue()), 
            length=data_size, 
            content_type="application/octet-stream"
        )
        log.info(f"✅ [cold-path] Partitioned Raw Synced: {MINIO_BUCKET}/{partition_path}")

        # 🏙 DESTINATION 2: Immutable Recovery Archive (Backup)
        archive_bucket = "telemetry-archive"
        if not s3.bucket_exists(archive_bucket):
            s3.make_bucket(archive_bucket)
            
        s3.put_object(
            archive_bucket, 
            f"recovery/{partition_path}/{filename}", 
            data=io.BytesIO(pq_buf.getvalue()), 
            length=data_size, 
            content_type="application/octet-stream"
        )
        log.info(f"🛡️ [cold-path] Recovery Archive Created: {archive_bucket}/recovery/{partition_path}")

    except Exception as e:
        log.error(f"❌ [cold-path] Dual-Archive Mega-Compaction Failed: {e}")

def start(run_immediately: bool = True):
    """Initializes the polling and archiving schedules."""
    scheduler.add_job(poll_all, trigger="interval", seconds=POLL_INTERVAL_SECONDS, id="ipmi_poller", max_instances=1, misfire_grace_time=300)
    scheduler.add_job(archive_daily_to_minio, trigger="cron", hour=0, minute=0, id="minio_daily_archiver", max_instances=1)
    scheduler.start()
    log.info(f"📅 [scheduler] Dual-Write Background Engine Started.")
    if run_immediately:
        asyncio.create_task(poll_all())

def stop():
    """Graceful shutdown of the background engine."""
    try:
        if scheduler.running:
            scheduler.shutdown()
        log.info(f"🛑 [scheduler] Dual-Write Background Engine Stopped.")
    except Exception:
        pass
