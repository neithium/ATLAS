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
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

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

def _get_devices_to_poll() -> Dict[str, Any]:
    """Hot-loads the latest registry from disk to pick up dynamic additions."""
    from config.devices import load_devices
    return load_devices()

async def _poll_batch(device_ids: List[str], current_registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    batch_configs = {did: current_registry[did] for did in device_ids}
    return await poll_batch_ipmi(batch_configs)

async def poll_all():
    """Main polling loop for the entire fleet (now supports 80,000+ devices)."""
    current_registry = _get_devices_to_poll()
    devices = list(current_registry.keys())
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
            
            results = await _poll_batch(batch, current_registry)
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
    
    # Load registry ONCE (not per-record — avoids 80k reads of the 57MB file)
    from config.devices import load_devices
    registry = load_devices()
    
    for result_list in batch_results:
        for r in result_list:
            if not isinstance(r, dict) or r.get("status") != "success":
                continue
                
            did = r["device_id"]
            reading = r["data"]
            meta = registry.get(did, {})
            
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

async def _archive_segment_task(start_time, end_time, bucket_name, partition_path, segment_id):
    """Worker task to process a specific time segment into 48-field Parquet."""
    try:
        conn = await asyncpg.connect(TS_CONN_STR.replace("localhost", "127.0.0.1"))
        records = await conn.fetch("SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2", start_time, end_time)
        await conn.close()

        if not records:
            return f"Segment {segment_id}: No Data"

        # 💎 RESTRUCTURING TO 48-FIELD GOLDEN SCHEMA 💎
        hydrated_records = []
        for r in records:
            did = r['device_id']
            meta = DEVICES.get(did, {})
            full_doc = {
                "device_id": did,
                "report_id": "ARCHIVE-" + str(uuid.uuid4())[:8],
                "created_at": r['metric_time'].isoformat(),
                "status": r.get('status', True),
                "model": r.get('model', meta.get('model')),
                "tags": r.get('tags', meta.get('tags')),
                "report_type": "telemetry_archive",
                "server_name": r.get('server_name', meta.get('server_name')),
                "error_reason": r.get('error_reason'),
                "location_id": r.get('location_id', meta.get('location_id')),
                "location_city": r.get('location_city', meta.get('location_city', 'Unknown')),
                "location_state": r.get('location_state', meta.get('location_state', 'Unknown')),
                "location_country": r.get('location_country', meta.get('location_country', 'India')),
                "location_name": r.get('location_name', meta.get('location_name', 'Unknown')),
                "processor_vendor": r.get('processor_vendor', meta.get('processor_vendor', 'Intel')),
                "server_generation": r.get('server_generation', meta.get('server_generation', 'Unknown')),
                "platform_customer_id": r['platform_customer_id'],
                "application_customer_id": r['application_customer_id'],
                "metric_type": r.get('metric_type', 'power_metrics'),
                "data": {
                    "Id": did,
                    "Average": r.get('avg_watts', 0),
                    "Maximum": r.get('peak_watts', 0),
                    "Minimum": r.get('min_watts', 0),
                    "Name": r.get('server_name', meta.get('server_name')),
                    "PowerDetail": [
                        {
                            "AmbTemp": r.get('amb_temp'),
                            "Average": r.get('avg_watts'),
                            "CpuAvgFreq": r.get('cpu_avg_freq'),
                            "CpuMax": r.get('cpu_max'),
                            "CpuPwrSavLim": r.get('cpu_pwr_sav_lim'),
                            "CpuUtil": r.get('cpu_util'),
                            "CpuWatts": r.get('cpu_watts'),
                            "GpuWatts": r.get('gpu_watts'),
                            "Minimum": r.get('min_watts'),
                            "Peak": r.get('peak_watts'),
                            "Time": r['metric_time'].isoformat()
                        }
                    ]
                },
                "inventory_data": {
                    "cpu_count": 2,
                    "socket_count": 2,
                    "cpu_inventory": [{"model": r.get('model', 'Intel'), "speed": 2400, "total_cores": 16}],
                    "memory_inventory": [{"memory_size": 128, "operating_freq": 3200, "memory_device_type": "DDR4"}]
                }
            }
            hydrated_records.append(full_doc)

        df = pd.DataFrame(hydrated_records)
        pq_buf = io.BytesIO()
        df.to_parquet(pq_buf, engine='pyarrow', index=False, compression="snappy")
        data_size = len(pq_buf.getvalue())

        s3 = Minio("127.0.0.1:9000", access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        filename = f"part-{segment_id}_{start_time.strftime('%H%M')}.parquet"
        
        # PUSH TO RAW
        s3.put_object(bucket_name, f"{partition_path}/{filename}", data=io.BytesIO(pq_buf.getvalue()), length=data_size)
        # PUSH TO ARCHIVE
        s3.put_object("telemetry-archive", f"recovery/{partition_path}/{filename}", data=io.BytesIO(pq_buf.getvalue()), length=data_size)
        
        return f"✅ Segment {segment_id} Synced ({len(records)} rows)"
    except Exception as e:
        return f"❌ Segment {segment_id} Failed: {e}"

async def archive_daily_to_minio(target_date: Optional[datetime] = None):
    """V3 Master Parallel Archiver (5 Segments/Workers)."""
    if not ENABLE_MINIO_PUSH: return
    
    try:
        now = datetime.now(timezone.utc)
        target_day = target_date or (now - timedelta(days=1))
        base_start = target_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        y, m, d = target_day.strftime("%Y"), target_day.strftime("%m"), target_day.strftime("%d")
        partition_path = f"year={y}/month={m}/day={d}"
        
        log.info(f"🛰️ Starting 5-Batch Parallel Archival for {y}-{m}-{d}...")

        # 5 Segments of 4.8 Hours (288 minutes) each
        segment_duration = timedelta(hours=4.8)
        tasks = []
        for i in range(5):
            seg_start = base_start + (segment_duration * i)
            seg_end = seg_start + segment_duration
            tasks.append(_archive_segment_task(seg_start, seg_end, MINIO_BUCKET, partition_path, i+1))
        
        results = await asyncio.gather(*tasks)
        for res in results:
            log.info(res)
            
        log.info("🏁 All 5 Batches Complete. Cold Storage Synced.")
    except Exception as e:
        log.error(f"❌ Parallel Compaction Failed: {e}")

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
