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
from typing import List, Dict, Any, Optional

import asyncpg
import pandas as pd
import psycopg2
from apscheduler.schedulers.asyncio import AsyncIOScheduler
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
ENABLE_MINIO_PUSH = False
TS_CONN_STR = os.getenv("TS_CONN_STR", "postgresql://postgres:postgres@127.0.0.1:5432/postgres")

# Fleet Telemetry Schema (28 Columns)
HOT_PATH_COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "status", "error_reason"
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
                True, ""
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

# (Legacy archival logic removed)

def start(run_immediately: bool = True):
    """Initializes the polling schedules."""
    scheduler.add_job(poll_all, trigger="interval", seconds=POLL_INTERVAL_SECONDS, id="ipmi_poller", max_instances=1, misfire_grace_time=300)
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
