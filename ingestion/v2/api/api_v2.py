"""
PowerPulse V3 Ingestion API: Hierarchical Outgress Streamer
──────────────────────────────────────────────────────────
Strategy: Demand-Based Kafka Ingestion
Hot Path: TimescaleDB (7-day history)
Streaming: Kafka (Redpanda)
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import aiokafka
import asyncpg
import orjson
from fastapi import FastAPI, BackgroundTasks, HTTPException

# ── Configuration ───────────────────────────────────────────────────────────
TSDB_HOST = os.getenv("TSDB_HOST", "127.0.0.1")
TSDB_PORT = os.getenv("TSDB_PORT", "5432")
TSDB_USER = os.getenv("TSDB_USER", "postgres")
TSDB_PASS = os.getenv("TSDB_PASS", "postgres")
TSDB_NAME = os.getenv("TSDB_NAME", "postgres")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry-export-v2")

# ── Global Shared Resources ────────────────────────────────────────────────
_kafka: Optional[aiokafka.AIOKafkaProducer] = None
_pool: Optional[asyncpg.Pool] = None

async def get_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME,
            min_size=10, max_size=30
        )
    return _pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-v3")

app = FastAPI(title="PowerPulse V3 Unified Ingestion API")

@app.on_event("startup")
async def startup_event():
    await get_kafka()
    await get_db_pool()
    log.info("🚀 [startup] Silo-Systems Online (Kafka + TSDB Pool)")

@app.on_event("shutdown")
async def shutdown_event():
    if _kafka: await _kafka.stop()
    if _pool: await _pool.close()

# ── Connections ─────────────────────────────────────────────────────────────
async def get_kafka():
    global _kafka
    if _kafka is None:
        # Dual-Silo Failover: Try Global Broker first, then Local Redpanda
        bootstrap_list = [KAFKA_BOOTSTRAP, "localhost:9092"]
        _kafka = aiokafka.AIOKafkaProducer(
            bootstrap_servers=bootstrap_list,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            compression_type="lz4",    # 🛰️ High-Throughput Compression
            linger_ms=10,             # 🛰️ Message Batching Window
            max_request_size=5242880, # 🚀 5MB Limit for 7-day bursts
            request_timeout_ms=15000,
            retry_backoff_ms=500
        )
        try:
            await _kafka.start()
            log.info(f"🛰️ Production Kafka Producer Active (LZ4 + 5MB + Batching)")
        except Exception as e:
            log.error(f"❌ Kafka Bootstrap Failed: {e}. Switching to LOCAL-ONLY mode.")
            _kafka = aiokafka.AIOKafkaProducer(
                bootstrap_servers="localhost:9092",
                compression_type="lz4",
                linger_ms=5,
                max_request_size=5242880,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
            )
            await _kafka.start()
    return _kafka

async def query_tsdb_range(device_id: str, start_time: datetime, end_time: datetime):
    await conn.close()
    return [dict(r) for r in records]

# ── Background Export Task ────────────────────────────────────────────────
async def _export_stream_task(device_ids: List[str], start_time: datetime, end_time: datetime):
    kafka_prod = await get_kafka()
    processed = 0
    sem = asyncio.Semaphore(15) 
    log.info(f"🚀 [export] Starting Parallel Golden-Schema Stream for {len(device_ids)} devices...")
    
    async def export_single_device(did):
        nonlocal processed
        async with sem:
            try:
                readings = await query_tsdb_range(did, start_time, end_time)
                if not readings: return
                
                # ── 1. Calculate Global Aggregates for the Envelope ──
                avg_val = sum(r.get('avg_watts', 0) for r in readings) / len(readings)
                max_val = max(r.get('peak_watts', 0) for r in readings)
                min_val = min(r.get('min_watts', 0) for r in readings)
                latest = readings[-1] # Base metadata
                
                # ── 2. Format as per Golden Spark Schema ──
                message = {
                    "device_id": did,
                    "report_id": str(uuid.uuid4()),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "status": latest.get('status', True),
                    "model": latest.get('model', 'Unknown'),
                    "tags": latest.get('tags', ''),
                    "report_type": latest.get('report_type', 'telemetry_live'),
                    "server_name": latest.get('server_name', did),
                    "error_reason": latest.get('error_reason'),
                    "location_id": latest.get('location_id'),
                    "location_name": latest.get('location_name'),
                    "location_city": latest.get('location_city'),
                    "location_state": latest.get('location_state'),
                    "location_country": latest.get('location_country'),
                    "processor_vendor": latest.get('processor_vendor'),
                    "server_generation": latest.get('server_generation'),
                    "platform_customer_id": latest.get('platform_customer_id'),
                    "application_customer_id": latest.get('application_customer_id'),
                    "metric_type": latest.get('metric_type'),
                    "data": {
                        "Id": did,
                        "Average": round(avg_val, 2),
                        "Maximum": float(max_val),
                        "Minimum": float(min_val),
                        "Name": latest.get('server_name', did),
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
                               "Time": r.get('metric_time').isoformat() if isinstance(r.get('metric_time'), datetime) else str(r.get('metric_time'))
                           } for r in readings
                        ]
                    },
                    "inventory_data": {
                        "cpu_count": 2,
                        "socket_count": 2,
                        "cpu_inventory": [
                            {"model": latest.get('model', 'Intel'), "speed": 2400, "total_cores": 16}
                        ],
                        "memory_inventory": [
                            {"memory_size": 128, "operating_freq": 3200, "memory_device_type": "DDR4"}
                        ]
                    }
                }
                
                # 🛰️ Zero-Loss confirmation
                await kafka_prod.send_and_wait(KAFKA_TOPIC, message)
                processed += 1
            except Exception as e:
                log.error(f"Export failed for {did}: {e}")

    await asyncio.gather(*(export_single_device(did) for did in device_ids))
    log.info(f"✅ [export] Golden-Schema Stream Complete: {processed} devices pushed to Kafka.")

async def _export_latest_task(device_ids: List[str], count: int = 2016):
    kafka_prod = await get_kafka()
    processed = 0
    sem = asyncio.Semaphore(15) 
    log.info(f"🚀 [export-latest] Starting Parallel Latest-Batch Stream for {len(device_ids)} devices...")
    
    async def export_single_device(did):
        nonlocal processed
        async with sem:
            try:
                readings = await query_tsdb_latest(did, limit=count)
                if not readings: return
                
                # ── 1. Calculate Global Aggregates for the Envelope ──
                avg_val = sum(r.get('avg_watts', 0) for r in readings) / len(readings)
                max_val = max(r.get('peak_watts', 0) for r in readings)
                min_val = min(r.get('min_watts', 0) for r in readings)
                latest = readings[-1]
                
                # ── 2. Format as per Golden Spark Schema ──
                message = {
                    "device_id": did,
                    "report_id": str(uuid.uuid4()),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "status": latest.get('status', True),
                    "model": latest.get('model', 'Unknown'),
                    "tags": latest.get('tags', ''),
                    "report_type": latest.get('report_type', 'telemetry_live'),
                    "server_name": latest.get('server_name', did),
                    "error_reason": latest.get('error_reason'),
                    "location_id": latest.get('location_id'),
                    "location_name": latest.get('location_name'),
                    "location_city": latest.get('location_city'),
                    "location_state": latest.get('location_state'),
                    "location_country": latest.get('location_country'),
                    "processor_vendor": latest.get('processor_vendor'),
                    "server_generation": latest.get('server_generation'),
                    "platform_customer_id": latest.get('platform_customer_id'),
                    "application_customer_id": latest.get('application_customer_id'),
                    "metric_type": latest.get('metric_type'),
                    "data": {
                        "Id": did,
                        "Average": round(avg_val, 2),
                        "Maximum": float(max_val),
                        "Minimum": float(min_val),
                        "Name": latest.get('server_name', did),
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
                               "Time": r.get('metric_time').isoformat() if isinstance(r.get('metric_time'), datetime) else str(r.get('metric_time'))
                           } for r in readings
                        ]
                    },
                    "inventory_data": {
                        "cpu_count": 2,
                        "socket_count": 2,
                        "cpu_inventory": [
                            {"model": latest.get('model', 'Intel'), "speed": 2400, "total_cores": 16}
                        ],
                        "memory_inventory": [
                            {"memory_size": 128, "operating_freq": 3200, "memory_device_type": "DDR4"}
                        ]
                    }
                }
                await kafka_prod.send_and_wait(KAFKA_TOPIC, message)
                processed += 1
            except Exception as e:
                log.error(f"Export-Latest failed for {did}: {e}")

    await asyncio.gather(*(export_single_device(did) for did in device_ids))
    log.info(f"✅ [export-latest] Latest-Batch Stream Complete: {processed} devices pushed to Kafka.")

# ── Hierarchical Endpoints ────────────────────────────────────────────────
@app.get("/pcid/{pcid}/acid/{acid}/telemetry")
async def trigger_customer_telemetry_export(pcid: str, acid: str, background_tasks: BackgroundTasks, days: int = 7):
    """Hierarchical Fetch: Triggers Kafka Ingestion for ALL devices in a PCID/ACID path."""
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        # 1. High-Scale Discovery: Use the local metadata registry instead of scanning 161M rows
        registry_path = "/app/device_configs.json"
        if not os.path.exists(registry_path):
             return {"status": "Registry file missing", "acid": acid}
             
        with open(registry_path, "rb") as f:
            registry = orjson.loads(f.read())
        
        device_ids = [did for did, meta in registry.items() 
                      if meta["platform_customer_id"] == pcid and meta["application_customer_id"] == acid]
        
        if not device_ids:
            return {"status": "No devices found", "pcid": pcid, "acid": acid}
            
        # 2. Trigger asynchronous Kafka Export for all fields
        background_tasks.add_task(_export_stream_task, device_ids, start_time, end_time)
        
        return {
            "status": "Kafka ingestion is started",
            "pcid": pcid,
            "acid": acid,
            "targeted_devices": len(device_ids),
            "window_days": days
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pcid/{pcid}/acid/{acid}/telemetry/latest")
async def trigger_latest_telemetry_export(pcid: str, acid: str, background_tasks: BackgroundTasks, count: int = 2016):
    """Latest-Batch Fetch: Triggers Kafka Ingestion for EXACTLY N latest points."""
    try:
        registry_path = "/app/device_configs.json"
        if not os.path.exists(registry_path):
             return {"status": "Registry file missing", "acid": acid}
             
        with open(registry_path, "rb") as f:
            registry = orjson.loads(f.read())
        
        device_ids = [did for did, meta in registry.items() 
                      if meta["platform_customer_id"] == pcid and meta["application_customer_id"] == acid]
        
        if not device_ids:
            return {"status": "No devices found", "pcid": pcid, "acid": acid}
            
        background_tasks.add_task(_export_latest_task, device_ids, count)
        
        return {
            "status": "Latest-Batch ingestion is started",
            "pcid": pcid,
            "acid": acid,
            "targeted_devices": len(device_ids),
            "requested_count": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pcid/{pcid}/acid/{acid}/devices")
async def list_customer_devices(pcid: str, acid: str):
    """Discovery: List latest snapshots for a PCID/ACID."""
    try:
        conn = await asyncpg.connect(host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME)
        records = await conn.fetch("SELECT DISTINCT ON (device_id) device_id, status, avg_watts FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 ORDER BY device_id, metric_time DESC", pcid, acid)
        await conn.close()
        return {"pcid": pcid, "acid": acid, "devices": [dict(r) for r in records]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pcid/{pcid}/acid/{acid}/export")
async def trigger_full_hierarchy_export(pcid: str, acid: str, background_tasks: BackgroundTasks, days: int = 7):
    """Bulk Export ALL in hierarchy."""
    conn = await asyncpg.connect(host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME)
    records = await conn.fetch("SELECT DISTINCT device_id FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2", pcid, acid)
    await conn.close()
    ids = [r["device_id"] for r in records]
    if not ids: return {"status": "Empty Hierarchy"}
    
    end = datetime.now(timezone.utc)
    background_tasks.add_task(_export_stream_task, ids, end - timedelta(days=days), end)
    return {"status": "Kafka Ingestion Started", "count": len(ids)}

@app.post("/pcid/{pcid}/acid/{acid}/id/{device_ids}/export")
async def trigger_targeted_path_export(pcid: str, acid: str, device_ids: str, background_tasks: BackgroundTasks, days: int = 7):
    """Bulk Export SPECIFIC IDs in path."""
    target_ids = device_ids.split(",")
    conn = await asyncpg.connect(host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME)
    records = await conn.fetch("SELECT DISTINCT device_id FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 AND device_id = ANY($3)", pcid, acid, target_ids)
    await conn.close()
    ids = [r["device_id"] for r in records]
    if not ids: return {"status": "No matching devices found in this hierarchy"}
    
    end = datetime.now(timezone.utc)
    background_tasks.add_task(_export_stream_task, ids, end - timedelta(days=days), end)
    return {"status": "Kafka Ingestion Started", "targeted_devices": ids, "count": len(ids)}

# ── General Endpoints ─────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "version": "v3-fixed"}

@app.get("/devices/{device_id}")
async def get_device_history(device_id: str, limit: int = 10):
    try:
        conn = await asyncpg.connect(host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME)
        records = await conn.fetch("SELECT * FROM telemetry_live WHERE device_id = $1 ORDER BY metric_time DESC LIMIT $2", device_id, limit)
        await conn.close()
        return [dict(r) for r in records]
    except Exception as e:
        return {"error": str(e)}

async def query_tsdb_range(device_id: str, start_time: datetime, end_time: datetime):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch("SELECT * FROM telemetry_live WHERE device_id = $1 AND metric_time >= $2 AND metric_time <= $3 ORDER BY metric_time ASC", device_id, start_time, end_time)
        return [dict(r) for r in records]

async def query_tsdb_latest(device_id: str, limit: int = 2016):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch("SELECT * FROM telemetry_live WHERE device_id = $1 ORDER BY metric_time DESC LIMIT $2", device_id, limit)
        return [dict(r) for r in reversed(records)]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
