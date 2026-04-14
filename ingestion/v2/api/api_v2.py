"""
# =============================================================================
# ATLAS - PowerPulse V3 Ingestion API
# =============================================================================
# Strategy: Demand-Based Kafka Ingestion
# Hot Path: TimescaleDB (7-day history)
# Streaming: Kafka (Redpanda)
# Automation: Hourly Multi-Silo Archival (MinIO)
# =============================================================================
"""

import asyncio
import json
import logging
import os
import time
import uuid
import io
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import aiokafka
import asyncpg
import orjson
from fastapi import FastAPI, BackgroundTasks, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from minio import Minio

# =============================================================================
# INFRASTRUCTURE CONFIGURATION
# =============================================================================
TSDB_HOST = os.getenv("TSDB_HOST", "127.0.0.1")
TSDB_PORT = os.getenv("TSDB_PORT", "5432")
TSDB_USER = os.getenv("TSDB_USER", "postgres")
TSDB_PASS = os.getenv("TSDB_PASS", "postgres")
TSDB_NAME = os.getenv("TSDB_NAME", "postgres")
TS_CONN_STR = f"postgresql://{TSDB_USER}:{TSDB_PASS}@{TSDB_HOST}:{TSDB_PORT}/{TSDB_NAME}"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-server-metrics")

MINIO_HOST = os.getenv("MINIO_HOST", "127.0.0.1:9000").replace("ingestion:", "127.0.0.1:")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")

REGISTRY_PATH = "/app/device_configs.json"

# =============================================================================
# GLOBAL RESOURCE POOLS
# =============================================================================
_kafka: Optional[aiokafka.AIOKafkaProducer] = None
_pool: Optional[asyncpg.Pool] = None
_scheduler = AsyncIOScheduler()

# System Guard: Prevents thread exhaustion under heavy burst loads
GLOBAL_EXPORT_SEM = asyncio.Semaphore(50)  # Reverted to 50 (100 was overloading I/O)

async def get_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME,
            min_size=10, max_size=50
        )
    return _pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-v3")

app = FastAPI(title="PowerPulse V3 Unified Ingestion API")

# =============================================================================
# 48-FIELD GOLDEN SCHEMA BUILDER (Matches Spark input_schema)
# =============================================================================
def _build_full_record(r, did: str, meta: dict) -> dict:
    """Hydrates a single DB row into the complete 48-field schema."""
    return {
        "device_id": did,
        "report_id": "RPT-" + str(uuid.uuid4())[:8],
        "created_at": r[0].isoformat() if hasattr(r[0], 'isoformat') else str(r[0]),
        "status": r[20],
        "model": r[15],
        "tags": r[22],
        "report_type": r[18],
        "server_name": r[14],
        "error_reason": r[21],
        "location_id": r[23],
        "location_city": r[24],
        "location_state": r[25],
        "location_country": r[26],
        "location_name": r[27],
        "processor_vendor": r[16],
        "server_generation": r[17],
        "platform_customer_id": r[2],
        "application_customer_id": r[3],
        "metric_type": r[19],
        "data": {
            "Id": did,
            "Average": float(r[5] or 0),
            "Maximum": float(r[13] or 0),
            "Minimum": float(r[12] or 0),
            "Name": r[14],
            "PowerDetail": [
                {
                    "AmbTemp": float(r[4] or 25),
                    "Average": float(r[5] or 0),
                    "CpuAvgFreq": int(r[6] or 3400000),
                    "CpuMax": int(r[7] or 4200000),
                    "CpuPwrSavLim": int(r[8] or 250),
                    "CpuUtil": int(r[9] or 50),
                    "CpuWatts": int(r[10] or 200),
                    "GpuWatts": int(r[11] or 50),
                    "Minimum": int(r[12] or 250),
                    "Peak": int(r[13] or 400),
                    "Time": r[0].isoformat()
                }
            ]
        },
        "inventory_data": {
            "cpu_count": 2,
            "socket_count": 2,
            "cpu_inventory": [{"model": r[15], "speed": 2400, "total_cores": 16}],
            "memory_inventory": [{"memory_size": 128, "operating_freq": 3200, "memory_device_type": "DDR4"}]
        }
    }

# =============================================================================
# AUTOMATED HOURLY ARCHIVAL JOB
# =============================================================================
async def hourly_archival_job():
    """Scheduled Task: Flushes the last 60 minutes of TSDB data to MinIO Parquet."""
    now = datetime.now(timezone.utc)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=60)
    
    log.info(f"🕰️ [SCHEDULER] Triggering 60-Min Production Migration: {start.strftime('%H:%M')} to {end.strftime('%H:%M')}...")
    
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            device_rows = await conn.fetch(
                "SELECT DISTINCT device_id FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2", 
                start, end
            )
            
            if not device_rows:
                log.info("ℹ️ [SCHEDULER] No data found for this period. Skipping archival.")
                return

            with open(REGISTRY_PATH, "rb") as f:
                DEVICES = orjson.loads(f.read())

            device_ids = [r[0] for r in device_rows]
            # Use 9000 for internal API access to bypass port 80 proxy
            s3 = Minio("127.0.0.1:9000", access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
            
            BATCH_SIZE = 100
            total_bytes = 0
            
            for i in range(0, len(device_ids), BATCH_SIZE):
                batch = device_ids[i:i + BATCH_SIZE]
                records = await conn.fetch(
                    "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
                    start, end, batch
                )
                
                if not records: continue
                
                hydrated = []
                for r in records:
                    hydrated.append(_build_full_record(r, r[1], DEVICES))

                df = pd.DataFrame(hydrated)
                pq_buf = io.BytesIO()
                df.to_parquet(pq_buf, engine='pyarrow', index=False, compression='snappy')
                content = pq_buf.getvalue()
                total_bytes += len(content)

                path = f"production/year={start.year}/month={start.month:02d}/day={start.day:02d}/hour={start.hour:02d}/"
                fname = f"auto_batch_{i//BATCH_SIZE}.parquet"
                
                s3.put_object("telemetry-raw", path + fname, data=io.BytesIO(content), length=len(content))
                s3.put_object("telemetry-archive", path + fname, data=io.BytesIO(content), length=len(content))

        print("\n" + "█" * 60)
        print(f"🚀 [SIGNAL] 10-MIN TEST ARCHIVE COMPLETED | {start.strftime('%H:%M')} - {end.strftime('%H:%M')}")
        print(f"📦 SILOS: telemetry-raw & telemetry-archive")
        print(f"📊 VOLUME: {len(device_ids)} devices | {total_bytes/1024:.1f} KB")
        print("█" * 60 + "\n")
        
    except Exception as e:
        log.error(f"💥 [SCHEDULER] Archival Failed: {str(e)}")

# =============================================================================
# LIFECYCLE MANAGEMENT
# =============================================================================
@app.on_event("startup")
async def startup_event():
    await get_kafka()
    await get_db_pool()
    
    # Staggered Scheduler: Runs at :03 to avoid poller overlap at :00/:05
    _scheduler.add_job(hourly_archival_job, 'cron', minute=3)
    _scheduler.start()
    
    log.info("🚀 [SYSTEM] Silo-Systems Online (Archival Scheduler ACTIVE - Staggered :03)")

@app.on_event("shutdown")
async def shutdown_event():
    _scheduler.shutdown()
    if _kafka: await _kafka.stop()
    if _pool: await _pool.close()

# =============================================================================
# CONNECTION FACTORIES
# =============================================================================
async def get_kafka():
    global _kafka
    if _kafka is None:
        # Dual-Silo Failover: Try Production Broker first, fall back to Local
        bootstrap_list = [KAFKA_BOOTSTRAP, "localhost:9092"]
        _kafka = aiokafka.AIOKafkaProducer(
            bootstrap_servers=bootstrap_list,
            value_serializer=lambda v: orjson.dumps(v),
            compression_type="lz4",    # 🛰️ Optimizing for High-Throughput
            linger_ms=50,             # 🛰️ Batch optimization
            max_request_size=8388608, # 🚀 8MB limit for 100k device bursts
            request_timeout_ms=30000,
            retry_backoff_ms=500
        )
        try:
            await _kafka.start()
            log.info(f"🛰️ [KAFKA] Production Producer Active (LZ4 Enabled)")
        except Exception as e:
            log.error(f"❌ [KAFKA] Bootstrap Failed: {e}. Switching to LOCAL-ONLY.")
            _kafka = aiokafka.AIOKafkaProducer(
                bootstrap_servers="localhost:9092",
                compression_type="lz4",
                linger_ms=50,
                max_request_size=8388608,
                value_serializer=lambda v: orjson.dumps(v)
            )
            await _kafka.start()
    return _kafka

# =============================================================================
# DATABASE DATA ACCESS LAYER (DAL)
# =============================================================================
async def query_tsdb_range(device_id: str, start_time: datetime, end_time: datetime):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT * FROM telemetry_live WHERE device_id = $1 AND metric_time >= $2 AND metric_time < $3 ORDER BY metric_time ASC",
            device_id, start_time, end_time
        )
        return [dict(r) for r in records]

async def query_tsdb_latest(device_id: str, limit: int = 2016):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Optimized for speed: Ordering ASC in SQL removes Python reversed() overhead
        return await conn.fetch(
            "SELECT * FROM telemetry_live WHERE device_id = $1 ORDER BY metric_time ASC LIMIT $2",
            device_id, limit
        )

# =============================================================================
# BACKGROUND ASYNC WORKERS (Bulk Query Architecture)
# =============================================================================
BULK_BATCH_SIZE = 100  # Devices per DB query

def _build_power_detail(r):
    """Builds a single PowerDetail entry from a DB row."""
    return {
        "AmbTemp": float(r.get('amb_temp', 25)),
        "Average": float(r.get('avg_watts', 0)),
        "CpuAvgFreq": int(r.get('cpu_avg_freq', 3400000)),
        "CpuMax": int(r.get('cpu_max', 4200000)),
        "CpuPwrSavLim": int(r.get('cpu_pwr_sav_lim', 250)),
        "CpuUtil": int(r.get('cpu_util', 50)),
        "CpuWatts": int(r.get('cpu_watts', 200)),
        "GpuWatts": int(r.get('gpu_watts', 50)),
        "Minimum": int(r.get('min_watts', 250)),
        "Peak": int(r.get('peak_watts', 400)),
        "Time": r['metric_time'].isoformat() if hasattr(r.get('metric_time'), 'isoformat') else str(r.get('metric_time', ''))
    }

async def _export_stream_task(device_ids: List[str], start_time: datetime, end_time: datetime):
    """Heavyworker: High-Speed Batch SQL + 48-field hydration + batch Kafka push."""
    kafka_prod = await get_kafka()
    processed = 0
    log.info(f"🚀 [WORKER] Batch-Streaming {len(device_ids)} devices for stream export...")
    
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    
    pool = await get_db_pool()
    batch_size = 100
    
    for i in range(0, len(device_ids), batch_size):
        batch_ids = device_ids[i:i + batch_size]
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM telemetry_live WHERE device_id = ANY($1) AND metric_time >= $2 AND metric_time < $3 ORDER BY device_id, metric_time ASC",
                batch_ids, start_time, end_time
            )
        
        if not rows: continue
        
        # Fast grouping by device_id
        current_did = None
        device_readings = []
        for r in rows:
            did = r[1]
            if current_did is None: current_did = did
            if did != current_did:
                try:
                    await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
                    processed += 1
                except Exception as e:
                    log.error(f"❌ Stream device {current_did} failed: {e}")
                current_did = did
                device_readings = []
            device_readings.append(r)
        
        if current_did and device_readings:
            try:
                await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
                processed += 1
            except Exception as e:
                log.error(f"❌ Stream final device {current_did} failed: {e}")
    
    await kafka_prod.flush()
    log.info(f"✅ [WORKER] Bulk Stream Complete: {processed} devices processed.")

async def _export_latest_task(device_ids: List[str], count: int = 2016):
    """Latest Batch Task: High-Speed 23s Batch Engine."""
    kafka_prod = await get_kafka()
    processed = 0
    start_time = datetime.now(timezone.utc) - timedelta(days=8)
    log.info(f"🚀 [WORKER] Batch-Streaming {len(device_ids)} devices (Latest 7d)...")
    
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    
    pool = await get_db_pool()
    batch_size = 100
    for i in range(0, len(device_ids), batch_size):
        batch_ids = device_ids[i:i + batch_size]
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM telemetry_live WHERE device_id = ANY($1) AND metric_time >= $2 ORDER BY device_id, metric_time ASC",
                batch_ids, start_time
            )
        if not rows: continue
        current_did, device_readings = None, []
        for r in rows:
            did = r[1]
            if current_did is None: current_did = did
            if did != current_did:
                await _process_and_send(current_did, device_readings[-count:], DEVICES, kafka_prod)
                processed += 1
                current_did, device_readings = did, []
            device_readings.append(r)
        if current_did:
            await _process_and_send(current_did, device_readings[-count:], DEVICES, kafka_prod)
            processed += 1
    await kafka_prod.flush()
    log.info(f"✅ [WORKER] Latest-Batch Complete: {processed} devices processed.")

async def _export_first_task(device_ids: List[str], count: int = 2016):
    """Historical Task: Parallel Batch Engine (The Fastest Possible Strategy)."""
    kafka_prod = await get_kafka()
    processed = 0
    log.info(f"📜 [WORKER] Parallel-Batch Fetching OLDEST {count} points for {len(device_ids)} devices...")
    
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    
    pool = await get_db_pool()
    batch_size = 100
    
    async def process_batch(batch_ids):
        nonlocal processed
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT t.* FROM UNNEST($1::text[]) AS d(id)
                CROSS JOIN LATERAL (
                    SELECT * FROM telemetry_live 
                    WHERE device_id = d.id 
                    ORDER BY metric_time ASC 
                    LIMIT $2
                ) AS t
                """,
                batch_ids, count
            )
        
        if not rows: return
        
        current_did, device_readings = None, []
        for r in rows:
            did = r[1]
            if current_did is None: current_did = did
            if did != current_did:
                await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
                processed += 1
                current_did, device_readings = did, []
            device_readings.append(r)
        
        if current_did:
            await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
            processed += 1

    # Launch all batches concurrently
    batches = [device_ids[i:i + batch_size] for i in range(0, len(device_ids), batch_size)]
    await asyncio.gather(*(process_batch(b) for b in batches))
    
    await kafka_prod.flush()
    log.info(f"✅ [WORKER] Historical-First Complete: {processed} devices processed.")

async def _process_and_send(did, readings, DEVICES, kafka_prod):
    """Helper for fast single-device processing."""
    meta = DEVICES.get(did, {})
    latest = readings[-1]
    
    total_watts = 0
    max_watts = -1.0
    min_watts = 1e9
    pd_list = []
    
    for r in readings:
        w_avg, w_min, w_max = r[5] or 0.0, r[12] or 0.0, r[13] or 0.0
        total_watts += w_avg
        if w_max > max_watts: max_watts = w_max
        if w_min < min_watts: min_watts = w_min
        pd_list.append({
            "AmbTemp": float(r[4] or 25), "Average": float(w_avg),
            "CpuAvgFreq": int(r[6] or 3400000), "CpuMax": int(r[7] or 4200000),
            "CpuPwrSavLim": int(r[8] or 250), "CpuUtil": int(r[9] or 50),
            "CpuWatts": int(r[10] or 200), "GpuWatts": int(r[11] or 50),
            "Minimum": int(w_min), "Peak": int(w_max), "Time": r[0].isoformat()
        })

    message = _build_full_record(latest, did, meta)
    message["data"].update({
        "Average": round(total_watts / len(readings), 2),
        "Maximum": float(max_watts), "Minimum": float(min_watts),
        "PowerDetail": pd_list
    })
    await kafka_prod.send(KAFKA_TOPIC, message)

# =============================================================================
# HIERARCHICAL API ENDPOINTS
# =============================================================================
@app.get("/pcid/{pcid}/acid/{acid}/telemetry")
async def trigger_customer_telemetry_export(pcid: str, acid: str, background_tasks: BackgroundTasks, days: int = 7):
    """Triggers Kafka Ingestion for ALL devices in a PCID/ACID hierarchy."""
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        # Discovery via Registry (High Scale Optimization)
        registry_path = "/app/device_configs.json"
        with open(registry_path, "rb") as f:
            registry = orjson.loads(f.read())
        
        device_ids = [did for did, meta in registry.items() 
                      if meta["platform_customer_id"] == pcid and meta["application_customer_id"] == acid]
        
        if not device_ids:
            return {"status": "Empty Hierarchy", "pcid": pcid, "acid": acid}
            
        background_tasks.add_task(_export_stream_task, device_ids, start_time, end_time)
        return {"status": "Archival Stream Started", "targeted_devices": len(device_ids)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pcid/{pcid}/acid/{acid}/telemetry/latest")
async def trigger_latest_telemetry_export(pcid: str, acid: str, background_tasks: BackgroundTasks, count: int = 2016):
    """Latest-Batch Fetch: Triggers Kafka Ingestion for EXACTLY N latest points (Sync Mode)."""
    try:
        registry_path = "/app/device_configs.json"
        with open(registry_path, "rb") as f:
            registry = orjson.loads(f.read())
        
        device_ids = [did for did, meta in registry.items() 
                      if meta["platform_customer_id"] == pcid and meta["application_customer_id"] == acid]
        
        if not device_ids:
            return {"status": "Empty Hierarchy"}
            
        background_tasks.add_task(_export_latest_task, device_ids, count)
        return {"status": "Latest Sync Started", "requested_points": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pcid/{pcid}/acid/{acid}/id/{device_string}/export")
async def trigger_manual_id_export(pcid: str, acid: str, device_string: str, background_tasks: BackgroundTasks, days: int = 7):
    """Specific ID Export: Targets a comma-separated list of Device IDs."""
    try:
        device_ids = [d.strip() for d in device_string.split(",")]
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)

        log.info(f"📥 [API] Manual Export Requested for {len(device_ids)} specific devices.")
        
        background_tasks.add_task(_export_stream_task, device_ids, start_time, end_time)
        return {
            "status": "Manual Stream Started", 
            "requested_devices": len(device_ids),
            "pcid": pcid,
            "acid": acid
        }
    except Exception as e:
        log.error(f"❌ Export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pcid/{pcid}/acid/{acid}/telemetry/historical/first")
async def trigger_historical_first_export(pcid: str, acid: str, background_tasks: BackgroundTasks, count: int = 2016):
    """Triggers export of the OLDEST telemetry for a customer hierarchy."""
    try:
        with open(REGISTRY_PATH, "rb") as f:
            registry = orjson.loads(f.read())
        
        target_ids = [
            did for did, meta in registry.items()
            if meta.get('platform_customer_id') == pcid and meta.get('application_customer_id') == acid
        ]
        
        if not target_ids:
            return {"status": "error", "message": "No devices found for hierarchy"}
            
        background_tasks.add_task(_export_first_task, target_ids, count)
        return {"status": "accepted", "job": "historical_first_sync", "device_count": len(target_ids)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/health")
def health():
    return {"status": "online", "engine": "V3-Hierarchical-Ingest", "timestamp": str(datetime.now())}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
