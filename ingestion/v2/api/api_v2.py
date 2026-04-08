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
from fastapi import FastAPI, BackgroundTasks, HTTPException

# ── Configuration ───────────────────────────────────────────────────────────
TSDB_HOST = os.getenv("TSDB_HOST", "127.0.0.1")
TSDB_PORT = os.getenv("TSDB_PORT", "5432")
TSDB_USER = os.getenv("TSDB_USER", "postgres")
TSDB_PASS = os.getenv("TSDB_PASS", "postgres")
TSDB_NAME = os.getenv("TSDB_NAME", "postgres")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry-export-v2")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-v3")

app = FastAPI(title="PowerPulse V3 Unified Ingestion API")

# ── Connections ─────────────────────────────────────────────────────────────
_kafka: Optional[aiokafka.AIOKafkaProducer] = None

async def get_kafka():
    global _kafka
    if _kafka is None:
        _kafka = aiokafka.AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
        )
        await _kafka.start()
    return _kafka

async def query_tsdb_range(device_id: str, start_time: datetime, end_time: datetime):
    conn = await asyncpg.connect(host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME)
    records = await conn.fetch("SELECT * FROM telemetry_live WHERE device_id = $1 AND metric_time >= $2 AND metric_time <= $3 ORDER BY metric_time ASC", device_id, start_time, end_time)
    await conn.close()
    return [dict(r) for r in records]

# ── Background Export Task ────────────────────────────────────────────────
async def _export_stream_task(device_ids: List[str], start_time: datetime, end_time: datetime):
    log.info(f"🚀 [export] Starting Background Stream for {len(device_ids)} devices...")
    kafka_prod = await get_kafka()
    processed = 0
    for did in device_ids:
        try:
            readings = await query_tsdb_range(did, start_time, end_time)
            if not readings: continue
            
            message = {
                "device_id": did,
                "history": readings,
                "msg_id": str(uuid.uuid4()),
                "pushed_at": datetime.now(timezone.utc).isoformat()
            }
            await kafka_prod.send(KAFKA_TOPIC, message)
            processed += 1
        except Exception as e:
            log.error(f"Export failed for {did}: {e}")
    log.info(f"✅ [export] Stream Complete: {processed} devices pushed to Kafka.")

# ── Hierarchical Endpoints ────────────────────────────────────────────────
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
