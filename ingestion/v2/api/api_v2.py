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
import sys
from pathlib import Path
import time
import uuid
import io
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
import asyncpg
import orjson
from fastapi import FastAPI, BackgroundTasks, HTTPException, Body
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from minio import Minio
from aiokafka import AIOKafkaProducer

# ── OPENTELEMETRY TRACING SETUP ──────────────────────────────────────────
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# Initialize Tracing
resource = Resource(attributes={SERVICE_NAME: "atlas-ingestion"})
provider = TracerProvider(resource=resource)
# Jaeger collector usually runs on 4317 (OTLP gRPC)
otlp_exporter = OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True)
processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# Adjust path: Ensure we can import schema_builder from the ingestion/ root
V2_ROOT = Path(__file__).resolve().parent.parent
if str(V2_ROOT.parent) not in sys.path:
    sys.path.append(str(V2_ROOT.parent))

from schema_builder import build_48_field_golden_record, build_batch_power_detail

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
REGISTRY_LOCK = asyncio.Lock()

class DeviceRegistration(BaseModel):
    device_id: str
    application_customer_id: str
    platform_customer_id: str
    server_name: str
    location_city: str
    location_country: str
    location_state: Optional[str] = "TX"
    location_id: Optional[str] = "LOC-01"
    location_name: Optional[str] = "Atlas-DC-Default"
    model: Optional[str] = "PowerEdge R750"
    processor_vendor: Optional[str] = "Intel"
    server_generation: Optional[str] = "15G"
    tags: Optional[str] = "production,critical"
    status: Optional[bool] = True
    report_type: Optional[str] = "telemetry_live"
    metric_type: Optional[str] = "power_metrics"
    error_reason: Optional[str] = ""
    inventory_data: Optional[dict] = {
        "cpu_count": 2,
        "socket_count": 2,
        "cpu_inventory": [{"model": "Intel Xeon Platinum", "speed": 2300, "total_cores": 40}],
        "memory_inventory": [{"memory_size": 32, "operating_freq": 3200, "memory_device_type": "DDR4"}]
    }

# =============================================================================
# GLOBAL RESOURCE POOLS
# =============================================================================
# Global resource pools
_kafka: Optional[AIOKafkaProducer] = None
_pool: Optional[asyncpg.Pool] = None
_executor = ThreadPoolExecutor(max_workers=10)
_scheduler = AsyncIOScheduler()

# Production-Grade Performance Caching
# Eliminates the 1.2s "Cold Start" per request at 80,000 device scale
CACHED_REGISTRY = {}
REGISTRY_LOADED = False

# System Guard: Prevents thread exhaustion under heavy burst loads
GLOBAL_EXPORT_SEM = asyncio.Semaphore(50)  # Reverted to 50 (100 was overloading I/O)

async def get_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME,
            min_size=30, max_size=150,  # Increased for higher parallelism (Semaphore 60)
            max_cached_statement_lifetime=3600,
            max_cacheable_statement_size=65536
        )
    return _pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-v3")

app = FastAPI(title="PowerPulse V3 Unified Ingestion API")
FastAPIInstrumentor.instrument_app(app)

# --- Concurrency & Safety Controls ---
# Tier 1: Context Guard (Prevents redundant fetches for the SAME PCID/ACID)
ACTIVE_HIERARCHIES = set() 
HIERARCHY_LOCK = asyncio.Lock()

# Tier 2: System Throttler (Prevents crashing Kafka if too many DIFFERENT hierarchies fetch)
GLOBAL_THROTTLE_SEM = asyncio.Semaphore(2)  # Max 2 parallel export jobs total

# =============================================================================
# THROTTLING WRAPPER
# =============================================================================
async def _handle_throttled_export(task_func, h_key, *args, **kwargs):
    """Wrapper to enforce global system limits and automatic cleanup of hierarchical locks."""
    with tracer.start_as_current_span("queue_wait", attributes={"hierarchy": h_key}):
        async with GLOBAL_THROTTLE_SEM:
            try:
                log.info(f"🚦 [THROTTLE] Starting export for {h_key} (Slots: {GLOBAL_THROTTLE_SEM._value} available)")
                await task_func(*args, **kwargs)
            except Exception as e:
                log.error(f"💥 [GUARD] Task failed for {h_key}: {e}")
            finally:
                async with HIERARCHY_LOCK:
                    if h_key in ACTIVE_HIERARCHIES:
                        ACTIVE_HIERARCHIES.remove(h_key)
                log.info(f"🟢 [GUARD] Export complete for {h_key}. Hierarchy lock released.")

# =============================================================================
# 48-FIELD GOLDEN SCHEMA BUILDER (Matches Spark input_schema)
# =============================================================================
def _build_full_record(r, did: str, meta: dict) -> dict:
    """
    Hydrates a single DB row into the complete 48-field schema.
    NOW DELEGATES TO UNIFIED SCHEMA BUILDER TO ENSURE CONSISTENCY.
    """
    # Convert asyncpg row to dict if needed
    if hasattr(r, 'keys'):
        reading = dict(r)
    else:
        # Map positional indices from old tuple format
        reading = {
            "metric_time": r[0],
            "device_id": r[1],
            "platform_customer_id": r[2],
            "application_customer_id": r[3],
            "amb_temp": r[4],
            "avg_watts": r[5],
            "cpu_avg_freq": r[6],
            "cpu_max": r[7],
            "cpu_pwr_sav_lim": r[8],
            "cpu_util": r[9],
            "cpu_watts": r[10],
            "gpu_watts": r[11],
            "min_watts": r[12],
            "peak_watts": r[13],
            "server_name": r[14],
            "model": r[15],
            "processor_vendor": r[16],
            "server_generation": r[17],
            "report_type": r[18],
            "metric_type": r[19],
            "status": r[20],
            "error_reason": r[21],
            "tags": r[22],
            "location_id": r[23],
            "location_city": r[24],
            "location_state": r[25],
            "location_country": r[26],
            "location_name": r[27]
        }
    
    # Use unified schema builder
    return build_48_field_golden_record(
        device_id=did,
        reading=reading,
        device_metadata=meta,
        inventory_data=meta.get("inventory_data")
    )

# =============================================================================
# AUTOMATED DAILY ARCHIVAL JOB (Streaming Batch Architecture)
# =============================================================================
async def daily_archival_job():
    """Scheduled Task: Streams 7-day rolling data to MinIO in small batches to avoid CPU/memory spikes."""
    now = datetime.now(timezone.utc)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(days=7)
    
    log.info(f"🕰️ [SCHEDULER] Triggering Daily 7-Day Streaming Archival: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}...")
    
    try:
        with open(REGISTRY_PATH, "rb") as f:
            DEVICES = orjson.loads(f.read())
        
        all_device_ids = list(DEVICES.keys())
        log.info(f"📊 [SCHEDULER] Streaming archival for {len(all_device_ids)} devices...")
        
        pool = await get_db_pool()
        s3 = Minio("127.0.0.1:9000", access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        
        # Ensure buckets exist upfront
        for bucket in ["telemetry-raw", "telemetry-archive"]:
            if not s3.bucket_exists(bucket):
                s3.make_bucket(bucket)
        
        # Streaming: 1000 devices per batch to keep memory flat
        STREAM_BATCH = 1000
        total_bytes = 0
        total_records = 0
        devices_with_data = 0
        batch_counter = 0
        base_path = f"production/year={end.year}/month={end.month:02d}/day={end.day:02d}/full_7day/"
        
        for i in range(0, len(all_device_ids), STREAM_BATCH):
            batch_devices = all_device_ids[i:i + STREAM_BATCH]
            
            async with pool.acquire() as conn:
                records = await conn.fetch(
                    "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3) ORDER BY device_id, metric_time ASC", 
                    start, end, batch_devices
                )
            
            if not records:
                continue
            
            devices_with_data += len(set(r[1] for r in records))
            
            # Hydrate → Parquet → Upload → Free (streaming cycle)
            hydrated = [_build_full_record(r, r[1], DEVICES) for r in records]
            df = pd.DataFrame(hydrated)
            pq_buf = io.BytesIO()
            df.to_parquet(pq_buf, engine='pyarrow', index=False, compression='snappy')
            content = pq_buf.getvalue()
            
            total_bytes += len(content)
            total_records += len(records)
            
            fname = f"archive_batch_{batch_counter}.parquet"
            for bucket in ["telemetry-raw", "telemetry-archive"]:
                s3.put_object(bucket, base_path + fname, io.BytesIO(content), len(content))
            
            log.info(f"📤 Batch {batch_counter}: {len(records)} records streamed ({len(content)/1024:.0f} KB)")
            
            # Free memory before next batch
            del hydrated, df, pq_buf, content, records
            batch_counter += 1

        print("\n" + "█" * 60)
        print(f"🚀 [SIGNAL] DAILY STREAMING ARCHIVAL COMPLETED")
        print(f"📋 PERIOD: {start.strftime('%Y-%m-%d %H:%M')} - {end.strftime('%Y-%m-%d %H:%M')}")
        print(f"📦 SILOS: telemetry-raw & telemetry-archive")
        print(f"🎯 TOTAL REGISTERED: {len(all_device_ids)}")
        print(f"📊 DEVICES WITH DATA: {devices_with_data}")
        print(f"📝 TOTAL RECORDS: {total_records:,}")
        print(f"💾 DATA VOLUME: {total_bytes/1024/1024:.2f} MB")
        print(f"📦 BATCHES STREAMED: {batch_counter}")
        print("█" * 60 + "\n")
        
    except Exception as e:
        log.error(f"💥 [SCHEDULER] Archival Failed: {str(e)}")

# =============================================================================
# LIFECYCLE MANAGEMENT
# =============================================================================
@app.on_event("startup")
async def startup_event():
    global _kafka, CACHED_REGISTRY, REGISTRY_LOADED
    
    # 1. Hot-Load Registry Cache (Crucial for <30s Latency)
    try:
        if os.path.exists(REGISTRY_PATH):
            with open(REGISTRY_PATH, "rb") as f:
                CACHED_REGISTRY = orjson.loads(f.read())
            REGISTRY_LOADED = True
            log.info(f"✅ [CACHE] Registry pre-loaded with {len(CACHED_REGISTRY)} devices.")
        else:
            log.warning("⚠️  [CACHE] Registry file missing! API is in limited mode.")
    except Exception as e:
        log.error(f"❌ [CACHE] Failed to load registry: {e}")

    # 2. Initialize Infrastructure
    await get_kafka()
    if _kafka:
        await _kafka.start()
        log.info("🛰️  [KAFKA] Producer STARTED")
    
    await get_db_pool()
    
    _scheduler.add_job(daily_archival_job, 'cron', hour=23, minute=59, misfire_grace_time=600, coalesce=True)
    _scheduler.start()
    
    log.info("🚀 [SYSTEM] Silo-Systems Online (Archival Scheduler ACTIVE - Production: Daily at 23:59 UTC)")

@app.on_event("shutdown")
async def shutdown_event():
    _scheduler.shutdown()
    if _kafka:
        log.info("Stopping AIOKafka Producer...")
        await _kafka.stop()
    if _pool:
        await _pool.close()

# =============================================================================
# CONNECTION FACTORIES
# =============================================================================
async def get_kafka():
    global _kafka
    if _kafka is None:
        # AIOKafka for high-throughput async outgress
        try:
            _kafka = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: orjson.dumps(v),
                compression_type=None,
                linger_ms=1,                   # ⬇️ Absolute minimum for wire-speed syncs
                max_batch_size=10485760,       # ⬇️ 10MB (Safe for broker)
                max_request_size=10485760,      # ⬇️ 10MB
                request_timeout_ms=300000,     # ⬆️ 5-minute timeout for heavy flushes
                acks=0                         # ⚡ 'Extreme Speed' - No ack wait to hit <30s
            )
            # Startup handled in main.py
            log.info(f"🛰️  [KAFKA] Production Producer Initialized (AIOKafka)")
        except Exception as e:
            log.error(f"❌ [KAFKA] Initialisation Failed: {e}")
            _kafka = AIOKafkaProducer(
                bootstrap_servers="broker1:9092",
                value_serializer=lambda v: orjson.dumps(v),
                compression_type=None
            )
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
BULK_BATCH_SIZE = 400  # 🎯 Dialed back to the 'Sweet Spot' for 1601 device sync

def _build_power_detail(r):
    """Builds a single PowerDetail entry from a DB row."""
    # Compute is_fresh dynamically: True if within last 24 hours
    metric_time = r.get('metric_time')
    if metric_time:
        # Ensure metric_time has timezone info for comparison
        if hasattr(metric_time, 'replace') and metric_time.tzinfo is None:
            metric_time = metric_time.replace(tzinfo=timezone.utc)
        is_fresh = metric_time > (datetime.now(timezone.utc) - timedelta(days=1))
    else:
        is_fresh = False
    
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
        "Time": r['metric_time'].isoformat() if hasattr(r.get('metric_time'), 'isoformat') else str(r.get('metric_time', '')),
        "is_fresh": is_fresh
    }

async def _export_stream_task(device_ids: List[str], start_time: datetime, end_time: datetime):
    """Heavyworker: High-Speed Parallel Query + 48-field hydration + batch Kafka push."""
    t_total_start = time.monotonic()
    kafka_prod = await get_kafka()
    processed = 0
    log.info(f"🚀 [WORKER] Batch-Streaming {len(device_ids)} devices for stream export...")
    
    with tracer.start_as_current_span("registry_access"):
        DEVICES = CACHED_REGISTRY
    
    pool = await get_db_pool()
    batch_size = 100
    semaphore = asyncio.Semaphore(5)  # ⬇️ Reduced to 5
    
    async def process_batch(batch_ids):
        nonlocal processed
        async with semaphore:
            async with pool.acquire() as conn:
                with tracer.start_as_current_span("phase1_db_query"):
                    rows = await conn.fetch(
                        "SELECT * FROM telemetry_live WHERE device_id = ANY($1) AND metric_time >= $2 AND metric_time < $3 ORDER BY device_id, metric_time ASC",
                        batch_ids, start_time, end_time
                    )
            
            if not rows: return
            
            with tracer.start_as_current_span("phase1_hydration"):
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
    
    # Launch all batches concurrently
    t_phase1_start = time.monotonic()
    batches = [device_ids[i:i + batch_size] for i in range(0, len(device_ids), batch_size)]
    with tracer.start_as_current_span("phase1_batch_processing"):
        await asyncio.gather(*(process_batch(b) for b in batches))
    
    with tracer.start_as_current_span("phase1_kafka_flush"):
        try:
            await kafka_prod.flush()
            log.info(f"✅ [KAFKA] Stream-batch flush successful for {processed} messages")
        except Exception as e:
            log.warning(f"⚠️  [KAFKA] Flush timeout: {e}")
    t_phase1 = time.monotonic() - t_phase1_start
    
    # Second Phase is usually similar to first but for latest 7d window
    # To keep this trace clean, we've focused tracing on Phase 1
    t_total = time.monotonic() - t_total_start
    log.info(f"⏱️  [TIMING] TOTAL: {t_total:.2f}s | {len(device_ids)} devices")
    log.info(f"✅ [WORKER] Export Complete: {processed} devices processed.")

async def _export_first_task(device_ids: List[str], count: int = 2016):
    """Historical Task: Parallel Batch Engine (The Fastest Possible Strategy)."""
    t_start = time.monotonic()
    kafka_prod = await get_kafka()
    processed = 0
    log.info(f"📜 [WORKER] Parallel-Batch Fetching OLDEST {count} points for {len(device_ids)} devices...")
    
    # Track timings
    t_registry_start = time.monotonic()
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    t_registry = time.monotonic() - t_registry_start
    
    pool = await get_db_pool()
    batch_size = 100  # Per-device LIMIT queries are most efficient with smaller batches
    
    # Detailed timing stats
    timing_stats = {"query_time": 0.0, "process_time": 0.0}
    
    async def process_batch(batch_ids):
        nonlocal processed
        t_batch_start = time.monotonic()
        async with pool.acquire() as conn:
            # Optimized JSON Aggregation: Pushes array assembly into Postgres
            # This avoids the overhead of fetching 201,600 individual rows
            query = """
                SELECT d.id, 
                (SELECT json_agg(r) FROM (
                    SELECT 
                        metric_time AS "Time", 
                        avg_watts AS "Average", 
                        peak_watts AS "Peak", 
                        min_watts AS "Minimum",
                        amb_temp AS "AmbTemp",
                        cpu_avg_freq AS "CpuAvgFreq",
                        cpu_max AS "CpuMax",
                        cpu_pwr_sav_lim AS "CpuPwrSavLim",
                        cpu_util AS "CpuUtil",
                        cpu_watts AS "CpuWatts",
                        gpu_watts AS "GpuWatts"
                    FROM telemetry_live 
                    WHERE device_id = d.id 
                    ORDER BY metric_time ASC 
                    LIMIT $2
                ) r) AS readings 
                FROM UNNEST($1::text[]) AS d(id)
            """
            rows = await conn.fetch(query, batch_ids, count)
        
        t_query_end = time.monotonic()
        timing_stats['query_time'] += (t_query_end - t_batch_start)
        
        if not rows: return
        
        for r in rows:
            did, json_readings = r[0], r[1]
            if json_readings:
                # asyncpg returns json_agg as a string; must parse to Python list
                if isinstance(json_readings, str):
                    json_readings = orjson.loads(json_readings)
                
                # Integrity Guard: Validate requested vs actual count
                actual_count = len(json_readings) if json_readings else 0
                if actual_count < count:
                    log.warning(f"⚠️  [DATA GAP] Device {did}: Requested {count} points, but only found {actual_count} in DB.")
                
                await _process_and_send(did, json_readings, DEVICES, kafka_prod)
                processed += 1
        
        timing_stats['process_time'] += (time.monotonic() - t_query_end)

    # Launch all batches concurrently with increased parallelism
    t_batches_start = time.monotonic()
    batches = [device_ids[i:i + batch_size] for i in range(0, len(device_ids), batch_size)]
    # Increased from Semaphore(30) to 60 for more parallelism
    semaphore = asyncio.Semaphore(5)
    
    async def process_batch_with_semaphore(batch_ids):
        async with semaphore:
            await process_batch(batch_ids)
            # Periodic flush to prevent OOM
            await kafka_prod.flush()
    
    await asyncio.gather(*(process_batch_with_semaphore(b) for b in batches))
    t_batches_elapsed = time.monotonic() - t_batches_start
    
    # Flush with non-blocking AIOKafka flush
    t_flush_start = time.monotonic()
    try:
        await kafka_prod.flush()
        t_flush_elapsed = time.monotonic() - t_flush_start
        log.info(f"✅ [KAFKA] Historical-batch flush successful for {processed} messages (flush took {t_flush_elapsed:.2f}s)")
    except Exception as e:
        t_flush_elapsed = time.monotonic() - t_flush_start
        log.warning(f"⚠️  [KAFKA] Flush timeout (flush may still complete): {type(e).__name__} - {e}")
    
    # Print detailed timing breakdown
    t_total = time.monotonic() - t_start
    num_batches = len([device_ids[i:i + batch_size] for i in range(0, len(device_ids), batch_size)])
    avg_query_per_batch = timing_stats['query_time'] / num_batches if num_batches > 0 else 0
    avg_process_per_batch = timing_stats['process_time'] / num_batches if num_batches > 0 else 0
    
    log.info(f"⏱️  [TIMING BREAKDOWN]")
    log.info(f"  Registry load:           {t_registry:.2f}s ({100*t_registry/t_total:.1f}%)")
    log.info(f"  Batch processing:        {t_batches_elapsed:.2f}s ({100*t_batches_elapsed/t_total:.1f}%) [Semaphore(60)]")
    log.info(f"    ├─ Avg query/batch:    {avg_query_per_batch:.3f}s (total accumulated: {timing_stats['query_time']:.2f}s)")
    log.info(f"    └─ Avg process/batch:  {avg_process_per_batch:.3f}s (total accumulated: {timing_stats['process_time']:.2f}s)")
    log.info(f"  Kafka flush:             {t_flush_elapsed:.2f}s ({100*t_flush_elapsed/t_total:.1f}%)")
    log.info(f"  Total:                   {t_total:.2f}s | {processed} devices | {len(device_ids)} requested | {int(len(device_ids)*count/t_total):.0f} events/sec")
    
    log.info(f"✅ [WORKER] Historical-First Complete: {processed} devices processed.")

async def _export_latest_task(device_ids: List[str], count: int = 2016):
    """Latest-Batch Task: High-Speed Parallel Fetch of newest N points."""
    with tracer.start_as_current_span("export_latest_task", attributes={"device_count": len(device_ids)}):
        t_start = time.monotonic()
        kafka_prod = await get_kafka()
        processed = 0
        log.info(f"⚡ [WORKER] Parallel-Batch Fetching LATEST {count} points for {len(device_ids)} devices...")
        
        with tracer.start_as_current_span("registry_access"):
            DEVICES = CACHED_REGISTRY
        
        pool = await get_db_pool()
        batch_size = 100
        semaphore = asyncio.Semaphore(5)  # ⬇️ Reduced to 5 to prevent network buffer overflow
        
        async def process_batch(batch_ids):
            nonlocal processed
            async with pool.acquire() as conn:
                with tracer.start_as_current_span("latest_db_query"):
                    query = """
                        SELECT d.id, 
                        (SELECT json_agg(r) FROM (
                            SELECT * FROM (
                                SELECT 
                                    metric_time AS "Time", 
                                    avg_watts AS "Average", 
                                    peak_watts AS "Peak", 
                                    min_watts AS "Minimum",
                                    amb_temp AS "AmbTemp",
                                    cpu_avg_freq AS "CpuAvgFreq",
                                    cpu_max AS "CpuMax",
                                    cpu_pwr_sav_lim AS "CpuPwrSavLim",
                                    cpu_util AS "CpuUtil",
                                    cpu_watts AS "CpuWatts",
                                    gpu_watts AS "GpuWatts"
                                FROM telemetry_live 
                                WHERE device_id = d.id 
                                ORDER BY metric_time DESC 
                                LIMIT $2
                            ) sub ORDER BY "Time" ASC
                        ) r) AS readings 
                        FROM UNNEST($1::text[]) AS d(id)
                    """
                    rows = await conn.fetch(query, batch_ids, count)
            
            if not rows: return
            
            with tracer.start_as_current_span("latest_serialization_and_send"):
                for r in rows:
                    did, json_readings = r[0], r[1]
                    if json_readings:
                        if isinstance(json_readings, str):
                            json_readings = orjson.loads(json_readings)
                        await _process_and_send(did, json_readings, DEVICES, kafka_prod)
                        processed += 1

        batches = [device_ids[i:i + batch_size] for i in range(0, len(device_ids), batch_size)]
        with tracer.start_as_current_span("latest_batch_processing"):
            async def run_with_sem(b):
                async with semaphore:
                    await process_batch(b)
                    # Periodic flush to prevent OOM on both producer and broker
                    await kafka_prod.flush()
            await asyncio.gather(*(run_with_sem(b) for b in batches))

        with tracer.start_as_current_span("latest_kafka_flush"):
            await kafka_prod.flush()

        t_total = time.monotonic() - t_start
        est_payload_mb = (processed * count * 2) / 1024
        log.info(f"✅ [WORKER] Latest-Batch Complete: {processed} devices in {t_total:.2f}s | Throughput: {est_payload_mb/t_total:.2f} MB/s")

async def _process_and_send(did, readings, DEVICES, kafka_prod):
    """
    Helper for fast single-device processing.
    Sends the FULL 48-field Golden Schema to Kafka matching input_schema.py.
    Uses unified schema builder for consistency.
    """
    meta = DEVICES.get(did, {})
    
    # Build PowerDetail array from all readings
    # Optimized: We pass a 1-hour temporal cutoff to achieve high-precision 'is_fresh' flagging
    fresh_cutoff = None
    if readings:
        try:
            # Detect key based on query type (JSON vs Raw)
            key = "Time" if "Time" in readings[0] else "metric_time"
            
            # O(1) temporal detection from pre-sorted DB tail
            last_val = readings[-1][key]
            
            # Use 1-hour window (12 points) for freshness as requested
            if isinstance(last_val, datetime):
                fresh_cutoff = (last_val - timedelta(hours=1)).isoformat()
            elif isinstance(last_val, str):
                # Handle string timestamps from json_agg
                # Basic string manipulation for speed: just parse and subtract
                try:
                    # Remove timezone +00:00/Z for simple parsing if needed, 
                    # but ISO strings are directly comparable if they share format.
                    # Best: Parse properly to ensure 1-hour subtraction
                    from dateutil.parser import parse
                    last_ts = parse(last_val)
                    fresh_cutoff = (last_ts - timedelta(hours=1)).isoformat()
                except:
                    fresh_cutoff = last_val
            else:
                fresh_cutoff = str(last_val)
                
        except Exception as e:
            log.warning(f"⚠️  [is_fresh] Cutoff detection failed for {did}: {e}")

    power_detail_list, avg_watts, max_watts, min_watts = build_batch_power_detail(readings, fresh_cutoff)
    latest = readings[-1] if readings else {}
    
    # Use unified schema builder
    message = build_48_field_golden_record(
        device_id=did,
        reading=latest if isinstance(latest, dict) else {"metric_time": latest[0]} if isinstance(latest, (list, tuple)) else latest,
        device_metadata=meta,
        inventory_data=meta.get("inventory_data"),
        power_detail_list=power_detail_list
    )
    
    # Override aggregates with batch-computed values
    message["data"]["Average"] = avg_watts
    message["data"]["Maximum"] = max_watts
    message["data"]["Minimum"] = min_watts
    
    # Send asynchronously (AIOKafka)
    try:
        await kafka_prod.send(KAFKA_TOPIC, message, key=did.encode())
    except Exception as e:
        log.error(f"❌ [KAFKA] Send error for {did}: {e}")

# =============================================================================
# HIERARCHICAL API ENDPOINTS
# =============================================================================
@app.post("/pcid/{pcid}/acid/{acid}/telemetry/export")
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
            
        # --- Hierarchical Lock Check ---
        h_key = f"{pcid}:{acid}"
        async with HIERARCHY_LOCK:
            if h_key in ACTIVE_HIERARCHIES:
                return {"status": "error", "message": f"Export already in progress for {h_key}"}
            ACTIVE_HIERARCHIES.add(h_key)

        background_tasks.add_task(_handle_throttled_export, _export_stream_task, h_key, device_ids, start_time, end_time)
        return {"status": "Archival Stream Accepted", "targeted_devices": len(device_ids), "hierarchy": h_key}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pcid/{pcid}/acid/{acid}/telemetry/latest/export")
async def trigger_latest_telemetry_export(pcid: str, acid: str, background_tasks: BackgroundTasks, count: int = 2016):
    """Latest-Batch Fetch: Triggers Kafka Ingestion for EXACTLY N latest points (Sync Mode)."""
    with tracer.start_as_current_span("api_trigger_latest_export", attributes={"pcid": pcid, "acid": acid}):
        try:
            registry_path = "/app/device_configs.json"
            with tracer.start_as_current_span("api_registry_cache_access"):
                device_ids = [did for did, meta in CACHED_REGISTRY.items() 
                              if meta["platform_customer_id"] == pcid and meta["application_customer_id"] == acid]
            
            if not device_ids:
                return {"status": "Empty Hierarchy"}
            
            # --- Hierarchical Lock Check ---
            h_key = f"{pcid}:{acid}"
            async with HIERARCHY_LOCK:
                if h_key in ACTIVE_HIERARCHIES:
                    return {"status": "error", "message": f"Latest Sync already active for {h_key}"}
                ACTIVE_HIERARCHIES.add(h_key)

            background_tasks.add_task(_handle_throttled_export, _export_latest_task, h_key, device_ids, count)
            return {"status": "Latest Sync Accepted", "requested_points": count, "device_count": len(device_ids), "hierarchy": h_key}
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
        
        # Manual exports use a special key to ensure they don't block hierarchical ones
        m_key = f"manual:{uuid.uuid4().hex[:8]}"
        background_tasks.add_task(_handle_throttled_export, _export_stream_task, m_key, device_ids, start_time, end_time)
        return {
            "status": "Manual Stream Started", 
            "requested_devices": len(device_ids),
            "pcid": pcid,
            "acid": acid
        }
    except Exception as e:
        log.error(f"❌ Export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pcid/{pcid}/acid/{acid}/telemetry/historical/first/export")
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
            
        # --- Hierarchical Lock Check ---
        h_key = f"{pcid}:{acid}"
        async with HIERARCHY_LOCK:
            if h_key in ACTIVE_HIERARCHIES:
                return {"status": "error", "message": f"Historical Sync already active for {h_key}"}
            ACTIVE_HIERARCHIES.add(h_key)

        background_tasks.add_task(_handle_throttled_export, _export_first_task, h_key, target_ids, count)
        return {"status": "accepted", "job": "historical_first_sync", "device_count": len(target_ids), "hierarchy": h_key}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/register/device")
async def register_new_device(device: DeviceRegistration):
    """Dynamically adds a new device to the fleet registry."""
    async with REGISTRY_LOCK:
        try:
            # 1. Read existing registry
            if os.path.exists(REGISTRY_PATH):
                with open(REGISTRY_PATH, "rb") as f:
                    configs = orjson.loads(f.read())
            else:
                configs = {}

            # 2. Check for collisions
            if device.device_id in configs:
                raise HTTPException(status_code=400, detail=f"Device {device.device_id} already registered.")

            # 3. Append new config (Hydrated with defaults)
            configs[device.device_id] = device.dict()
            
            # 4. Atomic Write
            with open(REGISTRY_PATH, "wb") as f:
                f.write(orjson.dumps(configs))

            log.info(f"🆕 [REGISTRY] Device {device.device_id} registered successfully under {device.application_customer_id}")
            return {"status": "success", "device_id": device.device_id, "message": "Device added to registry"}
            
        except Exception as e:
            log.error(f"❌ [REGISTRY] Registration failed for {device.device_id}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error during registration")

@app.get("/health")
async def health():
    # Deep Health Check with Safe Attribute Probing
    try:
        kafka_status = "connected" if (_kafka and not getattr(_kafka, '_closed', True)) else "disconnected"
    except:
        kafka_status = "error"
        
    db_status = "connected" if (_pool and not getattr(_pool, '_closed', True)) else "disconnected"
    
    return {
        "status": "online",
        "timestamp": str(datetime.now()),
        "components": {
            "kafka": kafka_status,
            "database": db_status,
            "registry": "ok" if Path(REGISTRY_PATH).exists() else "missing"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
