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
import datetime
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncpg
import orjson
from fastapi import FastAPI, BackgroundTasks, HTTPException, Body, Query
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from minio import Minio
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis
import pyarrow as pa
import pyarrow.compute as pc


# Config
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

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
# Thread pool for synchronous background tasks (like Kafka flushing)
_executor = ThreadPoolExecutor(max_workers=20)

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
if ":" not in MINIO_HOST:
    MINIO_HOST = f"{MINIO_HOST}:9000"
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
_redis: Optional[Any] = None
_minio: Optional[Minio] = None
_executor = ThreadPoolExecutor(max_workers=200)
_cpu_pool_internal = None

def get_cpu_pool():
    global _cpu_pool_internal
    if _cpu_pool_internal is None:
        import multiprocessing
        # 'spawn' is safer than 'fork' for avoiding deadlocks in async/multi-threaded apps
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass
        _cpu_pool_internal = ProcessPoolExecutor(max_workers=min(os.cpu_count() or 4, 10))
    return _cpu_pool_internal

_scheduler = AsyncIOScheduler()

# Redis Index Key: idx:telemetry:{pcid}:{acid} -> Set of available hours (YYYYMMDDHH)
REDIS_INDEX_PREFIX = "idx:telemetry"

# Database Schema Columns (for vectorized hydration)
DB_COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "server_name", "model", "processor_vendor", "server_generation",
    "report_type", "metric_type", "status", "error_reason", "tags",
    "location_id", "location_city", "location_state", "location_country", "location_name"
]

# Metric-Only Columns (Strips redundant metadata for lean exports)
METRIC_COLUMNS = [
    "metric_time", "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max",
    "cpu_pwr_sav_lim", "cpu_util", "cpu_watts", "gpu_watts", "min_watts",
    "peak_watts", "status", "error_reason"
]

# Production-Grade Performance Caching
# Eliminates the 1.2s "Cold Start" per request at 80,000 device scale
CACHED_REGISTRY = {}
REGISTRY_DF = pd.DataFrame()
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
GLOBAL_THROTTLE_SEM = asyncio.Semaphore(2)  # Throttled for stability under 500-platform bursts

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
# HOURLY CACHE JOB (Optimized for API Exports)
# =============================================================================
async def hourly_cache_job():
    """Scheduled Task: Streams the last hour of data into the PCID/ACID partitioned cache."""
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    
    log.info(f"🕰️ [CACHE] Refreshing Hourly Cache: {start.strftime('%H:%M')} to {end.strftime('%H:%M')}...")
    
    try:
        with open(REGISTRY_PATH, "rb") as f:
            DEVICES = orjson.loads(f.read())
        
        all_device_ids = list(DEVICES.keys())
        pool = await get_db_pool()
        s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        
        if not s3.bucket_exists("telemetry-cache"):
            s3.make_bucket("telemetry-cache")
        
        # Incremental Streaming
        STREAM_BATCH = 2000
        total_records = 0
        base_path = f"date={start.strftime('%Y-%m-%d')}/hour={start.strftime('%H')}/"
        
        for i in range(0, len(all_device_ids), STREAM_BATCH):
            batch_devices = all_device_ids[i:i + STREAM_BATCH]
            async with pool.acquire() as conn:
                records = await conn.fetch(
                    "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
                    start, end, batch_devices
                )
            
            if not records: continue
            
            log.info(f"📦 [CACHE] Processing batch of {len(records)} records...")
            
            # 🏎️ VECTORIZED HYDRATION (No loops!)
            # 1. Convert DB Records to DataFrame (Raw metrics only)
            df_raw = pd.DataFrame(records, columns=DB_COLUMNS)
            
            # 2. Vectorized Merge with Registry for Customer Info
            # This handles PCID/ACID mapping at C-speed via pandas merge
            df = df_raw.merge(
                REGISTRY_DF[['device_id', 'platform_customer_id', 'application_customer_id', 'server_name', 'model']], 
                on="device_id", 
                how="left",
                suffixes=('', '_reg')
            )
            
            # Use registry values if DB values are missing/null (fallback)
            df['platform_customer_id'] = df['platform_customer_id'].fillna(df['platform_customer_id_reg'])
            df['application_customer_id'] = df['application_customer_id'].fillna(df['application_customer_id_reg'])
            df['server_name'] = df['server_name'].fillna(df['server_name_reg'])
            df['model'] = df['model'].fillna(df['model_reg'])
            
            # Select relevant columns for cache (Raw Metrics + Keys)
            final_cols = [
                "metric_time", "device_id", "platform_customer_id", "application_customer_id",
                "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
                "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts"
            ]
            df_final = df[final_cols]
            
            for (pcid, acid), group_df in df_final.groupby(['platform_customer_id', 'application_customer_id']):
                cache_buf = io.BytesIO()
                group_df.to_parquet(cache_buf, engine='pyarrow', index=False, compression='snappy')
                cache_content = cache_buf.getvalue()
                
                cache_fname = f"{base_path}pcid={pcid}/acid={acid}/cache.parquet"
                s3.put_object("telemetry-cache", cache_fname, io.BytesIO(cache_content), len(cache_content))
                
                # 📝 Update Redis Index (Instant Discovery Heartbeat)
                rd = await get_redis()
                hour_key = start.strftime('%Y%m%d%H')
                index_key = f"{REDIS_INDEX_PREFIX}:{pcid}:{acid}"
                await rd.sadd(index_key, hour_key)
                
                total_records += len(group_df)
            
            del df_raw, df, df_final, records

        log.info(f"✅ [CACHE] Hourly refresh complete. Total records partitioned: {total_records:,}")
        
    except Exception as e:
        log.error(f"💥 [CACHE] Hourly Refresh Failed: {str(e)}")

# =============================================================================
# DAILY LONG-TERM ARCHIVAL JOB (Consolidated Data Lake)
# =============================================================================
async def daily_archival_job():
    """Scheduled Task: Streams 24h of consolidated data to Raw and Archive buckets."""
    now = datetime.now(timezone.utc)
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=1)
    
    log.info(f"🕰️ [ARCHIVE] Consolidating Daily Data Lake: {start.strftime('%Y-%m-%d')}...")
    
    try:
        with open(REGISTRY_PATH, "rb") as f:
            DEVICES = orjson.loads(f.read())
        
        all_device_ids = list(DEVICES.keys())
        pool = await get_db_pool()
        s3 = get_minio()
        
        for bucket in ["telemetry-raw", "telemetry-archive"]:
            if not s3.bucket_exists(bucket):
                s3.make_bucket(bucket)
        
        STREAM_BATCH = 1000
        total_records = 0
        date_str = start.strftime('%Y-%m-%d')
        
        for i in range(0, len(all_device_ids), STREAM_BATCH):
            batch_devices = all_device_ids[i:i + STREAM_BATCH]
            async with pool.acquire() as conn:
                records = await conn.fetch(
                    "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
                    start, end, batch_devices
                )
            
            if not records: continue
            
            hydrated = [_build_full_record(r, r[1], DEVICES) for r in records]
            df = pd.DataFrame(hydrated)
            pq_buf = io.BytesIO()
            df.to_parquet(pq_buf, engine='pyarrow', index=False, compression='snappy')
            content = pq_buf.getvalue()
            
            fname = f"date={date_str}/batch_{i//STREAM_BATCH}.parquet"
            for bucket in ["telemetry-raw", "telemetry-archive"]:
                s3.put_object(bucket, fname, io.BytesIO(content), len(content))
            
            total_records += len(records)
            del hydrated, df, pq_buf, content, records

        log.info(f"✅ [ARCHIVE] Daily consolidation complete. Total records archived: {total_records:,}")
        
    except Exception as e:
        log.error(f"💥 [ARCHIVE] Daily Consolidation Failed: {str(e)}")

# =============================================================================
# LIFECYCLE MANAGEMENT
# =============================================================================
def load_registry():
    global CACHED_REGISTRY, REGISTRY_LOADED, REGISTRY_DF
    if REGISTRY_LOADED: return
    
    try:
        with open(REGISTRY_PATH, "rb") as f:
            CACHED_REGISTRY = orjson.loads(f.read())
            
        # Build Vectorized Registry for Archival Performance
        registry_list = []
        for did, meta in CACHED_REGISTRY.items():
            registry_list.append({
                "device_id": did,
                "platform_customer_id": meta.get("platform_customer_id"),
                "application_customer_id": meta.get("application_customer_id"),
                "server_name": meta.get("server_name"),
                "model": meta.get("model")
            })
        REGISTRY_DF = pd.DataFrame(registry_list)
        
        REGISTRY_LOADED = True
        log.info(f"✅ [CACHE] Registry pre-loaded with {len(CACHED_REGISTRY)} devices.")
    except Exception as e:
        log.error(f"❌ [CACHE] Failed to load registry: {e}")

@app.on_event("startup")
async def startup_event():
    global _kafka, CACHED_REGISTRY, REGISTRY_LOADED
    
    # 1. Hot-Load Registry Cache (Crucial for <30s Latency)
    load_registry()

    # 2. Initialize Infrastructure
    await get_kafka()
    if _kafka:
        await _kafka.start()
        log.info("🛰️  [KAFKA] Producer STARTED")
    
    await get_db_pool()
    
    # 3. Initialize MinIO Buckets Upfront
    try:
        s3 = get_minio()
        for bucket in ["telemetry-raw", "telemetry-archive", "telemetry-cache"]:
            if not s3.bucket_exists(bucket):
                s3.make_bucket(bucket)
                log.info(f"🪣 [MINIO] Bucket created: {bucket}")
    except Exception as e:
        log.error(f"❌ [MINIO] Bucket initialization failed: {e}")
    
    # Schedule: Dual-Tier Archival Strategy
    _scheduler.add_job(hourly_cache_job, 'cron', minute=0, misfire_grace_time=600, coalesce=True)
    _scheduler.add_job(daily_archival_job, 'cron', hour=0, minute=10, misfire_grace_time=3600, coalesce=True)
    _scheduler.start()
    
    log.info("🚀 [SYSTEM] Silo-Systems Online (Dual-Tier Archival ACTIVE: Hourly Cache + Daily Long-term)")

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
                value_serializer=lambda v: v if isinstance(v, bytes) else orjson.dumps(v),
                compression_type="snappy",     # 📦 Snappy for massive bandwidth reduction
                linger_ms=5,                   # ⬇️ Reduced from 25ms to 5ms for lower local latency
                max_batch_size=10485760,       # ⬆️ Increased to 10MB to match large telemetry exports
                max_request_size=10485760,      # ⬆️ Increased to 10MB
                request_timeout_ms=300000,     # ⬆️ 5-minute timeout for heavy flushes
                acks=1                         # ⚖️ Balanced - Wait for leader ack to prevent socket flooding
            )
            # Startup handled in main.py
            log.info(f"🛰️  [KAFKA] Production Producer Initialized (AIOKafka + Snappy)")
        except Exception as e:
            log.error(f"❌ [KAFKA] Initialisation Failed: {e}")
            _kafka = AIOKafkaProducer(
                bootstrap_servers="broker1:9092",
                value_serializer=lambda v: orjson.dumps(v),
                compression_type=None
            )
    return _kafka

async def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return _redis

def get_minio():
    global _minio
    if _minio is None:
        import urllib3
        # 🚀 CUSTOM POOL: Expanded for 168+ parallel hourly downloads
        http_client = urllib3.PoolManager(
            retries=False,
            maxsize=300,
            num_pools=10
        )
        _minio = Minio(
            MINIO_HOST, 
            access_key=MINIO_ACCESS, 
            secret_key=MINIO_SECRET, 
            secure=False,
            http_client=http_client
        )
    return _minio

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

def process_device_batch_hydration(
    table: pa.Table, 
    devices_meta: dict, 
    count: int
) -> list:
    """
    Vectorized Hydration: Processes a batch of devices via PyArrow.
    Runs in ProcessPoolExecutor to bypass GIL.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import orjson
    from datetime import datetime, timezone
    import time

    if table.num_rows == 0: return []
    
    # Sort for consistent latest-point slicing
    indices = pc.sort_indices(table, sort_keys=[("device_id", "ascending"), ("metric_time", "descending")])
    sorted_table = table.take(indices)
    
    # 🏎️ Vectorized String Conversion (for JSON compatibility)
    if "metric_time" in sorted_table.column_names:
        # Format as ISO string: 2026-05-03T07:27:31Z
        ts_col = sorted_table["metric_time"]
        str_ts = pc.strftime(ts_col, format="%Y-%m-%dT%H:%M:%SZ")
        sorted_table = sorted_table.set_column(sorted_table.schema.get_field_index("metric_time"), "metric_time", str_ts)

    unique_dids = pc.unique(sorted_table["device_id"]).to_pylist()
    results = []
    
    for did in unique_dids:
        # 🏎️ Vectorized Slicing
        mask = pc.equal(sorted_table["device_id"], did)
        device_table = sorted_table.filter(mask)
        
        # Take N latest points
        if count > 0:
            device_table = device_table.slice(0, min(count, device_table.num_rows))
            
        # 🏎️ Lean Payload: Only select METRIC_COLUMNS
        # We find which columns actually exist in the table
        metric_cols = ["metric_time", "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max",
                       "cpu_pwr_sav_lim", "cpu_util", "cpu_watts", "gpu_watts", "min_watts",
                       "peak_watts", "status", "error_reason"]
        available_metrics = [c for c in metric_cols if c in device_table.column_names]
        device_table = device_table.select(available_metrics)
            
        # Convert to Golden Records
        readings = device_table.to_pylist()
        meta = devices_meta.get(did, {})
        
        payload = {
            "device_id": did,
            "report_id": f"STITCHED-{int(time.time())}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": True,
            "model": meta.get('model', 'Unknown'),
            "tags": meta.get('tags', ''),
            "report_type": "telemetry_live",
            "server_name": meta.get('server_name', 'Unknown'),
            "error_reason": "",
            "location_id": meta.get('location_id', ''),
            "location_city": meta.get('location_city', ''),
            "location_name": meta.get('location_name', ''),
            "location_state": meta.get('location_state', ''),
            "location_country": meta.get('location_country', ''),
            "processor_vendor": meta.get('processor_vendor', ''),
            "server_generation": meta.get('server_generation', ''),
            "platform_customer_id": meta.get('platform_customer_id', ''),
            "application_customer_id": meta.get('application_customer_id', ''),
            "metric_type": "power_metrics",
            "data": { "PowerDetail": readings }
        }
        results.append((did, orjson.dumps(payload)))
        
    return results

def _serialize_record(did, readings, meta):
    """CPU-Bound: Runs in ProcessPool to bypass GIL."""
    import orjson
    from datetime import datetime, timezone
    payload = {
        "device_id": did,
        "report_id": meta.get('report_id', 'STITCHED-' + str(int(time.time()))),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": True,
        "model": meta.get('model', 'Unknown'),
        "tags": meta.get('tags', ''),
        "report_type": "telemetry_live",
        "server_name": meta.get('server_name', 'Unknown'),
        "error_reason": "",
        "location_id": meta.get('location_id', ''),
        "location_city": meta.get('location_city', ''),
        "location_name": meta.get('location_name', ''),
        "location_state": meta.get('location_state', ''),
        "location_country": meta.get('location_country', ''),
        "processor_vendor": meta.get('processor_vendor', ''),
        "server_generation": meta.get('server_generation', ''),
        "platform_customer_id": meta.get('platform_customer_id', ''),
        "application_customer_id": meta.get('application_customer_id', ''),
        "metric_type": "power_metrics",
        "data": {
            "PowerDetail": readings
        }
    }
    return orjson.dumps(payload)

async def _process_and_send(did, readings, DEVICES, kafka_prod):
    """Async wrapper that delegates serialization to CPU pool."""
    meta = DEVICES.get(did, {})
    loop = asyncio.get_event_loop()
    
    # 🏎️ Parallel Serialization (Offloaded to separate CPU core)
    payload_bytes = await loop.run_in_executor(_cpu_pool, _serialize_record, did, readings, meta)
    
    # 🛰️ Kafka Delivery
    await kafka_prod.send(KAFKA_TOPIC, payload_bytes, key=did.encode())

async def _export_stream_task(start_time: datetime, end_time: datetime, pcid: str = None, acid: str = None, device_ids: List[str] = None):
    """
    Hybrid Export Task:
    1. Checks MinIO Cache for historical data (if PCID/ACID provided).
    2. Fetches remaining 'Fresh' data from TimescaleDB.
    3. Streams the combined result to Kafka.
    """
    t_start = time.monotonic()
    kafka_prod = await get_kafka()
    processed_records = 0
    
    DEVICES = CACHED_REGISTRY
    h_label = f"{pcid}:{acid}" if pcid and acid else "manual-export"
    
    # Discovery Logic
    if not device_ids:
        if pcid and acid:
            target_ids = [did for did, meta in DEVICES.items() 
                          if meta.get('platform_customer_id') == pcid and meta.get('application_customer_id') == acid]
        else:
            log.warning(f"⚠️ [WORKER] Missing hierarchy info")
            return
    else:
        target_ids = device_ids
    
    if not target_ids:
        log.warning(f"⚠️ [WORKER] No devices found for {h_label}")
        return

    # Initialize MinIO Client
    s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

    # 1. CACHE PATH (MinIO) - Only if we have PCID/ACID
    missing_slots = [start_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=i) 
                     for i in range(int((end_time - start_time).total_seconds() // 3600) + 1)]
    
    if pcid and acid:
        with tracer.start_as_current_span("cache_fetch"):
            cached_records, found_slots = await _fetch_from_cache(s3, pcid, acid, start_time, end_time)
            if cached_records:
                log.info(f"🎯 [CACHE] Found {len(cached_records)} records in MinIO for {pcid}:{acid}")
                df_cached = pd.DataFrame(cached_records)
                # If we are targeting specific device_ids, filter the cache
                if device_ids:
                    df_cached = df_cached[df_cached['device_id'].isin(device_ids)]
                
                for did, group in df_cached.groupby('device_id'):
                    readings = group.to_dict('records')
                    await _process_and_send(did, readings, DEVICES, kafka_prod)
                    processed_records += len(readings)
                
                # Update missing slots
                missing_slots = [s for s in missing_slots if s not in found_slots]
    
    # 2. HOT PATH (TimescaleDB Fallback)
    if missing_slots:
        log.info(f"🔌 [HOT PATH] Fetching {len(missing_slots)} hourly slots from TimescaleDB...")
        pool = await get_db_pool()
        db_start = min(missing_slots)
        db_end = end_time
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM telemetry_live WHERE device_id = ANY($1) AND metric_time >= $2 AND metric_time < $3 ORDER BY device_id, metric_time ASC",
                target_ids, db_start, db_end
            )
            
            if rows:
                current_did = None
                device_readings = []
                for r in rows:
                    did = r[1]
                    if current_did is None: current_did = did
                    if did != current_did:
                        await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
                        processed_records += len(device_readings)
                        current_did = did
                        device_readings = []
                    device_readings.append({k: r[k] for k in METRIC_COLUMNS if k in r})
                
                if current_did and device_readings:
                    await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
                    processed_records += len(device_readings)

    await kafka_prod.flush()
    t_total = time.monotonic() - t_start
    log.info(f"✅ [HYBRID] Export Complete for {h_label} | Total Points: {processed_records:,} | Time: {t_total:.2f}s")

async def _fetch_from_cache_df(s3, pcid, acid, start_time: datetime, end_time: datetime):
    """Memory Optimized: Returns (DataFrame, found_slots)"""
    rd = await get_redis()
    index_key = f"{REDIS_INDEX_PREFIX}:{pcid}:{acid}"
    available_hours = await rd.smembers(index_key)
    
    current = start_time.replace(minute=0, second=0, microsecond=0)
    target_hours = []
    while current < end_time:
        hour_str = current.strftime('%Y%m%d%H')
        if hour_str in available_hours:
            target_hours.append(current)
        current += timedelta(hours=1)

    if not target_hours:
        return pd.DataFrame(), []

    def download_parquet(h):
        path = f"date={h.strftime('%Y-%m-%d')}/hour={h.strftime('%H')}/pcid={pcid}/acid={acid}/cache.parquet"
        try:
            response = s3.get_object("telemetry-cache", path)
            try:
                return pd.read_parquet(io.BytesIO(response.read()), engine='pyarrow')
            finally:
                response.close()
                response.release_conn()
        except:
            return None

    loop = asyncio.get_event_loop()
    dfs = await asyncio.gather(*[
        loop.run_in_executor(_executor, download_parquet, h) for h in target_hours
    ])
    
    final_dfs = [df for df in dfs if df is not None]
    if not final_dfs:
        return pd.DataFrame(), []
        
    return pd.concat(final_dfs), target_hours

async def _fetch_from_cache(s3, pcid, acid, start_time: datetime, end_time: datetime):
    """Legacy/Simple: Returns (records, found_slots)"""
    df, slots = await _fetch_from_cache_df(s3, pcid, acid, start_time, end_time)
    if df.empty:
        return [], []
    return df.to_dict('records'), slots

async def _export_first_task(device_ids: List[str], count: int = 2016):
    """Historical Task: Parallel Batch Engine (The Fastest Possible Strategy)."""
    with tracer.start_as_current_span("export_historical_task_streaming", attributes={"device_count": len(device_ids)}):
        t_start = time.monotonic()
        kafka_prod = await get_kafka()
        processed = 0
        log.info(f"🌊 [STREAM] Parallel Streaming Fetch for OLDEST {count} points for {len(device_ids)} devices...")
        
        DEVICES = CACHED_REGISTRY
        pool = await get_db_pool()
        batch_size = 200
        semaphore = asyncio.Semaphore(4)

        async def process_batch_historical(batch_ids):
            nonlocal processed
            async with pool.acquire() as conn:
                async with conn.transaction():
                    query = """
                        SELECT 
                            device_id,
                            metric_time, 
                            avg_watts, 
                            peak_watts, 
                            min_watts,
                            amb_temp,
                            cpu_avg_freq,
                            cpu_max,
                            cpu_pwr_sav_lim,
                            cpu_util,
                            cpu_watts,
                            gpu_watts,
                            status,
                            error_reason
                        FROM telemetry_live 
                        WHERE device_id = ANY($1)
                        ORDER BY device_id, metric_time ASC
                    """
                    cursor = conn.cursor(query, batch_ids)
                    
                    current_did = None
                    device_readings = []
                    
                    async for row in cursor:
                        did = row['device_id']
                        if current_did is None: current_did = did
                        
                        if did != current_did:
                            if device_readings:
                                await _process_and_send(current_did, device_readings[:count], DEVICES, kafka_prod)
                                processed += 1
                            current_did = did
                            device_readings = []
                        
                        if len(device_readings) < count:
                            device_readings.append({k: row[k] for k in METRIC_COLUMNS if k in row})

                    if current_did and device_readings:
                        await _process_and_send(current_did, device_readings[:count], DEVICES, kafka_prod)
                        processed += 1

        batches = [device_ids[i:i + batch_size] for i in range(0, len(device_ids), batch_size)]
        
        async def run_with_sem(b):
            async with semaphore:
                await process_batch_historical(b)
                await kafka_prod.flush()
        
        await asyncio.gather(*(run_with_sem(b) for b in batches))
        await kafka_prod.flush()

        t_total = time.monotonic() - t_start
        log.info(f"✅ [STREAM] Completed {processed} historical devices in {t_total:.2f}s")


async def _export_latest_task(device_ids: List[str], count: int = 2016):
    """
    Hybrid Latest Task:
    1. Calculates the required time window (e.g., 2016 points = 7 days).
    2. Fetches archived hours from MinIO Cache.
    3. Fetches the remaining 'fresh' points from TimescaleDB.
    """
    with tracer.start_as_current_span("export_latest_task_hybrid", attributes={"device_count": len(device_ids)}):
        t_start = time.monotonic()
        kafka_prod = await get_kafka()
        processed_records = 0
        
        DEVICES = CACHED_REGISTRY
        # Assuming 5-min intervals, calculate required lookback
        days_lookback = (count // 288) + 1
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_lookback)
        
        # 1. CACHE PATH (MinIO)
        pcid, acid = None, None
        if device_ids:
            first_meta = DEVICES.get(device_ids[0], {})
            pcid = first_meta.get('platform_customer_id')
            acid = first_meta.get('application_customer_id')

        found_slots = []
        df_cached = pd.DataFrame()
        if pcid and acid:
            s3 = get_minio()
            df_cached, found_slots = await _fetch_from_cache_df(s3, pcid, acid, start_time, end_time)

        # 2. HOT PATH (TimescaleDB)
        log.info(f"🔌 [HYBRID] Fetching DB tail and merging with {len(df_cached):,} cache records...")
        pool = await get_db_pool()
        db_start = max(found_slots) + timedelta(hours=1) if found_slots else start_time
        db_end = end_time
        
        async with pool.acquire() as conn:
            # Fetch EVERYTHING from DB for this hierarchy in one high-speed query
            rows = await conn.fetch(
                "SELECT * FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 AND metric_time >= $3 AND metric_time < $4",
                pcid, acid, db_start, db_end
            )
            df_db = pd.DataFrame(rows, columns=DB_COLUMNS)

        # 3. VECTORIZED STITCHING
        # Combine, Sort, and deduplicate in one burst
        df_full = pd.concat([df_cached, df_db])
        if df_full.empty:
            log.warning(f"⚠️ [LATEST] No data found for {pcid}:{acid}")
            async with HIERARCHY_LOCK:
                ACTIVE_HIERARCHIES.discard(f"{pcid}:{acid}")
            return

        # ── HYBRID OPTIMIZATION: ARROW HYDRATION ────────────────────────────
        # Instead of grouping in Pandas and sending 1000 separate tasks,
        # we convert to Arrow and send large device batches.
        
        # 1. Convert to Arrow Table (Preserving schema)
        full_table = pa.Table.from_pandas(df_full)
        
        # 2. Batch Delivery via ProcessPool
        # We process devices in batches of 50 to maximize CPU utilization
        unique_dids = pc.unique(full_table["device_id"]).to_pylist()
        DEVICE_BATCH_SIZE = 50
        cpu_pool = get_cpu_pool()
        
        async def process_batch(batch_dids):
            nonlocal processed_records
            # Filter table for this batch of devices
            mask = pc.is_in(full_table["device_id"], value_set=pa.array(batch_dids))
            
            # 🏎️ IPC OPTIMIZATION: Create a fresh table with only batch data 
            # to avoid accidental serialization of the parent table's shared buffers.
            raw_batch = full_table.filter(mask)
            batch_table = pa.Table.from_batches(raw_batch.to_batches())
            
            # Offload to CPU Pool
            loop = asyncio.get_running_loop()
            batch_meta = {did: DEVICES.get(did, {}) for did in batch_dids}
            
            # This is the "Hot Path": No pickling of large lists in main thread
            results = await loop.run_in_executor(
                cpu_pool, process_device_batch_hydration,
                batch_table, batch_meta, count
            )
            
            # Send results to Kafka
            if results:
                tasks = [kafka_prod.send(KAFKA_TOPIC, payload, key=did.encode()) for did, payload in results]
                await asyncio.gather(*tasks)
                processed_records += len(results)

        # 3. Stream Batches
        log.info(f"🌊 [ARROW] Delivering {len(unique_dids)} devices via vectorized hydration...")
        for i in range(0, len(unique_dids), DEVICE_BATCH_SIZE):
            await process_batch(unique_dids[i : i + DEVICE_BATCH_SIZE])
            if i % (DEVICE_BATCH_SIZE * 4) == 0:
                await kafka_prod.flush()

        await kafka_prod.flush()
        
        t_total = time.monotonic() - t_start
        log.info(f"✅ [HYBRID LATEST] Vector-Hydrated Export Complete | Devices: {processed_records:,} | Time: {t_total:.2f}s")

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
        # 1. Hot-Load Device IDs from Cache
        device_ids = [did for did, meta in CACHED_REGISTRY.items() 
                      if meta["platform_customer_id"] == pcid and meta["application_customer_id"] == acid]
        
        if not device_ids:
            return {"status": "Empty Hierarchy", "pcid": pcid, "acid": acid}
            
        # --- Hierarchical Lock Check ---
        h_key = f"{pcid}:{acid}"
        async with HIERARCHY_LOCK:
            if h_key in ACTIVE_HIERARCHIES:
                return {"status": "error", "message": f"Export already in progress for {h_key}"}
            ACTIVE_HIERARCHIES.add(h_key)

        background_tasks.add_task(_handle_throttled_export, _export_stream_task, h_key, start_time, end_time, pcid=pcid, acid=acid)
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
        background_tasks.add_task(_handle_throttled_export, _export_stream_task, m_key, start_time, end_time, pcid=pcid, acid=acid, device_ids=device_ids)
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
        # 1. Hot-Load Device IDs from Cache
        target_ids = [
            did for did, meta in CACHED_REGISTRY.items()
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

@app.post("/telemetry/manual-cache-refresh")
async def manual_cache_refresh(background_tasks: BackgroundTasks):
    """Manually trigger the hourly cache refresh job."""
    background_tasks.add_task(hourly_cache_job)
    return {"status": "Cache refresh triggered in background", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/telemetry/manual-archive")
async def manual_archival_trigger(background_tasks: BackgroundTasks):
    """Manually trigger the daily archival job."""
    background_tasks.add_task(daily_archival_job)
    return {"status": "Daily archival job triggered in background", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "registry_loaded": REGISTRY_LOADED,
        "device_count": len(CACHED_REGISTRY)
    }
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
