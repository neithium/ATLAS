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
_executor = ThreadPoolExecutor(max_workers=64)
GLOBAL_IO_SEM = asyncio.Semaphore(100) # Limits total concurrent file reads across all exports
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
        # Hyper-scaling for 80k device fleet
        _cpu_pool_internal = ProcessPoolExecutor(max_workers=min(os.cpu_count() or 8, 20))
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
GLOBAL_THROTTLE_SEM = asyncio.Semaphore(8)  # Reduced to 8 to prevent IO/CPU death during burst

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
                
                # 🚀 DIRECT FS CACHE: Write raw Parquet to local directory
                local_dir = f"/app/telemetry-cache/{base_path}pcid={pcid}/acid={acid}"
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, "cache.parquet")
                
                with open(local_path, "wb") as f:
                    f.write(cache_content)
                
                # Also keep MinIO for long-term if needed, but the primary API cache is now local FS
                try:
                    cache_fname = f"{base_path}pcid={pcid}/acid={acid}/cache.parquet"
                    s3.put_object("telemetry-cache", cache_fname, io.BytesIO(cache_content), len(cache_content))
                except:
                    pass
                
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
    """Scheduled Task: Consolidation of last 24h data to Local RAW and ARCHIVE."""
    now = datetime.now(timezone.utc)
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=7)
    
    log.info(f"🕰️ [ARCHIVE] Consolidating 7-Day Sliding Window: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}...")
    
    try:
        # 1. Prepare Local & Hive Paths
        RAW_LOCAL = "/app/data/raw"
        ARCHIVE_LOCAL = "/app/data/archive"
        # Using a special 'full_7day' marker for the 7-day daily consolidation
        hive_path = f"production/year={end.year}/month={end.month:02d}/day={end.day:02d}/full_7day/"
        
        raw_dir = os.path.join(RAW_LOCAL, hive_path)
        archive_dir = os.path.join(ARCHIVE_LOCAL, hive_path)
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(archive_dir, exist_ok=True)

        with open(REGISTRY_PATH, "rb") as f:
            DEVICES_META = orjson.loads(f.read())
        
        all_device_ids = list(DEVICES_META.keys())
        pool = await get_db_pool()
        
        # 🏎️ LARGE SILO STRATEGY (Target: 128MB+)
        # 2,016 points/device/7-days -> ~1,000 devices per 128MB silo
        SILO_SIZE = 1000 
        MICRO_BATCH = 100
        total_records = 0
        
        for i in range(0, len(all_device_ids), SILO_SIZE):
            silo_devices = all_device_ids[i:i + SILO_SIZE]
            pq_buf = io.BytesIO()
            writer = None
            silo_records_count = 0
            
            # Process Silo in Micro-batches to keep RAM stable
            for j in range(0, len(silo_devices), MICRO_BATCH):
                micro_devices = silo_devices[j:j + MICRO_BATCH]
                async with pool.acquire() as conn:
                    records = await conn.fetch(
                        "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
                        start, end, micro_devices
                    )
                
                if not records: continue
                
                # 🚀 Group by Device and Hydrate using Unified Batch Builder
                from schema_builder import build_batch_power_detail
                
                # Group records by device_id
                from collections import defaultdict
                device_groups = defaultdict(list)
                for r in records:
                    device_groups[r['device_id']].append(dict(r))
                
                hydrated = []
                for did, raw_readings in device_groups.items():
                    meta = DEVICES_META.get(did, {})
                    
                    # 1. Build PowerDetail and aggregates (2016 points for 7 days)
                    pd_list, avg_v, max_v, min_v = build_batch_power_detail(raw_readings)
                    
                    # 2. Build 48-field record
                    payload = build_48_field_golden_record(
                        device_id=did,
                        reading=raw_readings[-1],
                        device_metadata=meta,
                        inventory_data=meta.get("inventory_data"),
                        power_detail_list=pd_list
                    )
                    
                    # 3. Inject computed aggregates
                    payload["data"]["Average"] = avg_v
                    payload["data"]["Maximum"] = max_v
                    payload["data"]["Minimum"] = min_v
                    
                    hydrated.append(payload)

                table = pa.Table.from_pylist(hydrated)
                
                if writer is None:
                    writer = pq.ParquetWriter(pq_buf, table.schema, compression='snappy')
                
                writer.write_table(table)
                silo_records_count += len(table)
                del records, hydrated, table
                gc.collect()
            
            if writer:
                writer.close()
                content = pq_buf.getvalue()
                fname = f"daily_silo_{i//SILO_SIZE}.parquet"
                
                # 🚀 LOCAL FS MIRRORING (Primary Storage)
                with open(os.path.join(raw_dir, fname), "wb") as f:
                    f.write(content)
                with open(os.path.join(archive_dir, fname), "wb") as f:
                    f.write(content)
                
                total_records += silo_records_count
                log.info(f"📦 [ARCHIVE] Silo {i//SILO_SIZE} Created: {silo_records_count:,} records | Size: {len(content)/1024/1024:.2f} MB")
                del content, pq_buf
                gc.collect()

        log.info(f"✅ [ARCHIVE] Daily Local Consolidation complete. Total records: {total_records:,}")

        log.info(f"✅ [ARCHIVE] Daily Local Consolidation complete. Total records: {total_records:,}")
        
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
    
    # 1. Sort ONCE per batch for consistent slicing
    # Sorting by device_id ASC and metric_time DESC (latest first)
    indices = pc.sort_indices(table, sort_keys=[("device_id", "ascending"), ("metric_time", "descending")])
    sorted_table = table.take(indices)
    
    # 🏎️ Vectorized String Conversion for JSON Compatibility
    if "metric_time" in sorted_table.column_names:
        ts_col = sorted_table["metric_time"]
        # Format as ISO string: 2026-05-03T07:27:31Z
        str_ts = pc.strftime(ts_col, format="%Y-%m-%dT%H:%M:%SZ")
        sorted_table = sorted_table.set_column(sorted_table.schema.get_field_index("metric_time"), "metric_time", str_ts)

    # 2. Identify Device Boundaries (O(N) vs O(N*M) with filters)
    dids = sorted_table["device_id"].to_pylist()
    results = []
    
    # Mapping for PascalCase Schema compliance (per input_schema.py)
    METRIC_MAP = {
        "amb_temp": "AmbTemp",
        "avg_watts": "Average",
        "cpu_avg_freq": "CpuAvgFreq",
        "cpu_max": "CpuMax",
        "cpu_pwr_sav_lim": "CpuPwrSavLim",
        "cpu_util": "CpuUtil",
        "cpu_watts": "CpuWatts",
        "gpu_watts": "GpuWatts",
        "min_watts": "Minimum",
        "peak_watts": "Peak",
        "metric_time": "Time"
    }
    
    # Slicing Logic
    n = len(dids)
    if n == 0: return []
    
    start_idx = 0
    while start_idx < n:
        current_did = dids[start_idx]
        # Find end of this device's block
        end_idx = start_idx + 1
        while end_idx < n and dids[end_idx] == current_did:
            end_idx += 1
        
        # Slice the table for this device
        device_table = sorted_table.slice(start_idx, end_idx - start_idx)
        
        # Take N latest points
        if count > 0 and device_table.num_rows > count:
            device_table = device_table.slice(0, count)
            
        # 🏎️ PascalCase Transformation & Aggregation
        raw_readings = device_table.to_pylist()
        
        # USE UNIFIED BUILDERS FOR CONSISTENCY
        from schema_builder import build_48_field_golden_record, build_batch_power_detail
        
        # 1. Build PowerDetail and compute aggregates using unified logic
        power_detail_list, avg_v, max_v, min_v = build_batch_power_detail(raw_readings)
        
        # 2. Build complete 48-field record
        meta = devices_meta.get(current_did, {})
        payload = build_48_field_golden_record(
            device_id=current_did,
            reading=raw_readings[-1] if raw_readings else {},
            device_metadata=meta,
            inventory_data=meta.get("inventory_data"),
            power_detail_list=power_detail_list
        )
        
        # 3. Ensure aggregates match the vectorized batch calculation
        payload["data"]["Average"] = avg_v
        payload["data"]["Maximum"] = max_v
        payload["data"]["Minimum"] = min_v
        
        results.append((current_did, orjson.dumps(payload)))
        start_idx = end_idx
        
    return results

def _serialize_record(did, readings, meta):
    """CPU-Bound: Runs in ProcessPool to bypass GIL."""
    import orjson
    from schema_builder import build_48_field_golden_record, build_batch_power_detail
    
    # 1. Build PowerDetail and compute aggregates using unified logic
    power_detail_list, avg_v, max_v, min_v = build_batch_power_detail(readings)
    
    # 2. Build complete 48-field record (Fixes missing metadata/inventory)
    payload = build_48_field_golden_record(
        device_id=did,
        reading=readings[-1] if readings else {},
        device_metadata=meta,
        inventory_data=meta.get("inventory_data"),
        power_detail_list=power_detail_list
    )
    
    # 3. Ensure aggregates match the batch calculation
    payload["data"]["Average"] = avg_v
    payload["data"]["Maximum"] = max_v
    payload["data"]["Minimum"] = min_v
    
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
        cached_records, found_slots = await _fetch_from_cache_df(pcid, acid, start_time, end_time)
        # Convert DF to list of dicts for legacy processing
        cached_records = cached_records.to_dict('records') if not cached_records.empty else []
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



async def _fetch_from_cache_arrow(pcid, acid, start_time: datetime, end_time: datetime):
    """Memory Optimized: Returns (Arrow Table, found_slots)"""
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
        return pa.table([]), []

    def read_local_parquet(h):
        # Local FS path (Directly written by hourly_cache_job)
        base_path = "/app/telemetry-cache"
        dir_path = os.path.join(base_path, f"date={h.strftime('%Y-%m-%d')}/hour={h.strftime('%H')}/pcid={pcid}/acid={acid}")
        
        if not os.path.exists(dir_path):
            return None
            
        try:
            import glob
            import pyarrow.parquet as pq
            files = glob.glob(os.path.join(dir_path, "*.parquet"))
            if not files:
                return None
            
            # Read all files and combine
            return pa.concat_tables([pq.read_table(f, memory_map=True) for f in files])
        except Exception as e:
            log.warning(f"⚠️ [CACHE] Failed to read local parquets in {dir_path}: {e}")
            return None

    async def throttled_read(h):
        async with GLOBAL_IO_SEM:
            return await loop.run_in_executor(_executor, read_local_parquet, h)

    # log.info(f"🔍 [CACHE] Fetching {len(target_hours)} hours from Local FS for {pcid}:{acid}...")
    loop = asyncio.get_event_loop()
    t_io_start = time.monotonic()
    tables = await asyncio.gather(*[throttled_read(h) for h in target_hours])
    t_io_end = time.monotonic()
    # log.info(f"📦 [CACHE] Read {len(target_hours)} Parquet files for {pcid}:{acid} in {t_io_end - t_io_start:.2f}s")
    
    # Filter target_hours to only include those where a table was actually found
    found_slots = [target_hours[i] for i, t in enumerate(tables) if t is not None]
    
    final_tables = [t for t in tables if t is not None]
    if not final_tables:
        return pa.table([]), []
        
    # 🎯 CONSISTENT SCHEMA ALIGNMENT (Prevents "Schema at index X was different")
    # We pick the schema from the first table as the "Master" (or a pre-defined one)
    # and strip virtual partition columns (date, hour, etc.)
    master_schema = None
    aligned_tables = []
    
    # Pre-defined expected columns to filter out phantom partition fields
    # Use dict.fromkeys to maintain order while ensuring uniqueness (prevents double metric_time)
    real_cols = list(dict.fromkeys(["metric_time", "device_id", "platform_customer_id", "application_customer_id"] + METRIC_COLUMNS))
    
    for t in final_tables:
        # Strip phantom columns (date, hour, etc.)
        existing_real_cols = [c for c in real_cols if c in t.column_names]
        t_clean = t.select(existing_real_cols)
        
        if master_schema is None:
            master_schema = t_clean.schema
            aligned_tables.append(t_clean)
        else:
            # Cast to master schema to handle null vs int64 and precision
            try:
                aligned_tables.append(t_clean.cast(master_schema))
            except:
                # If cast fails, just keep it and hope concat handles it (or it will fail gracefully later)
                aligned_tables.append(t_clean)

    return pa.concat_tables(aligned_tables), found_slots

async def _fetch_from_cache(s3, pcid, acid, start_time: datetime, end_time: datetime):
    """Legacy/Simple: Returns (records, found_slots)"""
    table, slots = await _fetch_from_cache_arrow(pcid, acid, start_time, end_time)
    if table.num_rows == 0:
        return [], []
    return table.to_pylist(), slots

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
        
        # 1. CACHE PATH (Local FS)
        pcid, acid = None, None
        if device_ids:
            first_meta = DEVICES.get(device_ids[0], {})
            pcid = first_meta.get('platform_customer_id')
            acid = first_meta.get('application_customer_id')

        found_slots = []
        table_cached = pa.table([])
        if pcid and acid:
            table_cached, found_slots = await _fetch_from_cache_arrow(pcid, acid, start_time, end_time)

        # 2. HOT PATH (TimescaleDB)
        log.info(f"🔌 [HYBRID] Fetching DB tail and merging with {table_cached.num_rows:,} cache records...")
        pool = await get_db_pool()
        # 2. TSDB GAP FETCH (Identify and fill missing hours)
        all_hours = []
        curr_h = start_time.replace(minute=0, second=0, microsecond=0)
        while curr_h <= end_time:
            all_hours.append(curr_h)
            curr_h += timedelta(hours=1)
            
        query_cols = list(set(["device_id", "metric_time", "platform_customer_id", "application_customer_id"] + METRIC_COLUMNS))
        missing_hours = sorted([h for h in all_hours if h not in found_slots])
        
        if not missing_hours:
            table_db = pa.table({k: [] for k in query_cols})
            log.info("📥 [HYBRID] No gaps found. TSDB fetch skipped.")
        else:
            # 🏎️ RANGE OPTIMIZATION: Group contiguous missing hours into start/end pairs
            ranges = []
            if missing_hours:
                start = missing_hours[0]
                prev = missing_hours[0]
                for i in range(1, len(missing_hours)):
                    if missing_hours[i] == prev + timedelta(hours=1):
                        prev = missing_hours[i]
                    else:
                        ranges.append((start, prev + timedelta(hours=1)))
                        start = missing_hours[i]
                        prev = missing_hours[i]
                ranges.append((start, prev + timedelta(hours=1)))
            
            log.info(f"📥 [HYBRID] Fetching {len(ranges)} gaps from TSDB (Total {len(missing_hours)} hours)...")
            
            # Construct multi-range query
            where_clauses = []
            params = [pcid, acid]
            for i, (r_start, r_end) in enumerate(ranges):
                p_idx = len(params) + 1
                where_clauses.append(f"(metric_time >= ${p_idx} AND metric_time < ${p_idx+1})")
                params.extend([r_start, r_end])
            
            range_sql = " OR ".join(where_clauses)
            query = f"SELECT {', '.join(query_cols)} FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 AND ({range_sql})"
            
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                
                if rows:
                    table_db = pa.Table.from_pylist([dict(r) for r in rows])
                    # Deduplicate in case of overlap
                    cached_times = set(table_cached["metric_time"].to_pylist()) if table_cached.num_rows > 0 else set()
                    if cached_times:
                        mask = [t not in cached_times for t in table_db["metric_time"].to_pylist()]
                        table_db = table_db.filter(pa.array(mask))
                    log.info(f"📥 [HYBRID] Fetched {table_db.num_rows:,} records from {len(ranges)} gaps")
                else:
                    table_db = pa.table({k: [] for k in query_cols})
                    log.info(f"📥 [HYBRID] TSDB gaps were EMPTY")

        # 3. VECTORIZED STITCHING (Pure Arrow)
        if table_cached.num_rows > 0 and table_db.num_rows > 0:
            # Step 1: Strip virtual partition columns from cache
            db_cols = set(table_db.column_names)
            strip_cols = [c for c in table_cached.column_names if c not in db_cols]
            if strip_cols:
                table_cached = table_cached.drop(strip_cols)
                log.info(f"🧹 [HYBRID] Stripped virtual columns from cache: {strip_cols}")

            # Step 2: Force DB table to match Cache schema exactly (order, ns vs us, etc.)
            try:
                # Reorder DB table columns to match Cache table exactly
                table_db = table_db.select(table_cached.column_names)
                # Now cast to match precision (ns vs us) and types
                table_db = table_db.cast(table_cached.schema)
            except Exception as cast_err:
                log.warning(f"⚠️ [HYBRID] Schema alignment failed: {cast_err}")
                # Final fallback attempt
                table_db = table_db.select(table_cached.column_names)

            full_table = pa.concat_tables([table_cached, table_db])
        elif table_cached.num_rows > 0:
            full_table = table_cached
        else:
            full_table = table_db

        if full_table.num_rows == 0:
            log.warning(f"⚠️ [LATEST] No data found for {pcid}:{acid}")
            async with HIERARCHY_LOCK:
                ACTIVE_HIERARCHIES.discard(f"{pcid}:{acid}")
            return

        # ── HYBRID OPTIMIZATION: ARROW HYDRATION ────────────────────────────
        unique_dids = pc.unique(full_table["device_id"]).to_pylist()
        DEVICE_BATCH_SIZE = 100  # 🎯 Reduced to avoid IPC/Pickle bottlenecks
        cpu_pool = get_cpu_pool()
        
        async def process_batch(batch_dids):
            nonlocal processed_records
            # Filter table for this batch of devices
            mask = pc.is_in(full_table["device_id"], value_set=pa.array(batch_dids))
            
            # 🏎️ IPC OPTIMIZATION: Create a fresh table with only batch data 
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
                # 🏎️ Throttled Send
                for did, payload in results:
                    await kafka_prod.send(KAFKA_TOPIC, payload, key=did.encode())
                processed_records += len(results)

        # 2. Stream Batches
        log.info(f"🌊 [ARROW] Delivering {len(unique_dids)} devices via vectorized hydration...")
        for i in range(0, len(unique_dids), DEVICE_BATCH_SIZE):
            await process_batch(unique_dids[i : i + DEVICE_BATCH_SIZE])
            # 🚦 Breath: Prevent Event Loop Saturation
            await asyncio.sleep(0.01)
            if i % (DEVICE_BATCH_SIZE * 5) == 0:
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
            new_config = device.dict()
            configs[device.device_id] = new_config
            
            # 4. Atomic Write
            with open(REGISTRY_PATH, "wb") as f:
                f.write(orjson.dumps(configs))

            # 🚀 DYNAMIC UPDATE: Refresh in-memory caches so API recognizes device immediately
            global CACHED_REGISTRY, REGISTRY_DF
            CACHED_REGISTRY[device.device_id] = new_config
            
            # Append to vectorized registry dataframe
            new_row = pd.DataFrame([{
                "device_id": device.device_id,
                "platform_customer_id": new_config.get("platform_customer_id"),
                "application_customer_id": new_config.get("application_customer_id"),
                "server_name": new_config.get("server_name"),
                "model": new_config.get("model")
            }])
            REGISTRY_DF = pd.concat([REGISTRY_DF, new_row], ignore_index=True)

            log.info(f"🆕 [REGISTRY] Device {device.device_id} registered successfully and hot-loaded.")
            return {"status": "success", "device_id": device.device_id, "message": "Device added to registry and hot-loaded"}
            
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
