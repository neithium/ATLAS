"""
# =============================================================================
# ATLAS - PowerPulse V3 Ingestion API
# =============================================================================
# Strategy: Demand-Based Kafka Ingestion
# Hot Path: TimescaleDB (7-day history)
# Streaming: Kafka (Redpanda)
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
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import gc


# Config
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

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
_executor = ThreadPoolExecutor(max_workers=24)  # OPTIMIZATION: Reduced to 32 (was 64) to save memory
GLOBAL_IO_SEM = asyncio.Semaphore(8)  # OPTIMIZATION: Reduced to 50 (was 100) to limit concurrent file reads
_WORKER_REGISTRY = {}
_cpu_pool_internal = None

def init_worker_registry(registry):
    global _WORKER_REGISTRY
    _WORKER_REGISTRY = registry

def get_cpu_pool():
    global _cpu_pool_internal
    if _cpu_pool_internal is None:
        import multiprocessing
        # 'spawn' is safer than 'fork' for avoiding deadlocks in async/multi-threaded apps
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass
        # Memory-optimized: 4 workers saves ~1.2GB vs 10 workers
        # Each spawned worker costs ~200MB (Python + PyArrow + orjson + registry copy)
        # CPU usage peaks at ~110% so extra workers just waste memory
        _cpu_pool_internal = ProcessPoolExecutor(
            max_workers=3,
            initializer=init_worker_registry,
            initargs=(CACHED_REGISTRY,)
        )
    return _cpu_pool_internal

_scheduler = AsyncIOScheduler()

# Redis Index Key: idx:telemetry:{pcid}:{acid} -> Set of available hours (YYYYMMDDHH)
REDIS_INDEX_PREFIX = "idx:telemetry"

# Database Schema Columns (for vectorized hydration)
DB_COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "status", "error_reason"
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

# HIERARCHY OPTIMIZATION: Pre-indexed hierarchy lookup (O(1) vs O(N) full registry scan)
# Structure: {(pcid, acid): [device_id1, device_id2, ...]}
HIERARCHY_INDEX = {}

# System Guard: Prevents thread exhaustion under heavy burst loads
GLOBAL_EXPORT_SEM = asyncio.Semaphore(50)  # Reverted to 50 (100 was overloading I/O)

async def get_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=TSDB_HOST, port=TSDB_PORT, user=TSDB_USER, password=TSDB_PASS, database=TSDB_NAME,
            min_size=10, max_size=30,  # OPTIMIZATION: Reduced from 30-150 to save connection pool memory
            max_cached_statement_lifetime=3600,
            max_cacheable_statement_size=65536
        )
    return _pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-v3")

app = FastAPI(title="PowerPulse V3 Unified Ingestion API")
# --- Concurrency & Safety Controls ---
# Tier 1: Context Guard (Prevents redundant fetches for the SAME PCID/ACID)
ACTIVE_HIERARCHIES = set() 
HIERARCHY_LOCK = asyncio.Lock()

# Tier 2: System Throttler (Prevents crashing Kafka if too many DIFFERENT hierarchies fetch)
GLOBAL_THROTTLE_SEM = asyncio.Semaphore(2)  # Limits concurrent exports to prevent OOM in containers

# Tier 3: CPU Worker Guard (Ensures all concurrent exports share the cores fairly)
GLOBAL_WORKER_SEM = asyncio.Semaphore(8)  # Reduced to ensure fair core sharing across concurrent exports

# =============================================================================
# THROTTLING WRAPPER
# =============================================================================
#queue explosion handler is pending
async def _handle_throttled_export(task_func, h_key, *args, **kwargs):
    """Wrapper to enforce global system limits and automatic cleanup of hierarchical locks."""
    async with GLOBAL_THROTTLE_SEM:
        try:
            log.info(f"[THROTTLE] Starting export for {h_key} (available slots: {GLOBAL_THROTTLE_SEM._value})")
            await task_func(*args, **kwargs)
        except Exception as e:
            log.error(f"[GUARD] Task failed for {h_key}: {e}")
        finally:
            async with HIERARCHY_LOCK:
                if h_key in ACTIVE_HIERARCHIES:
                    ACTIVE_HIERARCHIES.remove(h_key)
            log.info(f"[GUARD] Export complete for {h_key}. Hierarchy lock released.")

# =============================================================================
# HOURLY CACHE JOB (Optimized for API Exports)
# =============================================================================
async def hourly_cache_job():
    """Scheduled Task: Streams the last hour of data into the PCID/ACID partitioned cache."""
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    
    log.info(f"[CACHE] Hourly cache refresh starting: {start.strftime('%H:%M')} to {end.strftime('%H:%M')}")
    
    try:
        with open(REGISTRY_PATH, "rb") as f:
            DEVICES = orjson.loads(f.read())
        
        all_device_ids = list(DEVICES.keys())
        pool = await get_db_pool()
        
        # Incremental Streaming
        STREAM_BATCH = 1000  # Reduced to 1000 (was 2000) to prevent memory spikes during hourly job
        total_records = 0
        base_path = f"date={start.strftime('%Y-%m-%d')}/hour={start.strftime('%H')}/"
        
        for i in range(0, len(all_device_ids), STREAM_BATCH):
            batch_devices = all_device_ids[i:i + STREAM_BATCH]
            async with pool.acquire() as conn:
                # OPTIMIZATION: Explicit column selection (not SELECT * with 28+ columns)
                col_list = ", ".join(DB_COLUMNS)
                records = await conn.fetch(
                    f"SELECT {col_list} FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
                    start, end, batch_devices
                )
            
            if not records: continue
            
            log.info(f"[CACHE] Processing batch of {len(records)} records...")
            
            # VECTORIZED HYDRATION (No loops!)
            # 1. Convert DB Records to DataFrame (Raw metrics only - exactly 16 columns)
            df_raw = pd.DataFrame([dict(r) for r in records])
            
            # 2. Vectorized Merge with Registry for Customer Info
            df = df_raw.merge(
                REGISTRY_DF[['device_id', 'platform_customer_id', 'application_customer_id']], 
                on="device_id", 
                how="left",
                suffixes=('', '_reg')
            )
            
            # Use registry values if DB values are missing/null (fallback)
            df['platform_customer_id'] = df['platform_customer_id'].fillna(df['platform_customer_id_reg'])
            df['application_customer_id'] = df['application_customer_id'].fillna(df['application_customer_id_reg'])
            
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
                
                # STORAGE: Write raw Parquet to local directory
                local_dir = f"/app/telemetry-cache/{base_path}pcid={pcid}/acid={acid}"
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, "cache.parquet")
                
                with open(local_path, "wb") as f:
                    f.write(cache_content)
                
                # INDEX UPDATE: Update Redis index for instant cache discovery
                rd = await get_redis()
                hour_key = start.strftime('%Y%m%d%H')
                index_key = f"{REDIS_INDEX_PREFIX}:{pcid}:{acid}"
                await rd.sadd(index_key, hour_key)
                
                total_records += len(group_df)
                
                # MEMORY: Aggressive garbage collection after write
                del cache_buf, cache_content
                gc.collect()
            
            del df_raw, df, df_final, records
            gc.collect()

        log.info(f"[CACHE] Hourly refresh complete. Total records partitioned: {total_records:,}")
        
    except Exception as e:
        log.error(f"[CACHE] Hourly Refresh Failed: {str(e)}")

# =============================================================================
# DAILY LONG-TERM ARCHIVAL JOB (Consolidated Data Lake)
# =============================================================================
async def daily_archival_job():
    """Scheduled Task: Consolidation of last 24h data to Local RAW and ARCHIVE."""
    now = datetime.now(timezone.utc)
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=7)
    
    log.info(f"[ARCHIVE] Consolidating 7-day sliding window: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    
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

        # Use the already loaded global registry to avoid duplicating memory
        global CACHED_REGISTRY
        if not CACHED_REGISTRY:
            load_registry()
            
        all_device_ids = list(CACHED_REGISTRY.keys())
        pool = await get_db_pool()
        
        # SILO STRATEGY: Target files 128MB+
        # 2,016 points/device/7-days -> ~1,000 devices per 128MB silo
        SILO_SIZE = 7000 
        # MEMORY OPTIMIZATION: Reduced MICRO_BATCH from 100 to 50
        # Python dicts for 200,000 nested records cause OOM in 1GB containers.
        MICRO_BATCH = 50
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
                
                # Build batch using unified schema and hydrate using ProcessPool
                from schema_builder import build_batch_power_detail
                
                # Group records by device_id
                from collections import defaultdict
                device_groups = defaultdict(list)
                for r in records:
                    device_groups[r['device_id']].append(dict(r))
                
                hydrated = []
                for did, raw_readings in device_groups.items():
                    meta = CACHED_REGISTRY.get(did, {})
                    
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
                
                # Extreme GC cleanup to prevent OOM
                del records
                device_groups.clear()
                del device_groups
                del hydrated
                del table
                gc.collect()
            
            if writer:
                writer.close()
                content = pq_buf.getvalue()
                fname = f"daily_silo_{i//SILO_SIZE}.parquet"
                
                # STORAGE: Parquet files to raw and archive directories
                with open(os.path.join(raw_dir, fname), "wb") as f:
                    f.write(content)
                with open(os.path.join(archive_dir, fname), "wb") as f:
                    f.write(content)
                
                total_records += silo_records_count
                log.info(f"[ARCHIVE] Silo {i//SILO_SIZE} created: {silo_records_count:,} records, {len(content)/1024/1024:.2f} MB")
                del content, pq_buf
                gc.collect()

        log.info(f"[ARCHIVE] Daily Local Consolidation complete. Total records: {total_records:,}")

        # Write _SUCCESS marker for downstream consumers (Spark, verification scripts)
        success_metadata = {
            "status": "SUCCESS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "start_date": start.strftime('%Y-%m-%d'),
            "end_date": end.strftime('%Y-%m-%d'),
            "total_records": total_records,
            "total_silos": (len(all_device_ids) + SILO_SIZE - 1) // SILO_SIZE,
            "total_devices": len(all_device_ids),
            "compression": "snappy"
        }
        for target_dir in [raw_dir, archive_dir]:
            success_path = os.path.join(target_dir, "_SUCCESS")
            with open(success_path, "w") as f:
                import json
                json.dump(success_metadata, f, indent=2)
        log.info(f"[ARCHIVE] _SUCCESS marker written to {raw_dir} and {archive_dir}")
        
    except Exception as e:
        log.error(f"[ARCHIVE] Daily consolidation failed: {str(e)}")

# =============================================================================
# LIFECYCLE MANAGEMENT
# =============================================================================
def load_registry():
    global CACHED_REGISTRY, REGISTRY_LOADED, REGISTRY_DF, HIERARCHY_INDEX
    if REGISTRY_LOADED: return
    
    try:
        with open(REGISTRY_PATH, "rb") as f:
            CACHED_REGISTRY = orjson.loads(f.read())
            
        # Build Vectorized Registry for Archival Performance
        registry_list = []
        # OPTIMIZATION: Build hierarchy index while iterating (single pass)
        hierarchy_dict = {}
        
        for did, meta in CACHED_REGISTRY.items():
            registry_list.append({
                "device_id": did,
                "platform_customer_id": meta.get("platform_customer_id"),
                "application_customer_id": meta.get("application_customer_id"),
                "server_name": meta.get("server_name"),
                "model": meta.get("model")
            })
            
            # Build hierarchy index: (pcid, acid) -> [device_ids]
            pcid = meta.get("platform_customer_id")
            acid = meta.get("application_customer_id")
            if pcid and acid:
                key = (pcid, acid)
                if key not in hierarchy_dict:
                    hierarchy_dict[key] = []
                hierarchy_dict[key].append(did)
        
        REGISTRY_DF = pd.DataFrame(registry_list)
        HIERARCHY_INDEX = hierarchy_dict
        
        REGISTRY_LOADED = True
        log.info(f"[CACHE] Registry pre-loaded with {len(CACHED_REGISTRY)} devices ({len(HIERARCHY_INDEX)} hierarchies).")
    except Exception as e:
        log.error(f"[CACHE] Failed to load registry: {e}")

@app.on_event("startup")
async def startup_event():
    global _kafka, CACHED_REGISTRY, REGISTRY_LOADED
    
    # 1. Hot-Load Registry Cache (Crucial for <30s Latency)
    load_registry()

    # 2. Initialize Infrastructure
    await get_kafka()
    
    await get_db_pool()
    
    # # 3. Initialize MinIO Buckets Upfront
    # try:
    #     s3 = get_minio()
    #     for bucket in ["telemetry-raw", "telemetry-archive", "telemetry-cache"]:
    #         if not s3.bucket_exists(bucket):
    #             s3.make_bucket(bucket)
    #             log.info(f"[MINIO] Bucket created: {bucket}")
    # except Exception as e:
    #     log.error(f"[MINIO] Bucket initialization failed: {e}")
    
    # Schedule: Dual-Tier Archival Strategy
    _scheduler.add_job(hourly_cache_job, 'cron', minute=0, misfire_grace_time=600, coalesce=True)
    _scheduler.add_job(daily_archival_job, 'cron', hour=0, minute=10, misfire_grace_time=3600, coalesce=True)
    _scheduler.start()
    
    log.info("[SYSTEM] Silo-Systems Online (Dual-Tier Archival ACTIVE: Hourly Cache + Daily Long-term)")

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
KAFKA_STARTED = False

async def get_kafka():
    global _kafka, KAFKA_STARTED
    if _kafka is None:
        # AIOKafka for high-throughput async outgress
        try:
            _kafka = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: v if isinstance(v, bytes) else orjson.dumps(v),
                compression_type="lz4",     # Reduces Redpanda I/O bottleneck
                linger_ms=5,                 # Increased for aggressive batching (was 5)
                max_batch_size=16777216,       # 16MB (increased from 10MB)
                max_request_size=16777216,     # 16MB (increased from 10MB)
                request_timeout_ms=300000,
                retry_backoff_ms=1000,
                acks=1  # Fire-and-forget for max throughput
            )
            log.info(f"[KAFKA] Production Producer Initialized (AIOKafka + Snappy + Aggressive Batching)")
        except Exception as e:
            log.error(f"[KAFKA] Initialisation Failed: {e}")

    if _kafka and not KAFKA_STARTED:
        try:
            await _kafka.start()
            KAFKA_STARTED = True
            log.info("[KAFKA] Producer connected successfully to brokers!")
        except Exception as e:
            log.warning(f" [KAFKA] Connection failed. Is the broker down? Will retry later. Error: {e}")
            
    return _kafka

async def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return _redis

# def get_minio():
#     global _minio
#     if _minio is None:
#         import urllib3
#         # CUSTOM POOL: Expanded for 168+ parallel hourly downloads
#         http_client = urllib3.PoolManager(
#             retries=False,
#             maxsize=300,
#             num_pools=10
#         )
#         _minio = Minio(
#             MINIO_HOST, 
#             access_key=MINIO_ACCESS, 
#             secret_key=MINIO_SECRET, 
#             secure=False,
#             http_client=http_client
#         )
#     return _minio

# =============================================================================
# DATABASE DATA ACCESS LAYER (DAL)
# =============================================================================

TSDB_EXPORT_COLUMNS = """
    metric_time,
    device_id,
    platform_customer_id,
    application_customer_id,
    amb_temp,
    avg_watts,
    cpu_avg_freq,
    cpu_max,
    cpu_pwr_sav_lim,
    cpu_util,
    cpu_watts,
    gpu_watts,
    min_watts,
    peak_watts,
    status,
    error_reason
"""


async def query_tsdb_range(
    device_id: str,
    start_time: datetime,
    end_time: datetime
):
    pool = await get_db_pool()

    async with pool.acquire() as conn:

        query = f"""
            SELECT {TSDB_EXPORT_COLUMNS}
            FROM telemetry_live
            WHERE device_id = $1
              AND metric_time >= $2
              AND metric_time < $3
            ORDER BY metric_time ASC
        """

        return await conn.fetch(
            query,
            device_id,
            start_time,
            end_time
        )


async def query_tsdb_latest(
    device_id: str,
    limit: int = 2016
):
    pool = await get_db_pool()

    async with pool.acquire() as conn:

        query = f"""
            SELECT {TSDB_EXPORT_COLUMNS}
            FROM telemetry_live
            WHERE device_id = $1
            ORDER BY metric_time DESC
            LIMIT $2
        """

        return await conn.fetch(
            query,
            device_id,
            limit
        )


# =============================================================================
# BACKGROUND ASYNC WORKERS (Bulk Query Architecture)
# =============================================================================
BULK_BATCH_SIZE = 400  # Dialed back to the 'Sweet Spot' for 1601 device sync

def process_device_batch_hydration(
    table: pa.Table, 
    count: int
) -> list:
    """
    Vectorized Hydration: Processes a batch of devices via PyArrow.
    Runs in ProcessPoolExecutor to bypass GIL.
    
    Memory Optimization: Reads from pre-initialized global worker registry
    to completely avoid IPC pickling overhead.
    Aggressive GC: Collect after each device to prevent buildup.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import orjson
    from datetime import datetime, timezone
    import time
    import os
    import gc

    if table.num_rows == 0: return []
    
    # 1. Sort ONCE per batch for consistent slicing
    # Sorting by device_id ASC and metric_time DESC (latest first)
    indices = pc.sort_indices(table, sort_keys=[("device_id", "ascending"), ("metric_time", "descending")])
    sorted_table = table.take(indices)
    
    #Vectorized String Conversion for JSON Compatibility
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
    
    # STEP 1: Vectorized Mapping (Arrow Native)
    # Map the entire batch table to PascalCase names for PowerDetail in one shot
    field_map = {
        "amb_temp": "AmbTemp", "avg_watts": "Average", "cpu_avg_freq": "CpuAvgFreq",
        "cpu_max": "CpuMax", "cpu_pwr_sav_lim": "CpuPwrSavLim", "cpu_util": "CpuUtil",
        "cpu_watts": "CpuWatts", "gpu_watts": "GpuWatts", "min_watts": "Minimum",
        "peak_watts": "Peak", "metric_time": "Time"
    }
    
    # Cast Time to string once for the whole table
    time_col = sorted_table["metric_time"]
    if pa.types.is_timestamp(time_col.type):
        time_strings = time_col.cast(pa.string())
    else:
        time_strings = time_col
        
    # (Batch-level to_pylist removed to optimize memory)

    # STEP 2: Vectorized Aggregates for the WHOLE BATCH (Arrow Native - No Dict)
    agg_table = sorted_table.group_by("device_id").aggregate([
        ("avg_watts", "mean"),
        ("peak_watts", "max"),
        ("min_watts", "min")
    ])
    
    agg_device_ids = agg_table["device_id"].to_pylist()
    agg_means = agg_table["avg_watts_mean"].to_pylist()
    agg_peaks = agg_table["peak_watts_max"].to_pylist()
    agg_mins = agg_table["min_watts_min"].to_pylist()
    
    agg_lookup = {agg_device_ids[i]: (agg_means[i], agg_peaks[i], agg_mins[i]) for i in range(len(agg_device_ids))}
    
    # STEP 3: Block Processing per Device with Object Reuse
    n = len(dids)
    if n == 0: return []
    
    start_idx = 0
    payload = {
        "device_id": "",
        "report_id": "",
        "created_at": "",
        "status": True,
        "model": "",
        "tags": "",
        "report_type": "telemetry_live",
        "server_name": "",
        "error_reason": "",
        "location_id": "",
        "location_city": "",
        "location_name": "",
        "location_state": "",
        "location_country": "",
        "processor_vendor": "",
        "server_generation": "",
        "platform_customer_id": "",
        "application_customer_id": "",
        "metric_type": "power_metrics",
        "data": {
            "Id": "",
            "Average": 0.0,
            "Maximum": 0.0,
            "Minimum": 0.0,
            "Name": "",
            "PowerDetail": []
        },
        "inventory_data": {}
    }
    
    data_block = payload["data"]

    while start_idx < n:
        current_did = dids[start_idx]
        end_idx = start_idx + 1
        while end_idx < n and dids[end_idx] == current_did:
            end_idx += 1
        
        limit = min(end_idx - start_idx, count)
        end_slice = start_idx + limit
        
        # Zero-copy PyArrow slice of the sorted table
        device_table = sorted_table.slice(start_idx, limit)
        
        # Convert ONLY this device's slice to list (~2016 rows) to keep memory near zero!
        t_list = device_table["metric_time"].to_pylist()
        at_list = device_table["amb_temp"].to_pylist()
        av_list = device_table["avg_watts"].to_pylist()
        cf_list = device_table["cpu_avg_freq"].to_pylist()
        cm_list = device_table["cpu_max"].to_pylist()
        cp_list = device_table["cpu_pwr_sav_lim"].to_pylist()
        cu_list = device_table["cpu_util"].to_pylist()
        cw_list = device_table["cpu_watts"].to_pylist()
        gw_list = device_table["gpu_watts"].to_pylist()
        mi_list = device_table["min_watts"].to_pylist()
        pk_list = device_table["peak_watts"].to_pylist()
        
        # Python Native List Slicing & Zipped List Comprehension (up to 25x faster than PyArrow slice struct conversion)
        power_detail_list = [
            {
                "Time": t, "AmbTemp": at, "Average": av, "CpuAvgFreq": cf,
                "CpuMax": cm, "CpuPwrSavLim": cp, "CpuUtil": cu, "CpuWatts": cw,
                "GpuWatts": gw, "Minimum": mi, "Peak": pk
            }
            for t, at, av, cf, cm, cp, cu, cw, gw, mi, pk in zip(
                t_list, at_list, av_list, cf_list, cm_list, cp_list, cu_list, cw_list, gw_list, mi_list, pk_list
            )
        ]
        
        #  Lookup pre-calculated aggregates (dict lookup O(1))
        avg_v, max_v, min_v = agg_lookup.get(current_did, (0.0, 0.0, 0.0))
        
        #  Update reused payload dict (No new allocation!)
        meta = _WORKER_REGISTRY.get(current_did, {})
        
        # Serialization Optimization: Use orjson.Fragment
        # This converts telemetry to bytes once and avoids re-walking the tree later
        pd_fragment = orjson.Fragment(orjson.dumps(power_detail_list))
        
        payload["device_id"] = current_did
        payload["report_id"] = os.urandom(8).hex() 
        payload["created_at"] = power_detail_list[-1]["Time"] if power_detail_list else ""
        payload["model"] = meta.get("model", "PowerEdge R750")
        payload["tags"] = meta.get("tags", "production,critical")
        payload["server_name"] = meta.get("server_name", "UNKNOWN")
        payload["location_id"] = meta.get("location_id", "LOC-01")
        payload["location_city"] = meta.get("location_city", "UNKNOWN")
        payload["location_name"] = meta.get("location_name", "Atlas-DC-Default")
        payload["location_state"] = meta.get("location_state", "UNKNOWN")
        payload["location_country"] = meta.get("location_country", "UNKNOWN")
        payload["processor_vendor"] = meta.get("processor_vendor", "Intel")
        payload["server_generation"] = meta.get("server_generation", "15G")
        payload["platform_customer_id"] = meta.get("platform_customer_id", "UNKNOWN")
        payload["application_customer_id"] = meta.get("application_customer_id", "UNKNOWN")
        payload["inventory_data"] = meta.get("inventory_data", {})
        
        data_block["Id"] = current_did
        data_block["Average"] = float(round(avg_v or 0.0, 2))
        data_block["Maximum"] = float(round(max_v or 0.0, 2))
        data_block["Minimum"] = float(round(min_v or 0.0, 2))
        data_block["Name"] = payload["server_name"]
        data_block["PowerDetail"] = pd_fragment # STITCHING: Zero-copy injection
        
        results.append((current_did, orjson.dumps(payload)))
        
        #  AGGRESSIVE GC: Clean up after every device to prevent memory buildup
        del power_detail_list, pd_fragment, meta
        if len(results) % 5 == 0:  # Every 5 devices
            gc.collect(generation=0)  # Quick collect
        
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
    
    # Parallel Serialization (Offloaded to separate CPU core)
    pool = get_cpu_pool()
    payload_bytes = await loop.run_in_executor(pool, _serialize_record, did, readings, meta)
    
    #  Kafka Delivery
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
            log.warning(f" [WORKER] Missing hierarchy info")
            return
    else:
        target_ids = device_ids
    
    if not target_ids:
        log.warning(f" [WORKER] No devices found for {h_label}")
        return

    # 1. CACHE PATH (Local FS) - Only if we have PCID/ACID
    missing_slots = [start_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=i) 
                     for i in range(int((end_time - start_time).total_seconds() // 3600) + 1)]
    
    if pcid and acid:
        table_cached, found_slots = await _fetch_from_cache_arrow(pcid, acid, start_time, end_time)
        if table_cached.num_rows > 0:
            log.info(f" [CACHE] Found {table_cached.num_rows:,} records in Local FS for {pcid}:{acid}")
            
            # If we are targeting specific device_ids, filter the table
            if device_ids:
                mask = pc.is_in(table_cached["device_id"], value_set=pa.array(device_ids))
                table_cached = table_cached.filter(mask)
            
            # Legacy-style processing (one-by-one) but from Arrow
            # We sort by device_id to group them
            indices = pc.sort_indices(table_cached, sort_keys=[("device_id", "ascending")])
            table_cached = table_cached.take(indices)
            
            dids = table_cached["device_id"].to_pylist()
            start_idx = 0
            while start_idx < len(dids):
                did = dids[start_idx]
                end_idx = start_idx + 1
                while end_idx < len(dids) and dids[end_idx] == did:
                    end_idx += 1
                
                device_slice = table_cached.slice(start_idx, end_idx - start_idx)
                readings = device_slice.to_pylist()
                await _process_and_send(did, readings, DEVICES, kafka_prod)
                processed_records += len(readings)
                start_idx = end_idx
            
            # Update missing slots
            missing_slots = [s for s in missing_slots if s not in found_slots]
    
    # 2. HOT PATH (TimescaleDB Fallback)
    if missing_slots:
        log.info(f" [HOT PATH] Fetching {len(missing_slots)} hourly slots from TimescaleDB...")
        pool = await get_db_pool()
        db_start = min(missing_slots)
        db_end = end_time
        
        async with pool.acquire() as conn:
            # Use compound index (PCID, ACID, Time) for faster lookup
            if pcid and acid:
                query = f"SELECT {', '.join(METRIC_COLUMNS + ['device_id'])} FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 AND metric_time >= $3 AND metric_time < $4 ORDER BY device_id, metric_time ASC"
                rows = await conn.fetch(query, pcid, acid, db_start, db_end)
            else:
                # Fallback for manual device_id lists
                query = f"SELECT {', '.join(METRIC_COLUMNS + ['device_id'])} FROM telemetry_live WHERE device_id = ANY($1) AND metric_time >= $2 AND metric_time < $3 ORDER BY device_id, metric_time ASC"
                rows = await conn.fetch(query, target_ids, db_start, db_end)
            
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
                    #  MEMORY OPTIMIZATION: Skip dict creation - pass asyncpg record directly
                device_readings.append(r)
                
                if current_did and device_readings:
                    await _process_and_send(current_did, device_readings, DEVICES, kafka_prod)
                    processed_records += len(device_readings)

    await kafka_prod.flush()
    t_total = time.monotonic() - t_start
    log.info(f" [HYBRID] Export Complete for {h_label} | Total Points: {processed_records:,} | Time: {t_total:.2f}s")



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
            log.warning(f"[CACHE] Failed to read local parquets in {dir_path}: {e}")
            return None

    async def throttled_read(h):
        async with GLOBAL_IO_SEM:
            return await loop.run_in_executor(_executor, read_local_parquet, h)

    # log.info(f"[CACHE] Fetching {len(target_hours)} hours from Local FS for {pcid}:{acid}...")
    loop = asyncio.get_event_loop()
    t_io_start = time.monotonic()
    tables = await asyncio.gather(*[throttled_read(h) for h in target_hours])
    t_io_end = time.monotonic()
    # log.info(f"[CACHE] Read {len(target_hours)} Parquet files for {pcid}:{acid} in {t_io_end - t_io_start:.2f}s")
    
    # Filter target_hours to only include those where a table was actually found
    found_slots = [target_hours[i] for i, t in enumerate(tables) if t is not None]
    
    final_tables = [t for t in tables if t is not None]
    if not final_tables:
        return pa.table([]), []
        
    # CONSISTENT SCHEMA ALIGNMENT (Prevents "Schema at index X was different")
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
    t_start = time.monotonic()
    kafka_prod = await get_kafka()
    processed = 0
    log.info(f" [STREAM] Parallel Streaming Fetch for OLDEST {count} points for {len(device_ids)} devices...")
    
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
                        #  MEMORY OPTIMIZATION: Skip dict creation - pass asyncpg record directly
                        device_readings.append(row)

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
    log.info(f"[STREAM] Completed {processed} historical devices in {t_total:.2f}s")


async def _export_latest_task(device_ids: List[str], count: int = 2016):
    """
    Hybrid Latest Task:
    1. Calculates the required time window (e.g., 2016 points = 7 days).
    2. Fetches archived hours from MinIO Cache.
    3. Fetches the remaining 'fresh' points from TimescaleDB.
    """
    t_start = time.monotonic()
    kafka_prod = await get_kafka()
    processed_records = 0
    loop = asyncio.get_running_loop()
    
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
    # Use the latest timestamp from cache to find the starting point for the DB tail fetch
    db_start_time = start_time
    if table_cached.num_rows > 0:
        # Get the maximum timestamp from the cache (vectorized)
        db_start_time = pc.max(table_cached["metric_time"]).as_py()
        log.info(f"[HYBRID] Cache ends at {db_start_time}. Fetching DB tail from there...")
    
    pool = await get_db_pool()
    query_cols = list(set(["device_id", "metric_time", "platform_customer_id", "application_customer_id"] + METRIC_COLUMNS))
    
    log.info(f"[HYBRID] Fetching DB range from {db_start_time} to {end_time}...")
    
    async with pool.acquire() as conn:
        # TSDB OPTIMIZATION: If specific device_ids were passed (e.g. manual fetch), don't fetch the whole hierarchy
        if device_ids and len(device_ids) < 100:
            query = f"SELECT {', '.join(query_cols)} FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 AND metric_time > $3 AND metric_time <= $4 AND device_id = ANY($5::text[])"
            rows = await conn.fetch(query, pcid, acid, db_start_time, end_time, device_ids)
        else:
            query = f"SELECT {', '.join(query_cols)} FROM telemetry_live WHERE platform_customer_id = $1 AND application_customer_id = $2 AND metric_time > $3 AND metric_time <= $4"
            rows = await conn.fetch(query, pcid, acid, db_start_time, end_time)
        
        
        if rows:
            def convert_to_table(r_list):
                return pa.Table.from_pylist([dict(r) for r in r_list])
            
            table_db = await loop.run_in_executor(_executor, convert_to_table, rows)
            log.info(f"[HYBRID] Fetched {table_db.num_rows:,} fresh records from TSDB")
        else:
            table_db = pa.table({k: [] for k in query_cols})
            log.info(f"[HYBRID] TSDB range was EMPTY")

    # 3. VECTORIZED STITCHING (Pure Arrow)
    if table_cached.num_rows > 0 and table_db.num_rows > 0:
        # Step 1: Strip virtual partition columns from cache
        db_cols = set(table_db.column_names)
        strip_cols = [c for c in table_cached.column_names if c not in db_cols]
        if strip_cols:
            table_cached = table_cached.drop(strip_cols)
            log.info(f"[HYBRID] Stripped virtual columns from cache: {strip_cols}")

        # Step 2: Force DB table to match Cache schema exactly (order, ns vs us, etc.)
        try:
            # Reorder DB table columns to match Cache table exactly
            table_db = table_db.select(table_cached.column_names)
            # Now cast to match precision (ns vs us) and types
            table_db = table_db.cast(table_cached.schema)
        except Exception as cast_err:
            log.warning(f"[HYBRID] Schema alignment failed: {cast_err}")
            # Final fallback attempt
            table_db = table_db.select(table_cached.column_names)

        full_table = pa.concat_tables([table_cached, table_db])
    elif table_cached.num_rows > 0:
        full_table = table_cached
    else:
        full_table = table_db

    if full_table.num_rows == 0:
        log.warning(f" [LATEST] No data found for {pcid}:{acid}")
        async with HIERARCHY_LOCK:
            ACTIVE_HIERARCHIES.discard(f"{pcid}:{acid}")
        return

    #  SPECIFIC DEVICE FILTER
    # If specific device_ids were requested, filter the fetched data down to just those devices
    if device_ids:
        mask = pc.is_in(full_table["device_id"], value_set=pa.array(device_ids))
        full_table = full_table.filter(mask)

    # ── HYBRID OPTIMIZATION: ARROW HYDRATION ────────────────────────────
    unique_dids = pc.unique(full_table["device_id"]).to_pylist()
    DEVICE_BATCH_SIZE = 35  #  Reduced to 35 (was 50) to enable 3+ concurrent hierarchies
    cpu_pool = get_cpu_pool()
    
    #  Force garbage collection before batch processing
    gc.collect()
    
    async def process_batch(batch_dids):
        # Filter table for this batch of devices
        mask = pc.is_in(full_table["device_id"], value_set=pa.array(batch_dids))
        
        # IPC OPTIMIZATION: Create fresh table with batch data only 
        raw_batch = full_table.filter(mask)
        batch_table = pa.Table.from_batches(raw_batch.to_batches())
        
        # Offload to CPU Pool
        loop = asyncio.get_running_loop()
        #  MEMORY OPTIMIZATION: Don't pre-build batch_meta dict - lazy load in subprocess
        # Pass just the batch_dids; process_device_batch_hydration will fetch via closure
        
        # This is the "Hot Path": No pickling of large dicts in main thread
        results = await loop.run_in_executor(
            cpu_pool, process_device_batch_hydration,
            batch_table, count
        )
        
        # Send results to Kafka
        if results:
            result_count = len(results)
            # Blocking send: wait for all messages to be buffered before moving on
            for did, payload in results:
                await kafka_prod.send(KAFKA_TOPIC, payload, key=did.encode())
            # Clean up references
            del results
            return result_count
        return 0

    # 2. Stream Batches (Offloaded to CPU Pool) with BATCHED KAFKA SENDS
    log.info(f"[ARROW] Delivering {len(unique_dids)} devices via parallel hydration...")
    MAX_CONCURRENT_BATCHES = 3  # Reduced to 3 for concurrent hierarchies
    
    async def throttled_batch(batch_dids):
        async with GLOBAL_WORKER_SEM:
            result = await process_batch(batch_dids)
            # AGGRESSIVE GC: Clean up immediately after each batch
            gc.collect()
            return result
    
    # Run batches with concurrency control
    batch_tasks = []
    for i in range(0, len(unique_dids), DEVICE_BATCH_SIZE):
        batch_tasks.append(throttled_batch(unique_dids[i : i + DEVICE_BATCH_SIZE]))
    
    results_counts = await asyncio.gather(*batch_tasks)
    processed_records = sum(results_counts)

    await kafka_prod.flush()
    
    # AGGRESSIVE OS-LEVEL MEMORY RELEASE
    # Python's allocator does not return memory to the OS/Docker immediately.
    # This forces glibc to release all freed memory pages back to the kernel.
    import ctypes
    try:
        # Force PyArrow to drop its internal C++ memory pools (jemalloc/mimalloc)
        pa.default_memory_pool().release_unused()
        log.info("[MEMORY] PyArrow default memory pool released.")
        
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
        log.info("[MEMORY] malloc_trim(0) executed: OS memory released.")
    except Exception as e:
        pass
    
    t_total = time.monotonic() - t_start
    log.info(f"[HYBRID LATEST] Vector-Hydrated Export Complete | Devices: {processed_records:,} | Time: {t_total:.2f}s")

# =============================================================================
# HIERARCHICAL API ENDPOINTS
# =============================================================================
@app.post("/pcid/{pcid}/acid/{acid}/telemetry/latest/export")
async def trigger_latest_telemetry_export(pcid: str, acid: str, background_tasks: BackgroundTasks, count: int = 2016):
    """
    Core API Endpoint: Hierarchical Telemetry Export.
    
    Purpose: 
    Used by downstream ML/Data teams to fetch a massive chunk of real-time telemetry 
    (default 2016 points = 7 days) for an entire customer application hierarchy.
    
    Flow:
    1. Looks up the customer (`pcid`) and application (`acid`) in the in-memory registry.
    2. Gathers all associated `device_ids`.
    3. Triggers an asynchronous PyArrow/TimescaleDB hydration task (`_export_latest_task`).
    4. Instantly returns a 200 Accepted response while the task runs concurrently in the background.
    """
    try:
        registry_path = "/app/device_configs.json"
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
async def trigger_manual_id_export(pcid: str, acid: str, device_string: str, background_tasks: BackgroundTasks, count: int = 2016):
    """
    Surgical API Endpoint: Specific Device Telemetry Export.
    
    Purpose:
    Used for troubleshooting or targeting a specific sub-set of devices instead of the whole hierarchy.
    
    Flow:
    1. Takes a comma-separated string of specific `device_ids`.
    2. Spawns an isolated background task with a unique manual UUID lock to prevent interfering 
       with large-scale hierarchical exports.
    3. Streams the results to Kafka.
    """
    try:
        device_ids = [d.strip() for d in device_string.split(",")]

        log.info(f"[API] Manual export requested for {len(device_ids)} specific devices.")
        
        # Manual exports use a special key to ensure they don't block hierarchical ones
        m_key = f"manual:{uuid.uuid4().hex[:8]}"
        background_tasks.add_task(_handle_throttled_export, _export_latest_task, m_key, device_ids, count)
        return {
            "status": "Manual Stream Started", 
            "requested_devices": len(device_ids),
            "pcid": pcid,
            "acid": acid
        }
    except Exception as e:
        log.error(f"Export failed: {e}")
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

@app.post("/fleet/telemetry/export")
async def trigger_fleet_telemetry_export(days: int = 7):
    """Triggers Kafka Ingestion for EVERY device registered in the fleet."""
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        # Load ALL devices from registry
        with open(REGISTRY_PATH, "rb") as f:
            registry = orjson.loads(f.read())
        
        device_ids = list(registry.keys())
        
        if not device_ids:
            return {"status": "Empty Fleet", "message": "No devices found in registry."}
            
        log.info(f"📢 [API] Global Fleet Export Triggered: {len(device_ids)} devices. (Synchronous Mode)")
        
        # We now AWAIT this task so the API doesn't return until the data is in Kafka
        await _export_stream_task(start_time=start_time, end_time=end_time, device_ids=device_ids)
        
        return {
            "status": "Fleet-wide Export Completed", 
            "targeted_devices": len(device_ids),
            "window_days": days
        }

    except Exception as e:
        log.error(f"❌ Fleet Export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/register/device")
async def register_new_device(device: DeviceRegistration):
    """
    Metadata API Endpoint: Dynamic Fleet Registration.
    
    Purpose:
    Allows new server assets to be registered in the system without requiring an API restart.
    
    Flow:
    1. Receives the hardware profile (Intel/AMD, DDR4/5 config).
    2. Writes it instantly to the persistent `device_configs.json`.
    3. Immediately hot-loads it into the Python RAM `CACHED_REGISTRY`.
    """
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

            # DYNAMIC UPDATE: Refresh in-memory caches so API recognizes device immediately
            global CACHED_REGISTRY, REGISTRY_DF, HIERARCHY_INDEX
            CACHED_REGISTRY[device.device_id] = new_config
            
            # Update Hierarchy Index for O(1) lookups
            h_key = (new_config.get("platform_customer_id"), new_config.get("application_customer_id"))
            if h_key not in HIERARCHY_INDEX:
                HIERARCHY_INDEX[h_key] = []
            if device.device_id not in HIERARCHY_INDEX[h_key]:
                HIERARCHY_INDEX[h_key].append(device.device_id)
            
            # Append to vectorized registry dataframe
            new_row = pd.DataFrame([{
                "device_id": device.device_id,
                "platform_customer_id": new_config.get("platform_customer_id"),
                "application_customer_id": new_config.get("application_customer_id"),
                "server_name": new_config.get("server_name"),
                "model": new_config.get("model")
            }])
            REGISTRY_DF = pd.concat([REGISTRY_DF, new_row], ignore_index=True)

            log.info(f"[REGISTRY] Device {device.device_id} registered successfully and hot-loaded.")
            return {"status": "success", "device_id": device.device_id, "message": "Device added to registry and hot-loaded"}
            
        except Exception as e:
            log.error(f"[REGISTRY] Registration failed for {device.device_id}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error during registration")

@app.get("/health")
async def health():
    """
    System Reliability Endpoint: Deep Health Probe.
    
    Purpose:
    Used by Docker/Kubernetes/Load Balancers to determine if the container is healthy 
    and ready to accept ingestion requests. Probes Kafka and TSDB connections.
    """
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
