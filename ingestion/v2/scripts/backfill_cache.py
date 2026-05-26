import os
import io
import time
import orjson
import pandas as pd
import asyncio
from datetime import datetime, timedelta, timezone
from minio import Minio
import asyncpg
import redis

# Config
TSDB_DSN = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
MINIO_HOST = "127.0.0.1:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"
# REGISTRY_PATH = "../../device_configs.json"
REGISTRY_PATH = "/app/device_configs.json"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_INDEX_PREFIX = "idx:telemetry"

DB_COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "server_name", "model", "processor_vendor", "server_generation",
    "report_type", "metric_type", "status", "error_reason", "tags",
    "location_id", "location_city", "location_state", "location_country", "location_name"
]

async def backfill():
    print("🚀 Starting Indexed Vectorized Cache Backfill (7 Days)...")
    
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    
    # Pre-build Registry DataFrame
    registry_list = []
    for did, meta in DEVICES.items():
        registry_list.append({
            "device_id": did,
            "platform_customer_id_reg": meta.get("platform_customer_id"),
            "application_customer_id_reg": meta.get("application_customer_id")
        })
    REGISTRY_DF = pd.DataFrame(registry_list)
    
    s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    rd = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    pool = await asyncpg.create_pool(TSDB_DSN)
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    current = start_date
    while current < now:
        hour_start = current
        hour_end = current + timedelta(hours=1)
        base_path = f"date={hour_start.strftime('%Y-%m-%d')}/hour={hour_start.strftime('%H')}/"
        
        print(f"🕰️  Indexing & Archiving {hour_start.strftime('%Y-%m-%d %H:00')}...")
        
        async with pool.acquire() as conn:
            records = await conn.fetch(
                "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2",
                hour_start, hour_end
            )
        
        if records:
            df_raw = pd.DataFrame(records, columns=DB_COLUMNS)
            df = df_raw.merge(REGISTRY_DF, on="device_id", how="left")
            df['platform_customer_id'] = df['platform_customer_id'].fillna(df['platform_customer_id_reg'])
            df['application_customer_id'] = df['application_customer_id'].fillna(df['application_customer_id_reg'])
            
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
                
                # 📝 Update Redis Index
                hour_key = hour_start.strftime('%Y%m%d%H')
                index_key = f"{REDIS_INDEX_PREFIX}:{pcid}:{acid}"
                rd.sadd(index_key, hour_key)
                
        current += timedelta(hours=1)

    print("✅ Indexed Vectorized Backfill Complete!")

if __name__ == "__main__":
    asyncio.run(backfill())
