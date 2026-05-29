import os
import io
import time
import orjson
import pandas as pd
import asyncio
from datetime import datetime, timedelta, timezone
import asyncpg
import redis

# Config
TSDB_DSN = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
def get_registry_path():
    paths = [
        os.path.join(os.getcwd(), "device_configs.json"),
        "/app/device_configs.json",
        os.path.join(os.path.dirname(__file__), "../../device_configs.json")
    ]
    for p in paths:
        if os.path.exists(p): return p
    raise FileNotFoundError("Could not find device_configs.json")

REGISTRY_PATH = get_registry_path()
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_INDEX_PREFIX = "idx:telemetry"

DB_COLUMNS = [
    "metric_time", "device_id", "platform_customer_id", "application_customer_id",
    "amb_temp", "avg_watts", "cpu_avg_freq", "cpu_max", "cpu_pwr_sav_lim",
    "cpu_util", "cpu_watts", "gpu_watts", "min_watts", "peak_watts",
    "status", "error_reason"
]

async def backfill(days=7, offset_hours=0):
    print(f"[BACKFILL] Starting Indexed Vectorized Cache Backfill ({days} Days, Offset: {offset_hours}h)...")
    
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
    
    rd = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    pool = await asyncpg.create_pool(TSDB_DSN)
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate end point with offset
    end_date = now - timedelta(hours=offset_hours)
    
    current = start_date
    while current < end_date:
        hour_start = current
        hour_end = current + timedelta(hours=1)
        base_path = f"date={hour_start.strftime('%Y-%m-%d')}/hour={hour_start.strftime('%H')}/"
        
        print(f"[INDEXING] Indexing & Archiving {hour_start.strftime('%Y-%m-%d %H:00')}...")
        
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
                
                #Mirror to Local FS (Crucial for Local-First API)
                local_path = os.path.join("/app/telemetry-cache", cache_fname)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(cache_content)
                
                #Update Redis Index
                hour_key = hour_start.strftime('%Y%m%d%H')
                index_key = f"{REDIS_INDEX_PREFIX}:{pcid}:{acid}"
                rd.sadd(index_key, hour_key)
                
        current += timedelta(hours=1)

    print("[BACKFILL] Indexed Vectorized Backfill Complete!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--offset-hours", type=int, default=0)
    args = parser.parse_args()
    
    asyncio.run(backfill(days=args.days, offset_hours=args.offset_hours))
