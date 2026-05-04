import asyncio
from v2.api.api_v2 import load_registry
from datetime import datetime, timezone, timedelta
import os
import io
import orjson
import pandas as pd
import time

async def run():
    print("🚀 Ultra-Throttled Filling: 167 hours (6 days, 23 hrs)...")
    load_registry()
    
    from v2.api.api_v2 import get_db_pool, REGISTRY_PATH, DB_COLUMNS, REGISTRY_DF, REDIS_INDEX_PREFIX, get_redis
    
    cache_end = datetime(2026, 5, 1, 23, 0, 0, tzinfo=timezone.utc)
    
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    
    all_device_ids = list(DEVICES.keys())
    pool = await get_db_pool()
    rd = await get_redis()
    
    for h_offset in range(167, 0, -1):
        start = cache_end - timedelta(hours=h_offset)
        end = start + timedelta(hours=1)
        
        hour_key = start.strftime('%Y%m%d%H')
        base_path = f"date={start.strftime('%Y-%m-%d')}/hour={start.strftime('%H')}/"
        
        print(f"⏳ Processing hour: {start.strftime('%Y-%m-%d %H:00')}...")
        
        # Reduced batch size to 1000 for even lower spikes
        STREAM_BATCH = 1000
        total_hour_records = 0
        
        for i in range(0, len(all_device_ids), STREAM_BATCH):
            batch_devices = all_device_ids[i:i + STREAM_BATCH]
            async with pool.acquire() as conn:
                records = await conn.fetch(
                    "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
                    start, end, batch_devices
                )
            
            if not records: continue
            
            df_raw = pd.DataFrame(records, columns=DB_COLUMNS)
            df = df_raw.merge(
                REGISTRY_DF[['device_id', 'platform_customer_id', 'application_customer_id', 'server_name', 'model']], 
                on="device_id", how="left", suffixes=('', '_reg')
            )
            
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
                
                local_dir = f"/app/telemetry-cache/{base_path}pcid={pcid}/acid={acid}"
                os.makedirs(local_dir, exist_ok=True)
                local_path = os.path.join(local_dir, "cache.parquet")
                with open(local_path, "wb") as f:
                    f.write(cache_content)
                
                index_key = f"{REDIS_INDEX_PREFIX}:{pcid}:{acid}"
                await rd.sadd(index_key, hour_key)
                total_hour_records += len(group_df)
        
        print(f"✅ Hour {hour_key} complete. Sleeping 2s...")
        await asyncio.sleep(2.0) # 🛑 Ultra-Throttled for maximum stability

    print("🏁 Throttled population complete.")

if __name__ == "__main__":
    asyncio.run(run())
