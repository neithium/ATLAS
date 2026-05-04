import sys
import os
import asyncio
import time
from datetime import datetime, timezone, timedelta
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import asyncpg
import orjson
import logging
import gc

# Adjust path for V2/V3 structure
sys.path.append("/app")
from schema_builder import build_48_field_golden_record, build_batch_power_detail

# =============================================================================
# INFRASTRUCTURE CONFIGURATION
# =============================================================================
TSDB_HOST = "127.0.0.1"
TSDB_PORT = "5432"
TSDB_USER = "postgres"
TSDB_PASS = "postgres"
TSDB_NAME = "postgres"
TS_CONN_STR = f"postgresql://{TSDB_USER}:{TSDB_PASS}@{TSDB_HOST}:{TSDB_PORT}/{TSDB_NAME}"

REGISTRY_PATH = "/app/device_configs.json"
RAW_LOCAL = "/app/data/raw"
ARCHIVE_LOCAL = "/app/data/archive"

# =============================================================================
# CORE ARCHIVAL LOGIC
# =============================================================================
async def run_time_sliced_archive():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("archiver")
    
    # Target Window: May 3rd (Known good data)
    target_day = datetime(2026, 5, 3, tzinfo=timezone.utc)
    start = target_day.replace(hour=23, minute=0, second=0)
    end = start + timedelta(hours=1)
    
    log.info(f"⏳ [SLICE] Starting 1-hour archival: {start.isoformat()} to {end.isoformat()}")

    # 1. Load Device Metadata for Hydration
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    all_device_ids = list(DEVICES.keys())

    try:
        # 2. Database Connectivity
        pool = await asyncpg.create_pool(TS_CONN_STR, min_size=5, max_size=10)
        
        # 🏎️ LARGE SILO STRATEGY (Target: 128MB+)
        # For 1-hour slices (12 points/device), we need ~20,000+ devices per silo to reach 128MB
        SILO_SIZE = 20000 
        MICRO_BATCH = 2000
        
        hive_path = f"production/year={start.year}/month={start.month:02d}/day={start.day:02d}/hour={start.hour:02d}/"
        raw_dir = os.path.join(RAW_LOCAL, hive_path)
        archive_dir = os.path.join(ARCHIVE_LOCAL, hive_path)
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(archive_dir, exist_ok=True)

        log.info(f"🚀 Starting Streamed Slice Archival (Devices: {len(all_device_ids)})...")
        
        for i in range(0, len(all_device_ids), SILO_SIZE):
            silo_devices = all_device_ids[i:i + SILO_SIZE]
            pq_buf = io.BytesIO()
            writer = None
            silo_records_count = 0
            
            t_silo_start = time.monotonic()
            
            for j in range(0, len(silo_devices), MICRO_BATCH):
                micro_devices = silo_devices[j:j + MICRO_BATCH]
                async with pool.acquire() as conn:
                    records = await conn.fetch(
                        "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3) ORDER BY device_id, metric_time ASC", 
                        start, end, micro_devices
                    )
                
                if not records: continue

                # Group by Device and Hydrate using Unified Builder
                from collections import defaultdict
                device_groups = defaultdict(list)
                for r in records:
                    device_groups[r['device_id']].append(dict(r))
                
                hydrated = []
                for did, raw_readings in device_groups.items():
                    meta = DEVICES.get(did, {})
                    pd_list, avg_v, max_v, min_v = build_batch_power_detail(raw_readings)
                    
                    payload = build_48_field_golden_record(
                        device_id=did,
                        reading=raw_readings[-1],
                        device_metadata=meta,
                        inventory_data=meta.get("inventory_data"),
                        power_detail_list=pd_list
                    )
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
                fname = f"slice_silo_{i//SILO_SIZE}.parquet"
                
                # A. LOCAL MIRRORING
                with open(os.path.join(raw_dir, fname), "wb") as f:
                    f.write(content)
                with open(os.path.join(archive_dir, fname), "wb") as f:
                    f.write(content)
                
                t_silo_elapsed = time.monotonic() - t_silo_start
                log.info(f"✅ Silo {i//SILO_SIZE} Created: {silo_records_count:,} devices | Size: {len(content)/1024/1024:.2f} MB | Time: {t_silo_elapsed:.1f}s")
                del content, pq_buf
                gc.collect()

        await pool.close()
        log.info(f"🏁 [DONE] Hourly Migration Finished.")
        
    except Exception as e:
        log.error(f"💥 [ERROR] Archival Failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_time_sliced_archive())
