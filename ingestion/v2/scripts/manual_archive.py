import sys
import os
import asyncio
import time
from datetime import datetime, timezone, timedelta
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.commonconfig import CopySource
import asyncpg
import orjson
import logging
import gc

# Add parent path for schema_builder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from schema_builder import build_48_field_golden_record

# Configuration
TSDB_HOST = "127.0.0.1"
TSDB_PORT = "5432"
TSDB_USER = "postgres"
TSDB_PASS = "postgres"
TSDB_NAME = "postgres"
TS_CONN_STR = f"postgresql://{TSDB_USER}:{TSDB_PASS}@{TSDB_HOST}:{TSDB_PORT}/{TSDB_NAME}"

MINIO_HOST = "127.0.0.1:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"

REGISTRY_PATH = "/app/device_configs.json"

def _build_full_record(r, did, meta):
    if hasattr(r, 'keys'):
        reading = dict(r)
    else:
        reading = {"metric_time": r[0], "device_id": r[1]}
    return build_48_field_golden_record(
        device_id=did,
        reading=reading,
        device_metadata=meta,
        inventory_data=meta.get("inventory_data")
    )

async def manual_archival_push():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("manual-archive")
    
    t_total_start = time.monotonic()
    now = datetime.now(timezone.utc)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(days=7)
    
    log.info(f"🏗️ [PARQUET-STREAMING] 7-Day Archival: {start.strftime('%Y-%m-%d %H:%M')} → {end.strftime('%Y-%m-%d %H:%M')}")

    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    all_device_ids = list(DEVICES.keys())

    try:
        pool = await asyncpg.create_pool(TS_CONN_STR, min_size=5, max_size=10)
        s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        
        for bucket in ["telemetry-raw", "telemetry-archive"]:
            if not s3.bucket_exists(bucket):
                s3.make_bucket(bucket)

        # ARCHITECTURAL BALANCE:
        # We fetch in 100-device chunks (to keep RAM low)
        # But we write 10 chunks into 1 Parquet file (to solve the Small File Problem)
        MICRO_BATCH = 20 
        SILO_SIZE = 100 
        
        batch_counter = 0
        base_path = f"production/year={end.year}/month={end.month:02d}/day={end.day:02d}/full_7day/"
        
        log.info(f"🚀 Starting Streamed Archival (Goal: {len(all_device_ids)//SILO_SIZE + 1} Large Silos)...")
        
        for i in range(0, min(100, len(all_device_ids)), SILO_SIZE):
            silo_devices = all_device_ids[i:i + SILO_SIZE]
            pq_buf = io.BytesIO()
            writer = None
            silo_records_count = 0
            
            t_silo_start = time.monotonic()
            
            # Process this 1000-device silo in 100-device micro-chunks
            for j in range(0, len(silo_devices), MICRO_BATCH):
                micro_devices = silo_devices[j:j + MICRO_BATCH]
                
                async with pool.acquire() as conn:
                    records = await conn.fetch(
                        "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3) ORDER BY device_id, metric_time ASC", 
                        start, end, micro_devices
                    )
                
                if not records:
                    continue

                # Hydrate small chunk
                hydrated = [_build_full_record(r, r[1], DEVICES.get(r[1], {})) for r in records]
                del records
                
                # Convert to Arrow Table
                table = pa.Table.from_pandas(pd.DataFrame(hydrated))
                del hydrated
                
                # Initialize writer on first chunk with schema
                if writer is None:
                    writer = pq.ParquetWriter(pq_buf, table.schema, compression='snappy')
                
                writer.write_table(table)
                silo_records_count += len(table)
                del table
                gc.collect()

            if writer:
                writer.close()
                content = pq_buf.getvalue()
                
                # 1. Primary Upload to RAW
                fname = f"archive_silo_{batch_counter}.parquet"
                s3.put_object("telemetry-raw", base_path + fname, io.BytesIO(content), len(content))
                
                # 2. Server-Side Copy to ARCHIVE (Instant internal duplication)
                s3.copy_object(
                    "telemetry-archive", 
                    base_path + fname, 
                    CopySource("telemetry-raw", base_path + fname)
                )
                
                t_silo_elapsed = time.monotonic() - t_silo_start
                log.info(f"✅ Silo {batch_counter} Created: {silo_records_count:,} records in {len(content)/1024/1024:.2f} MB | Time: {t_silo_elapsed:.1f}s")
                
                batch_counter += 1
                del content, pq_buf
                gc.collect()

        await pool.close()
        log.info(f"🏁 STREAMED ARCHIVAL COMPLETE in {(time.monotonic()-t_total_start)/60:.2f} minutes")
        
    except Exception as e:
        log.error(f"💥 Archival Failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(manual_archival_push())
