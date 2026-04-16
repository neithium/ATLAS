import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta
import io
import pandas as pd
import uuid
from minio import Minio
import asyncpg
import orjson
import logging

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

async def manual_archival_push():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("manual-archive")
    
    # 🎯 TARGET WINDOW: Last 60 Minutes
    now = datetime.now(timezone.utc)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(hours=1)
    
    log.info(f"🏗️ [MANUAL] Archiving recent data window: {start.isoformat()} to {end.isoformat()}...")

    # Load Registry
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())

    try:
        conn = await asyncpg.connect(TS_CONN_STR)
        device_rows = await conn.fetch(
            "SELECT DISTINCT device_id FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2", 
            start, end
        )
        
        if not device_rows:
            log.error("❌ No data found in TSDB for this window!")
            await conn.close()
            return

        device_ids = [r['device_id'] for r in device_rows]
        log.info(f"📊 Found {len(device_ids)} devices. Starting push...")

        s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        
        # Process first 50 devices for this manual "Success Sample"
        batch_slice = device_ids[:50]
        records = await conn.fetch(
            "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
            start, end, batch_slice
        )

        hydrated = []
        for r in records:
            did = r['device_id']
            meta = DEVICES.get(did, {})
            hydrated.append({
                "device_id": did,
                "report_id": "MANUAL-" + str(uuid.uuid4())[:8],
                "created_at": r['metric_time'].isoformat(),
                "status": True,
                "model": meta.get('model', 'PowerEdge R750'),
                "data": {
                    "Id": did,
                    "Average": r.get('avg_watts', 0),
                    "PowerDetail": [{"Time": r['metric_time'].isoformat(), "CpuUtil": r.get('cpu_util', 0)}]
                },
                "inventory_data": {"cpu_count": 2}
            })

        df = pd.DataFrame(hydrated)
        pq_buf = io.BytesIO()
        df.to_parquet(pq_buf, engine='pyarrow', index=False, compression='snappy')
        file_bytes = pq_buf.getvalue()

        # UPLOAD TO HIVE PATH
        root_path = f"production/year={start.year}/month={start.month:02d}/day={start.day:02d}/hour={start.hour:02d}/"
        file_name = "manual_sample_50dev.parquet"
        
        # Dual-Silo Write
        for bucket in ["telemetry-raw", "telemetry-archive"]:
            try:
                s3.put_object(bucket, root_path + file_name, io.BytesIO(file_bytes), len(file_bytes))
            except:
                if not s3.bucket_exists(bucket):
                    s3.make_bucket(bucket)
                s3.put_object(bucket, root_path + file_name, io.BytesIO(file_bytes), len(file_bytes))

        log.info(f"✅ MANUAL PUSH SUCCESSFUL! Check MinIO: {root_path}")
        await conn.close()
        
    except Exception as e:
        log.error(f"💥 Manual Push Failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(manual_archival_push())
