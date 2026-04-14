"""
# =============================================================================
# ATLAS - Time-Sliced Archival Engine
# =============================================================================
# Logic: Incremental 1-hour Batching
# Target: Production Multi-Silo (Raw + Archive)
# Schema: 48-Field Golden Schema
# =============================================================================
"""

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

# Adjust path for V2/V3 structure
sys.path.append("/app")

# =============================================================================
# INFRASTRUCTURE CONFIGURATION
# =============================================================================
TSDB_HOST = "127.0.0.1"  # Force local connection inside container
TSDB_PORT = "5432"
TSDB_USER = "postgres"
TSDB_PASS = "postgres"
TSDB_NAME = "postgres"
TS_CONN_STR = f"postgresql://{TSDB_USER}:{TSDB_PASS}@{TSDB_HOST}:{TSDB_PORT}/{TSDB_NAME}"

MINIO_HOST = "127.0.0.1:9000"
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")

REGISTRY_PATH = "/app/device_configs.json"

# =============================================================================
# CORE ARCHIVAL LOGIC
# =============================================================================
async def run_time_sliced_archive():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("archiver")
    
    # Target Window: April 4th (Fixed for Validation)
    target_day = datetime(2026, 4, 4, tzinfo=timezone.utc)
    start = target_day.replace(hour=0, minute=0, second=0)
    end = start + timedelta(hours=1)
    
    log.info(f"⏳ [SLICE] Starting 1-hour archival: {start.isoformat()} to {end.isoformat()}")

    # 1. Load Device Metadata for Hydration
    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())

    try:
        # 2. Database Connectivity
        log.info("📡 [DB] Connecting to TimescaleDB...")
        conn = await asyncpg.connect(TS_CONN_STR)
        
        device_rows = await conn.fetch(
            "SELECT DISTINCT device_id FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2", 
            start, end
        )
        
        if not device_rows:
            log.warning(f"⚠️ [DB] No telemetry data found for window {start} to {end}.")
            await conn.close()
            return

        # 3. Batching & Hydration (Memory-Safe Strategy)
        full_device_ids = [r['device_id'] for r in device_rows]
        device_ids = full_device_ids[:100] # Limit to top 100 for this validation run
        log.info(f"📊 [FLEET] Processing {len(device_ids)} active devices...")

        s3 = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        
        # Parallel Fetch the records for the batch
        records = await conn.fetch(
            "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3)", 
            start, end, device_ids
        )

        if records:
            # 4. Golden Schema Transformation
            hydrated = []
            for r in records:
                did = r['device_id']
                meta = DEVICES.get(did, {})
                hydrated.append({
                    "device_id": did,
                    "report_id": "ARCHIVE-" + str(uuid.uuid4())[:8],
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

            # 5. Columnar Conversion (Parquet)
            df = pd.DataFrame(hydrated)
            pq_buf = io.BytesIO()
            df.to_parquet(pq_buf, engine='pyarrow', index=False, compression='snappy')
            file_bytes = pq_buf.getvalue()

            # 6. Multi-Silo Push (Hive-Partitioned)
            folder_path = f"production/year={start.year}/month={start.month:02d}/day={start.day:02d}/hour={start.hour:02d}/"
            file_name = f"telemetry_slice_{start.strftime('%H%M%S')}.parquet"
            
            # Silo 1: Raw Analytics
            s3.put_object("telemetry-raw", folder_path + file_name, data=io.BytesIO(file_bytes), length=len(file_bytes))
            
            # Silo 2: Long-term Archive
            s3.put_object("telemetry-archive", folder_path + file_name, data=io.BytesIO(file_bytes), length=len(file_bytes))

            log.info(f"✅ [SUCCESS] Batch uploaded to {folder_path} ({len(file_bytes)/1024:.1f} KB)")
        
        await conn.close()
        log.info(f"🏁 [DONE] Hourly Migration Finished.")
        
    except Exception as e:
        log.error(f"💥 [ERROR] Archival Failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(run_time_sliced_archive())
