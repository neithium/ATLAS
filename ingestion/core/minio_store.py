"""
core/minio_store.py
-------------------
Handles cold-storage archival of telemetry data to MinIO (S3-compatible).

Data structure in bucket:
  {device_id}/YYYY/MM/DD/{device_id}_HH.json
"""

import os
import json
import logging
import asyncio
from io import BytesIO
from datetime import datetime, timezone
import urllib3
from minio import Minio
from minio.error import S3Error

log = logging.getLogger(__name__)

# Configuration
MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
MINIO_PORT = os.getenv("MINIO_PORT", "9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "telemetry")

_client = None

def get_client():
    """Get the MinIO client (S3 protocol)."""
    global _client
    if _client is None:
        # Increase connection pool to handle parallel history fetches for 1600+ devices
        http_client = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=5, read=30),
            retries=urllib3.Retry(total=3, backoff_factor=0.2),
            maxsize=1000  # Increased to 1000 to match massive parallel S3 reads
        )
        
        _client = Minio(
            f"{MINIO_HOST}:{MINIO_PORT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
            http_client=http_client
        )
    return _client

async def ping() -> bool:
    """Check MinIO connectivity by listing buckets."""
    try:
        # Run synchronous Minio calls in a separate thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        client = get_client()
        await loop.run_in_executor(None, client.list_buckets)
        return True
    except Exception as e:
        log.error(f"[minio] Connectivity check failed: {e}")
        return False

async def ensure_bucket_exists():
    """Ensure the telemetry bucket exists on startup."""
    try:
        loop = asyncio.get_event_loop()
        client = get_client()
        
        exists = await loop.run_in_executor(None, client.bucket_exists, MINIO_BUCKET)
        if not exists:
            await loop.run_in_executor(None, client.make_bucket, MINIO_BUCKET)
            log.info(f"[minio] Created bucket: {MINIO_BUCKET}")
        else:
            log.debug(f"[minio] Bucket {MINIO_BUCKET} already exists.")
    except Exception as e:
        log.error(f"[minio] Failed to ensure bucket exists: {e}")

async def save_reading_batch(device_id: str, readings: list[dict]) -> bool:
    """
    Save a batch of 12 readings (1 hour) to MinIO.
    Path: {device_id}/YYYY/MM/DD/{device_id}_HH.json
    """
    if not readings:
        return False

    path = "unknown"
    try:
        client = get_client()
        
        # Determine path from the first reading's timestamp
        # Format: 2026-03-20T06:00:00Z
        first_time = readings[0]["Time"]
        dt = datetime.fromisoformat(first_time.replace("Z", "+00:00"))
        
        path = f"{device_id}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{device_id}_{dt.hour:02d}.json"
        
        # Serialize to JSON bytes
        data = json.dumps(readings).encode('utf-8')
        data_len = len(data)
        
        # Upload using put_object
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: client.put_object(
                MINIO_BUCKET,
                path,
                BytesIO(data),
                data_len,
                content_type="application/json"
            )
        )
        return True
    except Exception as e:
        log.error(f"[minio] Failed to archive {device_id} to {path}: {e}")
        return False

async def get_history(device_id: str, last_n: int = 2016) -> list[dict]:
    """
    Fetch historical readings from MinIO.
    Scans the last 7 days of hourly files and returns combined list.
    """
    try:
        client = get_client()
        loop = asyncio.get_event_loop()
        
        # 1. List all hourly files for this device
        objects = await loop.run_in_executor(
            None,
            lambda: list(client.list_objects(MINIO_BUCKET, prefix=f"{device_id}/", recursive=True))
        )
        
        if not objects:
            return []
            
        # 2. Sort by name (chronological) and take last 168 (7 days * 24h)
        objects.sort(key=lambda x: x.object_name)
        recent_objects = objects[-168:]
        
        # 3. Fetch files in parallel
        async def fetch_one(obj_name: str):
            try:
                resp = await loop.run_in_executor(None, lambda: client.get_object(MINIO_BUCKET, obj_name))
                try:
                    data = await loop.run_in_executor(None, resp.read)
                    return json.loads(data)
                finally:
                    await loop.run_in_executor(None, resp.close)
                    await loop.run_in_executor(None, resp.release_conn)
            except Exception as e:
                log.warning(f"[minio] Failed to fetch {obj_name}: {e}")
                return []

        # Use limited semaphore for parallel S3 reads
        semaphore = asyncio.Semaphore(50)
        async def fetch_with_sem(obj_name: str):
            async with semaphore:
                return await fetch_one(obj_name)
                
        results = await asyncio.gather(*[fetch_with_sem(obj.object_name) for obj in recent_objects])
        
        # 4. Flatten and sort
        all_readings = []
        for batch in results:
            all_readings.extend(batch)
            
        all_readings.sort(key=lambda r: r.get("Time", ""))
        return all_readings[-last_n:]
        
    except Exception as e:
        log.error(f"[minio] Failed to get history for {device_id}: {e}")
        return []
