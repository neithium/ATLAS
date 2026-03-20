"""
core/history_service.py
-----------------------
Orchestrates multi-layer data retrieval (Redis + MinIO).
Provides a unified view of the last 7 days (2016 readings).
"""

import asyncio
import logging
from typing import Optional
from config.devices import TOTAL_READINGS, REDIS_READINGS
from . import redis_store
try:
    from . import minio_store
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

log = logging.getLogger(__name__)

async def get_combined_history(device_id: str, last_n: int = TOTAL_READINGS) -> list[dict]:
    """
    Get unified history for one device:
    - Recent data from Redis (1 day)
    - Historical data from MinIO (6 days)
    """
    # 1. Fetch from Redis (newest)
    redis_data = await redis_store.get_history(device_id, last_n=REDIS_READINGS)
    
    # 2. Fetch from MinIO (oldest) if needed
    minio_data = []
    if MINIO_AVAILABLE and len(redis_data) < last_n:
        minio_result = await minio_store.get_history(device_id, last_n=last_n)
        if isinstance(minio_result, list):
            minio_data = minio_result
    
    # 3. Merge and deduplicate
    combined = list(minio_data) + list(redis_data)
    
    seen_times = set()
    merged = []
    # Traverse reversed to prioritize newer readings (Redis) over older (MinIO) in case of overlaps
    for r in reversed(combined):
        t = r.get("Time")
        if t and t not in seen_times:
            seen_times.add(t)
            merged.append(r)
            
    merged.sort(key=lambda x: x.get("Time", ""))
    result = merged[-last_n:]
    return result

async def get_combined_history_batch(device_ids: list[str], last_n: int = TOTAL_READINGS) -> dict[str, list[dict]]:
    """
    Fetch combined history for multiple devices in parallel.
    Returns {device_id: list[readings]}
    """
    semaphore = asyncio.Semaphore(20) # Limit concurrency for S3/Redis protection
    
    async def fetch_one(did: str):
        async with semaphore:
            return (did, await get_combined_history(did, last_n=last_n))
            
    results = await asyncio.gather(*[fetch_one(did) for did in device_ids])
    return {did: readings for did, readings in results}
