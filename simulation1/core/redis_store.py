"""
core/redis_store.py
-------------------
All Redis interactions for the power monitor ring buffer.

Key design
──────────
One Redis List per device:
  Key   : readings:{device_id}
  Value : JSON-serialised list of PowerDetail dicts
  Max   : 2016 entries (7 days @ 5-min intervals)
  TTL   : 7 days (auto-expires if device goes silent)

Operations (Async)
──────────────────
  async_push_reading()    append one new reading, trim to max 2016
  async_get_history()     return last N readings (default 2016)
  async_get_fresh()       return last 12 readings (current hour)
  async_get_all_keys()    list all device_ids that have data in Redis
  async_reading_count()   how many readings buffered for a device
  async_flush_device()   delete all readings for a device
  async_ping()           check Redis connectivity
"""

import json
import logging
from typing import Optional

import redis.asyncio as redis

from config.devices import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    TOTAL_READINGS, FRESH_READINGS, REDIS_READINGS, TTL_SECONDS,
    HISTORICAL_READINGS,
)

log = logging.getLogger(__name__)

# ── connection pool (async) ───────────────────────────────────────────────────

_pool: redis.ConnectionPool = None


async def get_redis_pool() -> redis.ConnectionPool:
    """Get or create the async Redis connection pool."""
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            max_connections=20,
        )
    return _pool


async def get_redis() -> redis.Redis:
    """Get an async Redis client from the pool."""
    pool = await get_redis_pool()
    return redis.Redis(connection_pool=pool)


async def ping() -> bool:
    """Check Redis connectivity."""
    try:
        r = await get_redis()
        return await r.ping()
    except Exception:
        return False


# ── key helpers ───────────────────────────────────────────────────────────────

def _key(device_id: str) -> str:
    return f"readings:{device_id}"


# ── write ─────────────────────────────────────────────────────────────────────

async def push_reading(device_id: str, reading: dict) -> int:
    """
    Append one PowerDetail reading to the device's Redis list.
    Trims to REDIS_READINGS (288) from the right (newest end).
    Resets TTL on every write.
    When Redis reaches capacity (288), archives older readings to MinIO.
    Returns current list length.
    """
    r = await get_redis()
    key = _key(device_id)

    pipe = r.pipeline()
    pipe.rpush(key, json.dumps(reading))          # append to right (newest)
    pipe.ltrim(key, -REDIS_READINGS, -1)          # keep only last 288
    pipe.expire(key, TTL_SECONDS)                 # reset 24-hour TTL
    await pipe.execute()

    count = await r.llen(key)
    log.debug(f"[redis] push {device_id} → len={count}")
    
    # Check if we need to archive to MinIO
    # When Redis is full (288), we should have readings to archive
    if count >= REDIS_READINGS:
        await _archive_to_minio(device_id)
    
    return count


async def _archive_to_minio(device_id: str):
    """
    Archive readings older than 24 hours to MinIO.
    Called when Redis buffer reaches capacity.
    """
    try:
        from core import minio_store
        
        r = await get_redis()
        key = _key(device_id)
        
        # Get all readings currently in Redis
        raw = await r.lrange(key, 0, -1)
        readings = [json.loads(item) for item in raw]
        
        if len(readings) < REDIS_READINGS:
            return  # Not enough readings to archive yet
        
        # Split: keep last 288 in Redis, archive the rest
        # Readings are oldest → newest in the list
        # First 288-12 = 276 are older than 1 hour (can be archived)
        # Last 12 are fresh (keep in Redis)
        archive_count = max(0, len(readings) - FRESH_READINGS)
        
        if archive_count > 0:
            older_readings = readings[:-FRESH_READINGS] if FRESH_READINGS > 0 else readings
            
            # Save to MinIO (grouped by hour)
            success = await minio_store.save_reading_batch(device_id, older_readings)
            
            if success:
                log.info(f"[minio] Archived {len(older_readings)} readings for {device_id}")
            
    except ImportError:
        log.warning("MinIO store not available, skipping archive")
    except Exception as e:
        log.error(f"[minio] Archive error for {device_id}: {e}")


# ── read ──────────────────────────────────────────────────────────────────────

async def get_history(device_id: str, last_n: int = TOTAL_READINGS) -> list[dict]:
    """
    Return the last `last_n` readings for a device (oldest → newest).
    Default: all 2016 readings.
    """
    r = await get_redis()
    key = _key(device_id)
    # lrange(-last_n, -1) → last N items in the list
    raw = await r.lrange(key, -last_n, -1)
    return [json.loads(item) for item in raw]


async def get_fresh(device_id: str) -> list[dict]:
    """Return only the last 12 readings (most recent 1 hour)."""
    return await get_history(device_id, last_n=FRESH_READINGS)


async def get_history_range(
    device_id : str,
    from_time : Optional[str] = None,
    to_time   : Optional[str] = None,
    limit     : Optional[int] = None,
) -> list[dict]:
    """
    Return readings optionally filtered by ISO8601 time range.
    from_time / to_time are compared against reading["Time"] strings.
    Since times are ISO8601 UTC, string comparison works correctly.
    """
    readings = await get_history(device_id)

    if from_time:
        readings = [r for r in readings if r["Time"] >= from_time]
    if to_time:
        readings = [r for r in readings if r["Time"] <= to_time]
    if limit:
        readings = readings[-limit:]

    return readings


# ── meta ─────────────────────────────────────────────────────────────────────

async def reading_count(device_id: str) -> int:
    r = await get_redis()
    return await r.llen(_key(device_id))


async def get_all_device_ids() -> list[str]:
    """Return all device_ids that have data in Redis."""
    r = await get_redis()
    keys = await r.keys("readings:*")
    return [k.replace("readings:", "") for k in keys]


async def flush_device(device_id: str) -> bool:
    """Delete all readings for a device. Returns True if key existed."""
    r = await get_redis()
    return bool(await r.delete(_key(device_id)))


# ── batch operations (parallel) ─────────────────────────────────────────────

async def get_history_batch(device_ids: list[str], last_n: int = TOTAL_READINGS) -> dict[str, list[dict]]:
    """
    Fetch history for multiple devices in parallel using asyncio.gather.
    Returns a dict mapping device_id -> list of readings.
    This enables parallel Redis reads for the batch endpoint.
    """
    import asyncio
    
    async def fetch_one(did: str) -> tuple[str, list[dict]]:
        readings = await get_history(did, last_n=last_n)
        return (did, readings)
    
    # Fetch all in parallel
    results = await asyncio.gather(*[fetch_one(did) for did in device_ids])
    
    # Convert to dict
    return {did: readings for did, readings in results}


async def reading_count_batch(device_ids: list[str]) -> dict[str, int]:
    """
    Fetch reading counts for multiple devices in parallel.
    """
    import asyncio
    
    async def fetch_one(did: str) -> tuple[str, int]:
        count = await reading_count(did)
        return (did, count)
    
    results = await asyncio.gather(*[fetch_one(did) for did in device_ids])
    return {did: count for did, count in results}

