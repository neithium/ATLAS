"""
core/redis_store.py
-------------------
All Redis interactions for the power monitor ring buffer.

Key design
──────────
One Redis List per device:
  Key   : readings:{device_id}
  Value : JSON-serialised list of PowerDetail dicts
  Max   : 288 entries (24 hours @ 5-min intervals)
  TTL   : 24 hours (auto-expires if device goes silent)

Data Flow:
  Every 5 mins:  IPMI → Redis (ring buffer, always 288 max)
  Every 1 hour:   Redis → MinIO (batch 12 readings)

Operations (Async)
──────────────────
  async_push_reading()    append one new reading, trim to max 288
  async_get_history()     return last N readings (default 288)
  async_get_fresh()       return last 12 readings (current hour)
  async_get_all_keys()    list all device_ids that have data in Redis
  async_reading_count()   how many readings buffered for a device
  async_flush_device()   delete all readings for a device
  async_ping()           check Redis connectivity
  archive_hourly_to_minio()  hourly job: copy Redis → MinIO
"""

import asyncio
import json
import logging
import os
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
        # For 100k devices with concurrent workers, we need a larger pool
        # Each worker can have multiple concurrent Redis operations
        max_conn = int(os.getenv("REDIS_MAX_CONNECTIONS", "2000"))
        _pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            max_connections=max_conn,
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

async def push_reading(device_id: str, reading: dict, max_retries: int = 3) -> int:
    """
    Append one PowerDetail reading to the device's Redis list.
    Trims to REDIS_READINGS (288) from the right (newest end).
    Redis acts as a ring buffer - oldest pushed out naturally.
    Returns current list length.
    
    Includes retry logic for connection errors.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            r = await get_redis()
            key = _key(device_id)

            pipe = r.pipeline()
            pipe.rpush(key, json.dumps(reading))          # append to right (newest)
            pipe.ltrim(key, -REDIS_READINGS, -1)          # keep only last 288
            pipe.expire(key, TTL_SECONDS)                 # reset 24-hour TTL
            await pipe.execute()

            count = await r.llen(key)
            log.debug(f"[redis] push {device_id} → len={count}")
            
            return count
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                log.warning(f"[redis] Retry {attempt + 1}/{max_retries} for {device_id}: {e}")
                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
    
    log.error(f"[redis] Failed to push {device_id} after {max_retries} attempts: {last_error}")
    raise last_error


async def archive_hourly_to_minio():
    """
    Run every hour: Archive oldest 12 readings from Redis to MinIO.
    Called by scheduler (not on every push).
    """
    from config.devices import DEVICES
    
    for device_id in DEVICES.keys():
        await _archive_one_hour(device_id)


async def _archive_one_hour(device_id: str):
    """
    Archive exactly 12 readings (one hour) from Redis to MinIO.
    Called hourly by the scheduler.
    """
    try:
        from core import minio_store
        
        r = await get_redis()
        key = _key(device_id)
        
        # Get current count
        count = await r.llen(key)
        
        # Need at least 12 readings to archive
        if count < FRESH_READINGS:
            log.debug(f"[minio] {device_id}: not enough readings ({count}) to archive")
            return
        
        # Get all readings (oldest → newest)
        raw = await r.lrange(key, 0, -1)
        readings = [json.loads(item) for item in raw]
        
        # Archive oldest 12 readings (first 12 in list)
        to_archive = readings[:FRESH_READINGS]
        
        if to_archive:
            # Save to MinIO (grouped by hour)
            success = await minio_store.save_reading_batch(device_id, to_archive)
            
            if success:
                log.info(f"[minio] {device_id}: archived {len(to_archive)} readings to MinIO")
            
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
