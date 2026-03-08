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

Operations
──────────
  push_reading()     append one new reading, trim to max 2016
  get_history()      return last N readings (default 2016)
  get_fresh()        return last 12 readings (current hour)
  get_all_keys()     list all device_ids that have data in Redis
  reading_count()    how many readings buffered for a device
  flush_device()     delete all readings for a device
"""

import json
import logging
from typing import Optional

import redis

from config.devices import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    TOTAL_READINGS, FRESH_READINGS, TTL_SECONDS,
)

log = logging.getLogger(__name__)

# ── connection ────────────────────────────────────────────────────────────────

_pool = redis.ConnectionPool(
    host     = REDIS_HOST,
    port     = REDIS_PORT,
    db       = REDIS_DB,
    password = REDIS_PASSWORD,
    decode_responses=True,
    max_connections=20,
)

def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=_pool)


def ping() -> bool:
    try:
        return get_redis().ping()
    except Exception:
        return False


# ── key helpers ───────────────────────────────────────────────────────────────

def _key(device_id: str) -> str:
    return f"readings:{device_id}"


# ── write ─────────────────────────────────────────────────────────────────────

def push_reading(device_id: str, reading: dict) -> int:
    """
    Append one PowerDetail reading to the device's Redis list.
    Trims to TOTAL_READINGS (2016) from the right (newest end).
    Resets TTL on every write.
    Returns current list length.
    """
    r   = get_redis()
    key = _key(device_id)

    pipe = r.pipeline()
    pipe.rpush(key, json.dumps(reading))          # append to right (newest)
    pipe.ltrim(key, -TOTAL_READINGS, -1)          # keep only last 2016
    pipe.expire(key, TTL_SECONDS)                 # reset 7-day TTL
    results = pipe.execute()

    count = r.llen(key)
    log.debug(f"[redis] push {device_id} → len={count}")
    return count


# ── read ──────────────────────────────────────────────────────────────────────

def get_history(device_id: str, last_n: int = TOTAL_READINGS) -> list[dict]:
    """
    Return the last `last_n` readings for a device (oldest → newest).
    Default: all 2016 readings.
    """
    r      = get_redis()
    key    = _key(device_id)
    # lrange(-last_n, -1) → last N items in the list
    raw    = r.lrange(key, -last_n, -1)
    return [json.loads(item) for item in raw]


def get_fresh(device_id: str) -> list[dict]:
    """Return only the last 12 readings (most recent 1 hour)."""
    return get_history(device_id, last_n=FRESH_READINGS)


def get_history_range(
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
    readings = get_history(device_id)

    if from_time:
        readings = [r for r in readings if r["Time"] >= from_time]
    if to_time:
        readings = [r for r in readings if r["Time"] <= to_time]
    if limit:
        readings = readings[-limit:]

    return readings


# ── meta ──────────────────────────────────────────────────────────────────────

def reading_count(device_id: str) -> int:
    return get_redis().llen(_key(device_id))


def get_all_device_ids() -> list[str]:
    """Return all device_ids that have data in Redis."""
    keys = get_redis().keys("readings:*")
    return [k.replace("readings:", "") for k in keys]


def flush_device(device_id: str) -> bool:
    """Delete all readings for a device. Returns True if key existed."""
    return bool(get_redis().delete(_key(device_id)))
