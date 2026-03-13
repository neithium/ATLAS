"""
main.py
-------
FastAPI entry point.

Endpoints
─────────
GET  /health                              → system + Redis health
GET  /devices                             → list all registered devices
GET  /devices/{device_id}                 → full JSON output (2016 readings)
GET  /devices/{device_id}?from_time=&to_time=&limit=   → filtered history
GET  /devices/{device_id}/summary         → aggregated stats only
GET  /devices/{device_id}/latest          → single most-recent reading
GET  /devices/{device_id}/fresh           → last 12 readings (current hour)
POST /devices/{device_id}/poll            → force immediate IPMI poll
DELETE /devices/{device_id}/flush         → clear Redis buffer for device
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config.devices import DEVICES, TOTAL_READINGS, FRESH_READINGS, REDIS_READINGS
from core import poller
from core.redis_store import (
    get_history, get_fresh, get_history_range,
    reading_count, get_all_device_ids, flush_device, ping as redis_ping,
    get_history_batch, reading_count_batch, push_reading,
)
from core.response_builder import build_response
from core.ipmi_reader import read_device, fetch_inventory

# Import MinIO ping for health check
try:
    from core.minio_store import ping as minio_ping, ensure_bucket_exists
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    minio_ping = None

# Control which instance runs the poller (only "api" should run it)
ENABLE_POLLER = os.getenv("ENABLE_POLLER", "false").lower() == "true"

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── app ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Power Monitor API",
    description = (
        "Fetches live power/thermal/CPU metrics from server BMCs via IPMI, "
        "persists a 7-day rolling buffer in Redis, and returns "
        "fresh (1 hr) + historical (23 hr + 6 days) data per device."
    ),
    version = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


# ── lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    log.info("=" * 60)
    log.info("Power Monitor API starting up")
    log.info(f"Registered devices : {list(DEVICES.keys())}")
    log.info(f"Redis              : {await redis_ping()}")
    
    # Initialize MinIO bucket if available
    if MINIO_AVAILABLE:
        try:
            await ensure_bucket_exists()
            minio_ok = await minio_ping()
            log.info(f"MinIO              : {'connected' if minio_ok else 'bucket not ready'}")
        except Exception as e:
            log.warning(f"MinIO initialization failed: {e}")
    else:
        log.info("MinIO              : not available (minio package not installed)")
    
    log.info(f"Poller enabled     : {ENABLE_POLLER}")
    if ENABLE_POLLER:
        poller.start(run_immediately=True)
    else:
        log.info("Poller disabled - use POST /devices/{device_id}/poll to manually trigger")


@app.on_event("shutdown")
async def shutdown():
    if ENABLE_POLLER:
        poller.stop()


# ── helpers ───────────────────────────────────────────────────────────────────

def _require_device(device_id: str):
    if device_id not in DEVICES:
        raise HTTPException(
            status_code = 404,
            detail      = {
                "error"          : f"Device '{device_id}' not registered.",
                "registered_ids" : list(DEVICES.keys()),
            },
        )

async def _require_readings(device_id: str):
    """
    Check if readings exist. For warmup, we allow empty readings
    but inform the client about the current status.
    """
    count = await reading_count(device_id)
    if count == 0:
        # During warmup, don't fail - just return empty with status info
        return {
            "status": "warmup",
            "buffered": 0,
            "max": TOTAL_READINGS,
            "coverage_pct": 0.0,
            "complete": False,
        }
    return None


# ── endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    """
    Returns API health, Redis connectivity, MinIO status, and buffer status per device.
    """
    redis_ok = await redis_ping()
    devices_status = {}

    # Fetch all counts in parallel for better performance
    counts = await reading_count_batch(list(DEVICES.keys()))

    for did in DEVICES:
        count = counts.get(did, 0)
        devices_status[did] = {
            "server_name"       : DEVICES[did]["server_name"],
            "buffered"          : count,
            "redis_max"         : REDIS_READINGS,
            "total_max"         : TOTAL_READINGS,
            "coverage_pct"      : round(count / TOTAL_READINGS * 100, 1),
            "redis_ready"      : count >= REDIS_READINGS,
            "complete"          : count >= TOTAL_READINGS,
        }

    # Check MinIO status
    minio_status = "not_configured"
    if MINIO_AVAILABLE:
        try:
            minio_ok = await minio_ping()
            minio_status = "connected" if minio_ok else "disconnected"
        except Exception:
            minio_status = "error"

    return {
        "status"    : "ok" if redis_ok else "degraded",
        "timestamp" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "redis"     : "connected" if redis_ok else "unreachable",
        "minio"     : minio_status,
        "architecture": {
            "redis_readings": REDIS_READINGS,
            "minio_readings": TOTAL_READINGS - REDIS_READINGS,
            "total_readings": TOTAL_READINGS,
        },
        "devices"   : devices_status,
    }


@app.get("/devices", tags=["Devices"])
async def list_devices():
    """List all registered devices with metadata and buffer status."""
    # Fetch all counts in parallel for better performance
    counts = await reading_count_batch(list(DEVICES.keys()))
    
    result = []
    for did, meta in DEVICES.items():
        count = counts.get(did, 0)
        result.append({
            "device_id"              : did,
            "server_name"            : meta["server_name"],
            "model"                  : meta["model"],
            "processor_vendor"       : meta["processor_vendor"],
            "server_generation"      : meta["server_generation"],
            "location_city"          : meta["location_city"],
            "location_country"       : meta["location_country"],
            "location_name"          : meta["location_name"],
            "platform_customer_id"   : meta["platform_customer_id"],
            "application_customer_id": meta["application_customer_id"],
            "buffer": {
                "buffered"  : count,
                "max"       : TOTAL_READINGS,
                "coverage_pct": round(count / TOTAL_READINGS * 100, 1),
            },
        })
    return result


@app.get("/devices/batch", tags=["Devices"])
async def get_devices_batch(
    ids: str = Query(..., description="Comma-separated device IDs, e.g. DEV-SERVER-01,DEV-SERVER-02"),
):
    """
    **Batch endpoint** to retrieve full data for multiple devices at once.
    
    Returns full JSON documents (up to 2016 readings per device) for each
    requested device. Invalid or missing device IDs are included in the
    response with error information.
    
    Uses asyncio.gather() for parallel Redis fetching - all devices are
    fetched concurrently instead of sequentially.
    
Example: `/devices/batch?ids=DEV-SERVER-01,DEV-SERVER-02,DEV-SERVER-03`
    """
    # Parse comma-separated IDs
    device_ids = [d.strip() for d in ids.split(",") if d.strip()]
    
    if not device_ids:
        raise HTTPException(
            status_code = 400,
            detail      = "At least one device ID required",
        )
    
    if len(device_ids) > 20:
        raise HTTPException(
            status_code = 400,
            detail      = "Maximum 20 devices allowed per batch request",
        )
    
    # Separate valid and invalid device IDs
    valid_device_ids = []
    errors = []
    
    for device_id in device_ids:
        if device_id not in DEVICES:
            errors.append({
                "device_id": device_id,
                "error": f"Device '{device_id}' not registered",
                "registered_ids": list(DEVICES.keys()),
            })
            continue
        valid_device_ids.append(device_id)
    
    # Fetch all counts in parallel
    counts = await reading_count_batch(valid_device_ids)
    
    # Separate devices with no readings vs with readings
    devices_with_readings = []
    for did in valid_device_ids:
        if counts.get(did, 0) == 0:
            errors.append({
                "device_id": did,
                "error": f"No readings buffered yet for '{did}'",
                "hint": "Wait for the next 5-min poll or POST /devices/{device_id}/poll",
            })
        else:
            devices_with_readings.append(did)
    
    # Fetch all history data in PARALLEL using asyncio.gather
    history_data = await get_history_batch(devices_with_readings, last_n=TOTAL_READINGS)
    
    # Build all responses in PARALLEL using asyncio.gather
    async def build_one(did: str):
        return (did, await build_response(did, preloaded_readings=history_data.get(did, [])))
    
    responses = await asyncio.gather(*[build_one(did) for did in devices_with_readings])
    results = {did: response for did, response in responses}
    
    return {
        "requested_count": len(device_ids),
        "successful_count": len(results),
        "failed_count": len(errors),
        "errors": errors,
        "devices": results,
    }


@app.get("/devices/range", tags=["Devices"])
async def get_devices_range(
    start: str = Query(..., description="Start device ID, e.g. PLAT1-DEV-0001-001"),
    end: str = Query(..., description="End device ID, e.g. PLAT1-DEV-0001-050"),
    include_data: bool = Query(False, description="Include full readings data"),
):
    """
    **Range endpoint** to retrieve devices within an ID range.
    
    Returns all devices from start to end (inclusive).
    Supports prefix matching, e.g., PLAT1-DEV-0001-001 to PLAT1-DEV-0001-050.
    
Example: `/devices/range?start=PLAT1-DEV-0001-001&end=PLAT1-DEV-0001-050`
    """
    # Normalize: extract prefix and numeric range
    # Match pattern like PLAT1-DEV-0001-001
    pattern = r"^(.+?)(\d+)$"
    
    start_match = re.match(pattern, start)
    end_match = re.match(pattern, end)
    
    if not start_match or not end_match:
        raise HTTPException(
            status_code = 400,
            detail      = "Invalid device ID format. Use format like PLAT1-DEV-0001-001",
        )
    
    start_prefix, start_num_str = start_match.groups()
    end_prefix, end_num_str = end_match.groups()
    
    if start_prefix != end_prefix:
        raise HTTPException(
            status_code = 400,
            detail      = "Start and end must have same prefix (e.g., both PLAT1)",
        )
    
    start_num = int(start_num_str)
    end_num = int(end_num_str)
    
    # Find all matching devices
    all_device_ids = sorted(DEVICES.keys())
    matching_ids = []
    
    for did in all_device_ids:
        match = re.match(pattern, did)
        if match:
            prefix, num_str = match.groups()
            if prefix == start_prefix:
                num = int(num_str)
                if start_num <= num <= end_num:
                    matching_ids.append(did)
    
    if not matching_ids:
        return {
            "start": start,
            "end": end,
            "count": 0,
            "devices": [],
            "message": "No devices found in range",
        }
    
    # Fetch counts
    counts = await reading_count_batch(matching_ids)
    
    if not include_data:
        # Return device metadata only
        result = []
        for did in matching_ids:
            meta = DEVICES[did]
            count = counts.get(did, 0)
            result.append({
                "device_id": did,
                "server_name": meta["server_name"],
                "model": meta["model"],
                "processor_vendor": meta["processor_vendor"],
                "location_city": meta["location_city"],
                "platform_customer_id": meta["platform_customer_id"],
                "application_customer_id": meta["application_customer_id"],
                "buffered": count,
                "coverage_pct": round(count / TOTAL_READINGS * 100, 1),
            })
        return {
            "start": start,
            "end": end,
            "count": len(result),
            "devices": result,
        }
    
    # Include full data
    history_data = await get_history_batch(matching_ids, last_n=TOTAL_READINGS)
    
    async def build_one(did: str):
        return (did, await build_response(did, preloaded_readings=history_data.get(did, [])))
    
    responses = await asyncio.gather(*[build_one(did) for did in matching_ids])
    results = {did: response for did, response in responses}
    
    return {
        "start": start,
        "end": end,
        "count": len(results),
        "devices": results,
    }


@app.get("/devices/{device_id}", tags=["Data"])
async def get_device_data(
    device_id : str,
    from_time : Optional[str] = Query(None, description="ISO8601 UTC  e.g. 2026-03-01T00:00:00Z"),
    to_time   : Optional[str] = Query(None, description="ISO8601 UTC  e.g. 2026-03-07T23:59:59Z"),
    limit     : Optional[int] = Query(None, description="Max readings to return (newest N)"),
):
    """
    **Primary endpoint.**

    Returns the full JSON document for a device matching input_schema:

    - `data.PowerDetail` contains up to **2016 readings** (7 days)
    - Last **12 readings** are fresh (current hour from IPMI)
    - Remaining readings are from Redis (24 hours) + MinIO (6 days)
    - Each reading has `is_fresh: true/false` to distinguish them
    - `coverage_pct` shows warmup completion percentage
    - `complete` indicates if full 7-day data is available

    During warmup, returns available data with coverage_pct < 100%.

    Optionally filter by `from_time` / `to_time` or cap with `limit`.
    """
    _require_device(device_id)
    warmup_status = await _require_readings(device_id)

    if from_time or to_time or limit:
        # Filtered mode — return slice only, no full schema wrapping
        readings = await get_history_range(device_id, from_time, to_time, limit)
        meta     = DEVICES[device_id]
        return {
            "device_id"  : device_id,
            "server_name": meta["server_name"],
            "filters"    : {"from_time": from_time, "to_time": to_time, "limit": limit},
            "total"      : len(readings),
            "PowerDetail": readings,
        }

    # Full mode — returns complete input_schema-shaped document
    # Even during warmup, this will return available data
    return await build_response(device_id)


@app.get("/devices/{device_id}/fresh", tags=["Data"])
async def get_fresh_readings(device_id: str):
    """
    Return only the **12 most recent readings** (current hour from IPMI).
    During warmup, returns whatever is available.
    """
    _require_device(device_id)
    warmup_status = await _require_readings(device_id)

    readings = await get_fresh(device_id)
    meta     = DEVICES[device_id]
    return {
        "device_id"  : device_id,
        "server_name": meta["server_name"],
        "description": "Last 12 readings (1 hour) — freshest data from IPMI",
        "total"      : len(readings),
        "warmup"     : warmup_status is not None,
        "PowerDetail": readings,
    }


@app.get("/devices/{device_id}/latest", tags=["Data"])
async def get_latest_reading(device_id: str):
    """Return the single most-recent reading for a device."""
    _require_device(device_id)
    warmup_status = await _require_readings(device_id)

    readings = await get_history(device_id, last_n=1)
    if not readings:
        return {
            "device_id"  : device_id,
            "server_name": DEVICES[device_id]["server_name"],
            "reading"    : None,
            "warmup"     : True,
            "message"    : "No readings available yet - still in warmup period",
        }
    return {
        "device_id"  : device_id,
        "server_name": DEVICES[device_id]["server_name"],
        "reading"    : readings[0],
    }


@app.get("/devices/{device_id}/summary", tags=["Data"])
async def get_summary(device_id: str):
    """
    Return aggregated 7-day summary stats without the full PowerDetail array.
    Fast — no large payload.
    During warmup, returns available data with coverage_pct < 100%.
    """
    _require_device(device_id)
    warmup_status = await _require_readings(device_id)

    doc = await build_response(device_id)
    return {
        "device_id"  : device_id,
        "server_name": doc["server_name"],
        "created_at" : doc["created_at"],
        "coverage_pct": doc.get("coverage_pct", 0.0),
        "complete"   : doc.get("complete", False),
        "summary"    : doc["summary"],
    }


@app.post("/devices/{device_id}/poll", tags=["System"])
async def force_poll(device_id: str):
    """
    Force an **immediate IPMI poll** for a device outside the 5-min schedule.
    Useful for testing or getting a reading right now.
    """
    _require_device(device_id)

    meta = DEVICES[device_id]
    try:
        # Run blocking IPMI call in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        reading = await loop.run_in_executor(
            None,
            lambda: read_device(
                device_id    = device_id,
                ipmi_host    = meta["ipmi_host"],
                ipmi_user    = meta["ipmi_user"],
                ipmi_password= meta["ipmi_password"],
                ipmi_port    = meta.get("ipmi_port", 623),
            )
        )
        count = await push_reading(device_id, reading)
        return {
            "status"     : "ok",
            "device_id"  : device_id,
            "reading"    : reading,
            "buffered"   : count,
        }
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/devices/{device_id}/inventory", tags=["Data"])
async def get_inventory(device_id: str):
    """
    Return the hardware inventory for a device (CPU and memory info).
    Fetches from IPMI (or returns mock data if MOCK_IPMI=true).
    
    This endpoint shows how to fetch real inventory data using ipmitool commands:
    - `ipmitool fru print` - Field Replaceable Unit info (CPU, memory)
    - `ipmitool dcmi info` - DCMI info including processor count
    - `ipmitool dcmi get memory_info` - Memory information
    
    Returns inventory_data matching input_schema structure.
    """
    _require_device(device_id)

    meta = DEVICES[device_id]
    try:
        # Run blocking IPMI call in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        inventory = await loop.run_in_executor(
            None,
            lambda: fetch_inventory(
                device_id    = device_id,
                ipmi_host    = meta["ipmi_host"],
                ipmi_user    = meta["ipmi_user"],
                ipmi_password= meta["ipmi_password"],
                ipmi_port    = meta.get("ipmi_port", 623),
            )
        )
        return {
            "device_id"    : device_id,
            "server_name"  : meta["server_name"],
            "inventory_data": inventory,
        }
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.delete("/devices/{device_id}/flush", tags=["System"])
async def flush_buffer(device_id: str):
    """
    Clear all buffered readings for a device from Redis.
    The poller will start refilling from the next poll cycle.
    """
    _require_device(device_id)
    deleted = await flush_device(device_id)
    return {
        "status"    : "flushed" if deleted else "already_empty",
        "device_id" : device_id,
    }
