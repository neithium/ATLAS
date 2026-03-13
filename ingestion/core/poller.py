"""
core/poller.py
--------------
Background async scheduler that:
- Polls devices via IPMI every 5 minutes → Redis
- Archives Redis → MinIO every 1 hour

Supports large-scale polling (100k+ devices) with:
- Batched polling (process devices in chunks)
- Configurable workers for parallel processing
- Rate limiting to avoid overwhelming IPMI interfaces

Runs inside the FastAPI process using APScheduler.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.devices import DEVICES, POLL_INTERVAL_SECONDS, REDIS_READINGS, TOTAL_READINGS
from core.ipmi_reader import read_device
from core.redis_store import push_reading, archive_hourly_to_minio

log = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# ── Polling Configuration ───────────────────────────────────────────────────

# Number of devices to process in each batch (helps with memory management)
# For 100k devices, smaller batches reduce memory pressure
POLL_BATCH_SIZE = int(os.getenv("POLL_BATCH_SIZE", "50"))

# Number of concurrent poll workers (parallel IPMI requests)
# For 100k devices, we need more workers to complete within reasonable time
# But too many will overwhelm the Redis connection pool
POLL_WORKERS = int(os.getenv("POLL_WORKERS", "10"))

# Delay between batches in seconds (rate limiting)
POLL_BATCH_DELAY = float(os.getenv("POLL_BATCH_DELAY", "0.05"))

# Staggered startup delay (seconds between each device when starting)
# This avoids thundering herd when starting the poller with 100k devices
POLL_STARTUP_DELAY = float(os.getenv("POLL_STARTUP_DELAY", "0.01"))

# Maximum number of devices to poll (use for testing with subset)
# Set to 0 or remove to poll all devices
POLL_MAX_DEVICES = int(os.getenv("POLL_MAX_DEVICES", "0")) or None

# Whether to enable batching mode for large device counts
ENABLE_BATCH_MODE = int(os.getenv("ENABLE_BATCH_MODE", "1")) == 1


def _get_devices_to_poll() -> list:
    """Get the list of devices to poll."""
    devices = list(DEVICES.keys())
    
    # Limit devices if POLL_MAX_DEVICES is set (for testing)
    if POLL_MAX_DEVICES and POLL_MAX_DEVICES > 0:
        devices = devices[:POLL_MAX_DEVICES]
        log.info(f"[poller] Limited to {POLL_MAX_DEVICES} devices for testing")
    
    return devices


async def _poll_single(device_id: str):
    """Poll one device and push result to Redis."""
    meta = DEVICES.get(device_id)
    if not meta:
        log.warning(f"[poller] Device {device_id} not found in configuration")
        return None
    
    try:
        # Run blocking IPMI call in thread pool to avoid blocking event loop
        # This is critical for large-scale polling (100k+ devices)
        loop = asyncio.get_event_loop()
        reading = await loop.run_in_executor(
            None,  # Use default ThreadPoolExecutor
            lambda: read_device(
                device_id    = device_id,
                ipmi_host    = meta["ipmi_host"],
                ipmi_user    = meta["ipmi_user"],
                ipmi_password= meta["ipmi_password"],
                ipmi_port    = meta.get("ipmi_port", 623),
            )
        )
        count = await push_reading(device_id, reading)
        log.debug(
            f"[poller] ✓ {device_id} | "
            f"avg={reading['Average']}W  cpu={reading['CpuUtil']}%  "
            f"temp={reading['AmbTemp']}°C | buffered={count}"
        )
        return {"device_id": device_id, "status": "success", "count": count}
    except ConnectionError as e:
        log.warning(f"[poller] ✗ {device_id} unreachable → {e}")
        return {"device_id": device_id, "status": "unreachable", "error": str(e)}
    except Exception as e:
        log.error(f"[poller] ✗ {device_id} unexpected error → {e}")
        return {"device_id": device_id, "status": "error", "error": str(e)}


async def _poll_batch(batch: list, worker_id: int = 0):
    """Poll a batch of devices concurrently."""
    log.info(f"[poller] Worker {worker_id}: Processing batch of {len(batch)} devices")
    
    results = await asyncio.gather(*[_poll_single(did) for did in batch], return_exceptions=True)
    
    success = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
    errors = len(results) - success
    
    log.info(f"[poller] Worker {worker_id}: Completed - {success} success, {errors} errors")
    return results


async def poll_all():
    """
    Poll all registered devices.
    
    For large device counts (>1000), uses batched polling:
    - Divides devices into batches of POLL_BATCH_SIZE
    - Processes batches in parallel with POLL_WORKERS workers
    - Adds delay between batches for rate limiting
    - Staggered startup to avoid thundering herd
    """
    devices = _get_devices_to_poll()
    total_devices = len(devices)
    
    log.info(f"[poller] Polling {total_devices} device(s) at "
             f"{datetime.now(timezone.utc).strftime('%H:%M:%SZ')} …")
    
    # For small device counts, use simple concurrent polling
    if not ENABLE_BATCH_MODE or total_devices <= POLL_BATCH_SIZE:
        # Add staggered startup delay to avoid thundering herd
        for i, did in enumerate(devices):
            if i > 0 and POLL_STARTUP_DELAY > 0:
                await asyncio.sleep(POLL_STARTUP_DELAY)
            await _poll_single(did)
        log.info(f"[poller] Completed polling {total_devices} devices")
        return
    
    # Batched polling for large device counts
    batches = [devices[i:i + POLL_BATCH_SIZE] for i in range(0, total_devices, POLL_BATCH_SIZE)]
    total_batches = len(batches)
    
    log.info(f"[poller] Using batch mode: {total_batches} batches, "
             f"batch_size={POLL_BATCH_SIZE}, workers={POLL_WORKERS}")
    
    # Process batches with limited concurrency
    semaphore = asyncio.Semaphore(POLL_WORKERS)
    
    async def process_batch_with_semaphore(batch_idx: int):
        async with semaphore:
            batch = batches[batch_idx]
            # Add staggered startup delay between batches on initial run
            if batch_idx > 0 and POLL_STARTUP_DELAY > 0:
                await asyncio.sleep(POLL_STARTUP_DELAY * POLL_BATCH_SIZE)
            await _poll_batch(batch, worker_id=batch_idx % POLL_WORKERS)
            # Rate limiting delay between batches
            if batch_idx < total_batches - 1:
                await asyncio.sleep(POLL_BATCH_DELAY)
    
    # Run all batches
    await asyncio.gather(*[process_batch_with_semaphore(i) for i in range(total_batches)])
    
    log.info(f"[poller] Completed polling {total_devices} devices in {total_batches} batches")


async def poll_platform(platform_customer_id: str):
    """Poll only devices for a specific platform customer."""
    devices = [
        did for did, config in DEVICES.items() 
        if config.get("platform_customer_id") == platform_customer_id
    ]
    
    log.info(f"[poller] Polling {len(devices)} device(s) for platform {platform_customer_id}")
    
    if not devices:
        log.warning(f"[poller] No devices found for platform {platform_customer_id}")
        return
    
    # Use batched polling for large platform device counts
    if len(devices) > POLL_BATCH_SIZE:
        batches = [devices[i:i + POLL_BATCH_SIZE] for i in range(0, len(devices), POLL_BATCH_SIZE)]
        
        semaphore = asyncio.Semaphore(POLL_WORKERS)
        
        async def process_batch(batch_idx: int):
            async with semaphore:
                # Staggered startup between batches
                if batch_idx > 0 and POLL_STARTUP_DELAY > 0:
                    await asyncio.sleep(POLL_STARTUP_DELAY * POLL_BATCH_SIZE)
                await _poll_batch(batches[batch_idx], worker_id=batch_idx % POLL_WORKERS)
        
        await asyncio.gather(*[process_batch(i) for i in range(len(batches))])
    else:
        # Staggered startup for small device counts
        for i, did in enumerate(devices):
            if i > 0 and POLL_STARTUP_DELAY > 0:
                await asyncio.sleep(POLL_STARTUP_DELAY)
            await _poll_single(did)
    
    log.info(f"[poller] Completed polling {len(devices)} devices for platform {platform_customer_id}")


async def poll_app_customer(application_customer_id: str):
    """Poll only devices for a specific application customer."""
    devices = [
        did for did, config in DEVICES.items() 
        if config.get("application_customer_id") == application_customer_id
    ]
    
    log.info(f"[poller] Polling {len(devices)} device(s) for app customer {application_customer_id}")
    
    if not devices:
        log.warning(f"[poller] No devices found for app customer {application_customer_id}")
        return
    
    await asyncio.gather(*[_poll_single(did) for did in devices])
    log.info(f"[poller] Completed polling {len(devices)} devices for app customer {application_customer_id}")


async def archive_all():
    """Archive all devices from Redis to MinIO."""
    log.info(f"[minio] Starting hourly archive to MinIO …")
    await archive_hourly_to_minio()
    log.info(f"[minio] Hourly archive complete")


def start(run_immediately: bool = True):
    """
    Start the background scheduler.
    - 5-minute IPMI polling job
    - 1-hour MinIO archive job
    """
    async def run_poll():
        await poll_all()
    
    async def run_archive():
        await archive_all()
    
# 5-minute IPMI polling job
    # misfire_grace_time allows jobs to run even if slightly delayed
    scheduler.add_job(
        run_poll,
        trigger  = "interval",
        seconds  = POLL_INTERVAL_SECONDS,
        id       = "ipmi_poller",
        max_instances = 1,
        misfire_grace_time = 300,  # Allow up to 5 minutes delay before considering it missed
    )
    
    # 1-hour MinIO archive job
    scheduler.add_job(
        run_archive,
        trigger  = "interval",
        hours    = 1,
        id       = "minio_archiver",
        max_instances = 1,
        misfire_grace_time = 3600,  # Allow up to 1 hour delay
    )
    
    scheduler.start()
    
    log.info(f"[poller] Scheduler started")
    log.info(f"  - poll_interval={POLL_INTERVAL_SECONDS}s")
    log.info(f"  - archive_interval=1h")
    log.info(f"  - batch_mode={ENABLE_BATCH_MODE}")
    log.info(f"  - batch_size={POLL_BATCH_SIZE}")
    log.info(f"  - workers={POLL_WORKERS}")

    if run_immediately:
        asyncio.create_task(poll_all())


def stop():
    scheduler.shutdown(wait=False)
    log.info("[poller] Scheduler stopped")


# ── CLI Helper ───────────────────────────────────────────────────────────────

def run_cli():
    """Run poller from command line for testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Device poller')
    parser.add_argument('--platform', help='Poll specific platform (PLATCUST1, PLATCUST2, PLATCUST3)')
    parser.add_argument('--app-customer', help='Poll specific app customer (APPCUST0001, etc.)')
    parser.add_argument('--once', action='store_true', help='Run once instead of starting scheduler')
    
    args = parser.parse_args()
    
    if args.platform:
        asyncio.run(poll_platform(args.platform))
    elif args.app_customer:
        asyncio.run(poll_app_customer(args.app_customer))
    else:
        if args.once:
            asyncio.run(poll_all())
        else:
            start()


if __name__ == "__main__":
    run_cli()

