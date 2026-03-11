"""
core/poller.py
--------------
Background async scheduler that:
- Polls every device via IPMI every 5 minutes → Redis
- Archives Redis → MinIO every 1 hour

Runs inside the FastAPI process using APScheduler.
"""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.devices import DEVICES, POLL_INTERVAL_SECONDS, REDIS_READINGS, TOTAL_READINGS
from core.ipmi_reader import read_device
from core.redis_store import push_reading, archive_hourly_to_minio

log = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def _poll_single(device_id: str):
    """Poll one device and push result to Redis."""
    meta = DEVICES[device_id]
    try:
        reading = read_device(
            device_id    = device_id,
            ipmi_host    = meta["ipmi_host"],
            ipmi_user    = meta["ipmi_user"],
            ipmi_password= meta["ipmi_password"],
            ipmi_port    = meta.get("ipmi_port", 623),
        )
        count = await push_reading(device_id, reading)
        log.info(
            f"[poller] ✓ {device_id} | "
            f"avg={reading['Average']}W  cpu={reading['CpuUtil']}%  "
            f"temp={reading['AmbTemp']}°C | "
            f"buffered={count}/{TOTAL_READINGS} (Redis:{REDIS_READINGS})"
        )
    except ConnectionError as e:
        log.warning(f"[poller] ✗ {device_id} unreachable → {e}")
    except Exception as e:
        log.error(f"[poller] ✗ {device_id} unexpected error → {e}")


async def poll_all():
    """Poll all registered devices concurrently."""
    log.info(f"[poller] Polling {len(DEVICES)} device(s) at "
             f"{datetime.now(timezone.utc).strftime('%H:%M:%SZ')} …")
    await asyncio.gather(*[_poll_single(did) for did in DEVICES])


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
    scheduler.add_job(
        run_poll,
        trigger  = "interval",
        seconds  = POLL_INTERVAL_SECONDS,
        id       = "ipmi_poller",
        max_instances = 1,
    )
    
    # 1-hour MinIO archive job
    scheduler.add_job(
        run_archive,
        trigger  = "interval",
        hours    = 1,
        id       = "minio_archiver",
        max_instances = 1,
    )
    
    scheduler.start()
    log.info(f"[poller] Scheduler started — poll_interval={POLL_INTERVAL_SECONDS}s, archive_interval=1h")

    if run_immediately:
        asyncio.create_task(poll_all())


def stop():
    scheduler.shutdown(wait=False)
    log.info("[poller] Scheduler stopped")
