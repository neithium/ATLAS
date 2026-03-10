"""
core/poller.py
--------------
Background async scheduler that polls every device via IPMI
every 5 minutes and pushes the reading into Redis.

Runs inside the FastAPI process using APScheduler.
"""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.devices import DEVICES, POLL_INTERVAL_SECONDS
from core.ipmi_reader import read_device
from core.redis_store import push_reading

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
            f"buffered={count}/2016"
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


def start(run_immediately: bool = True):
    """
    Start the background scheduler.
    If run_immediately=True, fires one poll right away before
    waiting for the first 5-min interval.
    """
    async def run_poll():
        await poll_all()
    
    scheduler.add_job(
        run_poll,
        trigger  = "interval",
        seconds  = POLL_INTERVAL_SECONDS,
        id       = "ipmi_poller",
        max_instances = 1,           # never overlap if a poll runs slow
    )
    scheduler.start()
    log.info(f"[poller] Scheduler started — interval={POLL_INTERVAL_SECONDS}s")

    if run_immediately:
        asyncio.create_task(poll_all())


def stop():
    scheduler.shutdown(wait=False)
    log.info("[poller] Scheduler stopped")
