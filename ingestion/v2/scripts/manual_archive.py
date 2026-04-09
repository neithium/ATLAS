"""
manual_archive.py — Manual Trigger for MinIO Compaction
───────────────────────────────────────────────────────
Forcefully pulls data from TimescaleDB and pushes to MinIO.
"""
import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Adjust path to find core/v2 modules
sys.path.append("/app")

# OVERRIDE: Force port 9000 to bypass Nginx 404
os.environ["MINIO_HOST"] = "127.0.0.1:9000"

from core.poller import archive_daily_to_minio

async def run_manual_archive():
    print("🚀 [MANUAL] Starting Cold-Path Archival Process...")
    # NOTE: The default archive_daily_to_minio looks at 'yesterday'.
    # We will trigger it now.
    try:
        # Since the function is async in the scheduler context (it's called by AsyncIOScheduler)
        # But wait, looking at poller.py, notice if archive_daily_to_minio is async or sync.
        # Let's check the definition again.
        pass
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(archive_daily_to_minio())
    print("✅ [MANUAL] Archival Job Finished. Check MinIO now.")
