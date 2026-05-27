import orjson
import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load devices from JSON
DEVICE_CONFIG_PATH = os.getenv("DEVICE_CONFIG_PATH", str(BASE_DIR / "device_configs.json"))

def load_devices():
    """Load the device registry from the JSON config file."""
    try:
        if os.path.exists(DEVICE_CONFIG_PATH):
            with open(DEVICE_CONFIG_PATH, 'rb') as f:
                return orjson.loads(f.read())
        return {}
    except Exception as e:
        print(f"Error loading devices from {DEVICE_CONFIG_PATH}: {e}")
        return {}

# The global device registry
DEVICES = load_devices()

# ── Sampling and Archiving Constants ──────────────────────────────────────────
# Data arrives every 5 minutes (12 readings per hour)
READINGS_PER_HOUR = 12
HOURS_PER_DAY = 24
DAYS_IN_REDIS = 1    # Recent buffer is stored in Redis
DAYS_IN_MINIO = 6    # After 1 hour, data is moved to MinIO for long-term storage
TOTAL_DAYS = 7       # Pipeline target is a 7-day rolling window

# Calculated capacity
REDIS_READINGS = READINGS_PER_HOUR * HOURS_PER_DAY * DAYS_IN_REDIS    # 288
MINIO_READINGS = READINGS_PER_HOUR * HOURS_PER_DAY * DAYS_IN_MINIO    # 1728
TOTAL_READINGS = READINGS_PER_HOUR * HOURS_PER_DAY * TOTAL_DAYS       # 2016

# How many readings qualify as "fresh" (last 1 hour of data)
FRESH_READINGS = 12

# Polling interval for background worker
POLL_INTERVAL_SECONDS = 300  # 5 minutes
ENABLE_POLLER = os.getenv("ENABLE_POLLER", "false").lower() == "true"

# ── Redis Configuration ───────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# TTL for Redis keys (24 hours + 1 hour safety margin)
TTL_SECONDS = (HOURS_PER_DAY * 3600) + 3600

# ── Storage Calculations ──────────────────────────────────────────────────────
HISTORICAL_READINGS = TOTAL_READINGS - REDIS_READINGS
