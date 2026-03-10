"""
core/minio_store.py
-------------------
MinIO/S3-compatible storage for historical power readings.

Key design
──────────
- Stores readings older than 24 hours in MinIO (S3-compatible)
- File structure: {bucket}/device_id/date/hour/readings.json
- Each file contains up to 288 readings (one hour at 5-min intervals)
- Used as fallback for historical data when Redis buffer is insufficient

Data retention
──────────────
- MinIO: 6 days of historical data (1728 readings)
- Redis: 24 hours of recent data (288 readings)
- Total: 7 days (2016 readings)

Operations (Async)
──────────────────
  async_save_hour()      Save one hour of readings to MinIO
  async_get_history()    Retrieve historical readings from MinIO
  async_get_range()      Get readings within a time range
  async_delete_old()     Delete data older than retention period
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

import minio
from minio.error import S3Error

from config.devices import (
    MINIO_HOST, MINIO_PORT, MINIO_BUCKET, 
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
    TOTAL_READINGS, FRESH_READINGS, HISTORICAL_READINGS,
)

log = logging.getLogger(__name__)

# ── connection pool ───────────────────────────────────────────────────────────

_client: minio.Minio = None


def get_minio_client() -> minio.Minio:
    """Get or create MinIO client."""
    global _client
    if _client is None:
        endpoint = f"{MINIO_HOST}:{MINIO_PORT}"
        _client = minio.Minio(
            endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
    return _client


async def ensure_bucket_exists():
    """Create bucket if it doesn't exist."""
    try:
        client = get_minio_client()
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            log.info(f"Created MinIO bucket: {MINIO_BUCKET}")
    except S3Error as e:
        log.error(f"Failed to ensure bucket exists: {e}")
        raise


# ── path helpers ───────────────────────────────────────────────────────────────

def _device_prefix(device_id: str) -> str:
    """Base path for device data in bucket."""
    return f"{MINIO_BUCKET}/{device_id}"


def _hour_path(device_id: str, hour_dt: datetime) -> str:
    """
    Generate object key for hourly readings file.
    Format: {device_id}/YYYY/MM/DD/HH/readings.json
    """
    return f"{device_id}/{hour_dt.strftime('%Y/%m/%d/%H')}/readings.json"


# ── write ─────────────────────────────────────────────────────────────────────

async def save_hour_readings(device_id: str, readings: list[dict]) -> bool:
    """
    Save one hour of readings to MinIO.
    Expects readings to be from the same hour.
    Returns True on success.
    """
    if not readings:
        return True
    
    try:
        client = get_minio_client()
        
        # Determine the hour from the first reading
        first_time = readings[0].get("Time", "")
        if not first_time:
            log.warning(f"No timestamp in readings for {device_id}")
            return False
        
        # Parse the timestamp
        try:
            dt = datetime.fromisoformat(first_time.replace("Z", "+00:00"))
        except ValueError:
            log.warning(f"Invalid timestamp format: {first_time}")
            return False
        
        # Round to hour
        hour_dt = dt.replace(minute=0, second=0, microsecond=0)
        
        # Create object key
        object_key = _hour_path(device_id, hour_dt)
        
        # Convert readings to JSON
        data = json.dumps(readings).encode('utf-8')
        
        # Upload to MinIO
        data_length = len(data)
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_key,
            data=__import__('io').BytesIO(data),
            length=data_length,
            content_type='application/json',
        )
        
        log.debug(f"Saved {len(readings)} readings to MinIO: {object_key}")
        return True
        
    except S3Error as e:
        log.error(f"MinIO save error for {device_id}: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error saving to MinIO: {e}")
        return False


async def save_reading_batch(device_id: str, readings: list[dict]) -> bool:
    """
    Save a batch of readings to MinIO, organized by hour.
    Reads are grouped by hour and saved to separate files.
    """
    if not readings:
        return True
    
    try:
        # Group readings by hour
        hourly_readings: dict[datetime, list[dict]] = {}
        
        for reading in readings:
            time_str = reading.get("Time", "")
            if not time_str:
                continue
            
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                hour_dt = dt.replace(minute=0, second=0, microsecond=0)
                
                if hour_dt not in hourly_readings:
                    hourly_readings[hour_dt] = []
                hourly_readings[hour_dt].append(reading)
            except ValueError:
                continue
        
        # Save each hour
        for hour_dt, hour_readings in hourly_readings.items():
            await save_hour_readings(device_id, hour_readings)
        
        return True
        
    except Exception as e:
        log.error(f"Error saving batch for {device_id}: {e}")
        return False


# ── read ─────────────────────────────────────────────────────────────────────

async def get_history(device_id: str, last_n: int = TOTAL_READINGS) -> list[dict]:
    """
    Retrieve historical readings from MinIO.
    Returns up to last_n readings sorted oldest → newest.
    """
    try:
        client = get_minio_client()
        
        # List all objects for this device
        prefix = f"{device_id}/"
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
        
        if not objects:
            return []
        
        # Sort by object name (which includes date/time)
        # This ensures chronological order
        object_names = sorted([obj.object_name for obj in objects])
        
        # Read all hourly files and combine
        all_readings = []
        
        for obj_name in object_names:
            try:
                response = client.get_object(MINIO_BUCKET, obj_name)
                data = response.read()
                readings = json.loads(data.decode('utf-8'))
                
                if isinstance(readings, list):
                    all_readings.extend(readings)
                elif isinstance(readings, dict):
                    # Handle single reading or object format
                    all_readings.append(readings)
                    
                response.close()
                response.release_conn()
                
            except Exception as e:
                log.warning(f"Error reading {obj_name}: {e}")
                continue
        
        # Sort by timestamp and return last N
        all_readings.sort(key=lambda r: r.get("Time", ""))
        
        return all_readings[-last_n:] if last_n > 0 else all_readings
        
    except S3Error as e:
        log.error(f"MinIO get_history error for {device_id}: {e}")
        return []
    except Exception as e:
        log.error(f"Unexpected error reading from MinIO: {e}")
        return []


async def get_history_range(
    device_id: str,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
) -> list[dict]:
    """
    Get historical readings within a time range from MinIO.
    """
    readings = await get_history(device_id, last_n=TOTAL_READINGS)
    
    if from_time:
        readings = [r for r in readings if r.get("Time", "") >= from_time]
    if to_time:
        readings = [r for r in readings if r.get("Time", "") <= to_time]
    
    return readings


# ── delete ───────────────────────────────────────────────────────────────────

async def delete_old_readings(device_id: str, days: int = 6) -> int:
    """
    Delete readings older than specified days from MinIO.
    Returns count of deleted objects.
    """
    try:
        client = get_minio_client()
        
        # Calculate cutoff date
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        # List all objects for device
        prefix = f"{device_id}/"
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
        
        deleted_count = 0
        for obj in objects:
            # Parse date from object name: device_id/YYYY/MM/DD/HH/readings.json
            parts = obj.object_name.split('/')
            if len(parts) >= 5:
                try:
                    date_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
                    obj_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    
                    if obj_date < cutoff:
                        client.remove_object(MINIO_BUCKET, obj.object_name)
                        deleted_count += 1
                except (ValueError, IndexError):
                    continue
        
        log.info(f"Deleted {deleted_count} old objects for {device_id}")
        return deleted_count
        
    except Exception as e:
        log.error(f"Error deleting old readings: {e}")
        return 0


# ── health check ─────────────────────────────────────────────────────────────

async def ping() -> bool:
    """Check MinIO connectivity."""
    try:
        client = get_minio_client()
        return client.bucket_exists(MINIO_BUCKET)
    except Exception:
        return False

