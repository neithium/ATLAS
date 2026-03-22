"""
fill_7day_data.py - Fills Redis and MinIO with 7 days of mock IPMI readings

Hierarchical device structure:
- Platform Customer (PLATCUST1, PLATCUST2, PLATCUST3)
  - Application Customer (APPCUST001 - APPCUST600 per platform)
    - Devices (50 devices per application customer)

Platform Configuration:
- Platform 1 (PLATCUST1): 600 app customers × 50 devices = 30,000 devices
- Platform 2 (PLATCUST2): 600 app customers × 50 devices = 30,000 devices (or 20,000)
- Platform 3 (PLATCUST3): 600 app customers × 50 devices = 30,000 devices (or 20,000)

Usage:
    python fill_7day_data.py                    # Generate devices with data
    python fill_7day_data.py --devices-only      # Generate device configs only
    python fill_7day_data.py --batch-size 1000  # Process in batches
"""

import json
import random
import argparse
from datetime import datetime, timezone, timedelta
from typing import Optional

# Configuration - Hierarchical structure
# Total: 100 devices from all 3 platforms
# Hierarchy: PLATCUST → APPCUST → DEVICES

PLATFORM_CONFIGS = [
    {
        "prefix": "PLAT1", 
        "platform_customer_id": "PLATCUST001", 
        "app_customers": 8,   
        "devices_per_app": 5000, # 40,000 devices
        "location": "Austin", 
        "state": "TX"
    },
    {
        "prefix": "PLAT2", 
        "platform_customer_id": "PLATCUST002", 
        "app_customers": 8,   
        "devices_per_app": 5000, # 40,000 devices
        "location": "Denver", 
        "state": "CO"
    },
]


# For 100k production:
# PLAT1: 1200 app_customers × 50 = 60,000
# PLAT2: 400 app_customers × 50 = 20,000
# PLAT3: 400 app_customers × 50 = 20,000

READINGS_PER_HOUR = 12
HOURS_PER_DAY = 24
DAYS_IN_REDIS = 1       # 24 hours
DAYS_IN_MINIO = 6        # 6 days
TOTAL_DAYS = 7           # 7 days total

REDIS_READINGS = READINGS_PER_HOUR * HOURS_PER_DAY * DAYS_IN_REDIS    # 288
MINIO_READINGS = READINGS_PER_HOUR * HOURS_PER_DAY * DAYS_IN_MINIO    # 1728
TOTAL_READINGS = READINGS_PER_HOUR * HOURS_PER_DAY * TOTAL_DAYS       # 2016

INTERVAL_SECONDS = 5 * 60  # 5 minutes


def generate_device_list() -> list[str]:
    """Generate list of all device IDs based on hierarchical platform configs."""
    devices = []
    for platform in PLATFORM_CONFIGS:
        prefix = platform["prefix"]
        app_customers = platform["app_customers"]
        devices_per_app = platform["devices_per_app"]
        
        for app_idx in range(1, app_customers + 1):
            for dev_idx in range(1, devices_per_app + 1):
                device_id = f"{prefix}-DEV-{app_idx:04d}-{dev_idx:03d}"
                devices.append(device_id)
    
    return devices


def generate_device_config(device_id: str, platform: dict, app_customer_num: int) -> dict:
    """Generate device configuration dynamically based on hierarchy."""
    parts = device_id.split("-")
    platform_prefix = parts[0]  # PLAT1, PLAT2, PLAT3
    app_num = int(parts[2])     # Application customer number
    dev_num = int(parts[3])     # Device number within app customer
    
    # Generate customer IDs based on hierarchy
    # PLATCUST1 → APPCUST0001 → DEV-0001-001
    platform_customer_id = platform["platform_customer_id"]  # PLATCUST1
    application_customer_id = f"APPCUST{app_num:04d}"       # APPCUST0001 - APPCUST0600
    
    return {
        "ipmi_host": f"192.168.{int(platform_prefix.replace('PLAT', ''))}.{(app_num % 254) + 1}",
        "ipmi_user": "admin",
        "ipmi_password": "admin",
        "ipmi_port": 623,
        
        "server_name": f"srv-{platform['location'].lower()}-{app_num:04d}-{dev_num:03d}",
        "model": f"ProLiant DL{360 + (dev_num % 10)} Gen{10 + (dev_num % 3)}",
        "processor_vendor": "Intel" if dev_num % 2 == 0 else "AMD",
        "server_generation": f"Gen{10 + (dev_num % 3)}",
        
        "location_id": f"LOC-{platform_prefix.replace('PLAT', '')}",
        "location_city": platform["location"],
        "location_state": platform["state"],
        "location_country": "US",
        "location_name": f"DC-{platform['location']}-01",
        
        "platform_customer_id": platform_customer_id,
        "application_customer_id": application_customer_id,
    }


import hashlib

def generate_mock_reading(device_id: str, base_time: datetime) -> dict:
    """Generate a single mock IPMI reading with realistic values."""
    # Use a stable hash so it generates consistent values across script runs
    hash_val = int(hashlib.md5(device_id.encode('utf-8')).hexdigest(), 16)
    device_seed = hash_val % 10000
    
    base_power = 300 + (device_seed % 150)  # 300-450W range
    cpu_max = 3600 + (device_seed % 600)
    
    return {
        "AmbTemp": round(random.uniform(18.0, 28.0), 1),
        "Average": round(random.uniform(base_power - 50, base_power + 100), 2),
        "CpuAvgFreq": random.randint(2400, cpu_max),
        "CpuMax": cpu_max,
        "CpuPwrSavLim": random.choice([0, 1, 2]),
        "CpuUtil": random.randint(5, 85),
        "CpuWatts": random.randint(80, 180),
        "GpuWatts": random.randint(50, 250),
        "Minimum": round(random.uniform(base_power - 100, base_power - 50), 2),
        "Peak": round(random.uniform(base_power + 150, base_power + 250), 2),
        "Time": base_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "is_fresh": False,
    }

def generate_readings_for_device(device_id: str, end_time: datetime, total_readings: int) -> list[dict]:
    """Generate a series of readings ending at end_time."""
    readings = []
    start_time = end_time - timedelta(seconds=total_readings * INTERVAL_SECONDS)
    
    for i in range(total_readings):
        reading_time = start_time + timedelta(seconds=i * INTERVAL_SECONDS)
        reading = generate_mock_reading(device_id, reading_time)
        readings.append(reading)
    
    return readings


def fill_redis_buffer(device_id: str, readings: list[dict]) -> int:
    """Fill Redis with the most recent readings (last 24 hours)."""
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        r.ping()
        
        key = f"readings:{device_id}"
        
        pipe = r.pipeline()
        pipe.delete(key)
        
        for reading in readings:
            pipe.rpush(key, json.dumps(reading))
        
        pipe.ltrim(key, -REDIS_READINGS, -1)
        pipe.expire(key, 24 * 3600)
        
        pipe.execute()
        
        return r.llen(key)
        
    except ImportError:
        return 0
    except Exception:
        return 0


def fill_minio_store(device_id: str, readings: list[dict]) -> int:
    """Fill MinIO with historical readings (6 days)."""
    try:
        from minio import Minio
        
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        
        bucket_name = "power-readings"
        
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        
        hourly_readings = {}
        for reading in readings:
            time_str = reading.get("Time", "")
            if not time_str:
                continue
            
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                hour_key = dt.replace(minute=0, second=0, microsecond=0)
                
                if hour_key not in hourly_readings:
                    hourly_readings[hour_key] = []
                hourly_readings[hour_key].append(reading)
            except ValueError:
                continue
        
        saved_count = 0
        for hour_dt, hour_readings in sorted(hourly_readings.items()):
            object_name = f"{device_id}/{hour_dt.strftime('%Y/%m/%d/%H')}/readings.json"
            
            data = json.dumps(hour_readings).encode('utf-8')
            
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=__import__('io').BytesIO(data),
                length=len(data),
                content_type='application/json',
            )
            saved_count += len(hour_readings)
        
        return saved_count
        
    except ImportError:
        return 0
    except Exception:
        return 0


def main():
    parser = argparse.ArgumentParser(description='Fill 7-day data for devices')
    parser.add_argument('--devices-only', action='store_true', 
                        help='Only generate device configs, skip data filling')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of devices to process per batch')
    parser.add_argument('--skip', type=int, default=0,
                        help='Skip first N devices')
    args = parser.parse_args()
    
    # Calculate total devices
    total_devices = sum(p["app_customers"] * p["devices_per_app"] for p in PLATFORM_CONFIGS)
    total_app_customers = sum(p["app_customers"] for p in PLATFORM_CONFIGS)
    
    print("=" * 70)
    print("Auto-filling 7 days of data for devices")
    print("=" * 70)
    print(f"\nHierarchy Structure:")
    print(f"  Platform Customer → Application Customer (600) → Devices (50)")
    print(f"\nPlatform Configuration:")
    for platform in PLATFORM_CONFIGS:
        devices_count = platform["app_customers"] * platform["devices_per_app"]
        print(f"  - {platform['platform_customer_id']}")
        print(f"      → {platform['app_customers']} app customers ({platform['app_customers']} × {platform['devices_per_app']} = {devices_count:,} devices)")
        print(f"      → Location: {platform['location']}, {platform['state']}")
    
    print(f"\n  Total Application Customers: {total_app_customers:,}")
    print(f"  Total Devices: {total_devices:,}")
    print(f"\nData Configuration:")
    print(f"  - Redis readings per device: {REDIS_READINGS} (24 hours)")
    print(f"  - MinIO readings per device: {MINIO_READINGS} (6 days)")
    print(f"  - Total readings per device: {TOTAL_READINGS} (7 days)")
    print("=" * 70)
    
    if args.devices_only:
        print("\nGenerating device configurations...")
        
        all_configs = {}
        device_idx = 0
        
        for platform in PLATFORM_CONFIGS:
            prefix = platform["prefix"]
            app_customers = platform["app_customers"]
            devices_per_app = platform["devices_per_app"]
            
            for app_idx in range(1, app_customers + 1):
                for dev_idx in range(1, devices_per_app + 1):
                    device_id = f"{prefix}-DEV-{app_idx:04d}-{dev_idx:03d}"
                    all_configs[device_id] = generate_device_config(device_id, platform, app_idx)
                    device_idx += 1
                    
                    if device_idx % 10000 == 0:
                        print(f"  Generated {device_idx:,} / {total_devices:,} device configs...")
        
        # Save to file
        with open('device_configs.json', 'w') as f:
            json.dump(all_configs, f, indent=2)
        
        print(f"\n✓ Saved {total_devices:,} device configurations to device_configs.json")
        return
    
    # Use current time as the end time
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)
    
    print(f"\nTime range:")
    print(f"  - Start: {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"  - End:   {end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print()
    
    # Process devices in batches
    total_redis = 0
    total_minio = 0
    batch_size = args.batch_size
    skip = args.skip
    
    # Generate devices to process
    devices_to_process = []
    current_skip = skip
    
    for platform in PLATFORM_CONFIGS:
        prefix = platform["prefix"]
        app_customers = platform["app_customers"]
        devices_per_app = platform["devices_per_app"]
        
        for app_idx in range(1, app_customers + 1):
            for dev_idx in range(1, devices_per_app + 1):
                device_id = f"{prefix}-DEV-{app_idx:04d}-{dev_idx:03d}"
                
                if current_skip > 0:
                    current_skip -= 1
                    continue
                
                devices_to_process.append((device_id, platform, app_idx))
    
    print(f"Processing {len(devices_to_process):,} devices (skipping first {skip:,})...")
    print(f"Batch size: {batch_size}")
    print()
    
    for batch_idx in range(0, len(devices_to_process), batch_size):
        batch_devices = devices_to_process[batch_idx:batch_idx + batch_size]
        
        for device_id, platform, app_idx in batch_devices:
            all_readings = generate_readings_for_device(device_id, end_time, TOTAL_READINGS)
            
            redis_readings = all_readings[-REDIS_READINGS:]
            minio_readings = all_readings[:MINIO_READINGS]
            
            redis_count = fill_redis_buffer(device_id, redis_readings)
            total_redis += redis_count
            
            minio_count = fill_minio_store(device_id, minio_readings)
            total_minio += minio_count
        
        processed = batch_idx + len(batch_devices)
        print(f"  Progress: {processed:,} / {len(devices_to_process):,} ({100*processed/len(devices_to_process):.1f}%)")
    
    print()
    print("=" * 70)
    print("Summary:")
    print(f"  - Total devices processed: {len(devices_to_process):,}")
    print(f"  - Total Redis readings: {total_redis:,}")
    print(f"  - Total MinIO readings: {total_minio:,}")
    print(f"  - Total readings: {total_redis + total_minio:,}")
    print("=" * 70)
    print("✓ Complete!")


if __name__ == "__main__":
    main()
