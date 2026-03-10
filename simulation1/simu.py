"""
fill_buffer.py - Fills Redis buffer with mock IPMI readings
"""
import json
import random
from datetime import datetime, timezone

def generate_mock_reading(device_id):
    return {
        "AmbTemp": round(random.uniform(18.0, 28.0), 1),
        "Average": round(random.uniform(250.0, 450.0), 2),
        "CpuAvgFreq": random.randint(2400, 3600),
        "CpuMax": random.randint(3600, 4200),
        "CpuPwrSavLim": random.choice([0, 1, 2]),
        "CpuUtil": random.randint(5, 85),
        "CpuWatts": random.randint(80, 180),
        "GpuWatts": random.randint(50, 250),
        "Minimum": round(random.uniform(200.0, 300.0), 2),
        "Peak": round(random.uniform(500.0, 650.0), 2),
        "Time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "is_fresh": True,
    }

def fill_redis_buffer():
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    devices = ["PLAT1-DEV-001", "PLAT1-DEV-002", "PLAT2-DEV-001", "PLAT2-DEV-002", "PLAT3-DEV-001", "PLAT3-DEV-002"]
    
    for device_id in devices:
        readings = []
        for i in range(288):
            reading = generate_mock_reading(device_id)
            ts = datetime.now(timezone.utc).timestamp() - (i * 300)
            reading["Time"] = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            readings.append(reading)
        readings.reverse()
        
        key = f"readings:{device_id}"
        pipe = r.pipeline()
        for reading in readings:
            pipe.rpush(key, json.dumps(reading))
        pipe.ltrim(key, -288, -1)
        pipe.expire(key, 86400)
        pipe.execute()
        print(f"✓ Filled {device_id}: {r.llen(key)} readings")

if __name__ == "__main__":
    fill_redis_buffer()
