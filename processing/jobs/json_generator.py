import json
import time
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = "/app/data/raw"
DEVICE_COUNT = 1000

VIRTUAL_START = datetime.utcnow()
START_REAL = time.time()
TIME_MULTIPLIER = 60  # 1 min = 1 hour

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_virtual_now():
    elapsed = time.time() - START_REAL
    return VIRTUAL_START + timedelta(seconds=elapsed * TIME_MULTIPLIER)

def generate_power_detail():
    records = []
    now = get_virtual_now()

    # 6 days history
    for i in range(6 * 24):
        t = now - timedelta(hours=i)
        records.append({
            "Time": t.isoformat() + "Z",
            "Average": float(random.uniform(200, 400)),
            "CpuUtil": int(random.randint(10, 90)),  # important
            "AmbTemp": float(random.uniform(20, 35)),
            "Minimum": float(random.uniform(150, 200)),
            "Peak": float(random.uniform(350, 450)),
            "is_fresh": False
        })

    # fresh (1 hour)
    for i in range(12):
        t = now - timedelta(minutes=i * 5)
        records.append({
            "Time": t.isoformat() + "Z",
            "Average": float(random.uniform(200, 400)),
            "CpuUtil": int(random.randint(10, 90)),
            "AmbTemp": float(random.uniform(20, 35)),
            "Minimum": float(random.uniform(150, 200)),
            "Peak": float(random.uniform(350, 450)),
            "is_fresh": True
        })

    return records

while True:
    ts = int(time.time())
    print("SIM TIME:", get_virtual_now())

    for i in range(DEVICE_COUNT):
        device_id = f"PLAT1-DEV-{i:04d}"

        payload = {
            "application_customer_id": "APPCUST0001",
            "device_count": 1,
            "devices": {
                device_id: {
                    "device_id": device_id,
                    "platform_customer_id": "PLATCUST001",
                    "application_customer_id": "APPCUST0001",
                    "report_type": "power",
                    "data": {
                        "PowerDetail": generate_power_detail()
                    }
                }
            }
        }

        with open(f"{OUTPUT_DIR}/data_{device_id}_{ts}.json", "w") as f:
            json.dump(payload, f)

    print("✅ Generated batch")
    time.sleep(300)