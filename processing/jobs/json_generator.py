import json
import time
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = "/app/data/raw"
DEVICE_COUNT = 1000

os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_power_detail():

    records = []
    now = datetime.utcnow()

    # 6 days historical
    for i in range(6 * 24):
        t = now - timedelta(hours=i)

        records.append({
            "Time": t.isoformat() + "Z",
            "Average": random.uniform(200, 400),
            "CpuUtil": random.randint(10, 90),
            "AmbTemp": random.uniform(20, 35),
            "Minimum": random.uniform(150, 200),
            "Peak": random.uniform(350, 450),
            "is_fresh": False
        })

    # fresh data
    for i in range(6):
        t = now - timedelta(minutes=i * 5)

        records.append({
            "Time": t.isoformat() + "Z",
            "Average": random.uniform(200, 400),
            "CpuUtil": random.randint(10, 90),
            "AmbTemp": random.uniform(20, 35),
            "Minimum": random.uniform(150, 200),
            "Peak": random.uniform(350, 450),
            "is_fresh": True
        })

    return records


while True:

    timestamp = int(time.time())

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

        file_path = f"{OUTPUT_DIR}/data_{device_id}_{timestamp}.json"

        with open(file_path, "w") as f:
            json.dump(payload, f)

    print(f"Generated {DEVICE_COUNT} files")

    time.sleep(30)