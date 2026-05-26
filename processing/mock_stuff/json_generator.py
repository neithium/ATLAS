# import json
# import time
# import random
# import os
# from datetime import datetime, timedelta
# from kafka import KafkaProducer
# import logging

# # ---------------- LOGGING ----------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
# )
# logger = logging.getLogger("ATLAS")

# logging.getLogger("kafka").setLevel(logging.ERROR)

# # ---------------- CONFIG ----------------
# DEVICE_COUNT = 100
# TOPIC = "raw-server-metrics"
# RAW_DIR = "/app/data/raw"

# VIRTUAL_START = datetime.utcnow()
# START_REAL = time.time()
# TIME_MULTIPLIER = 60

# os.makedirs(RAW_DIR, exist_ok=True)

# producer = KafkaProducer(
#     bootstrap_servers='broker1:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     linger_ms=50,
#     batch_size=32768
# )

# def get_virtual_now():
#     elapsed = time.time() - START_REAL
#     return VIRTUAL_START + timedelta(seconds=elapsed * TIME_MULTIPLIER)

# def generate_event(device_id):
#     now = get_virtual_now()
#     return {
#         "device_id": device_id,
#         "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
#         "cpu": random.randint(10, 90),
#         "mem": random.randint(20, 80)
#     }

# logger.info("GENERATOR STARTED")

# while True:
#     try:
#         virtual_time = get_virtual_now()
#         logger.info(f"Producing | virtual_time={virtual_time}")

#         ts = int(time.time())

#         with open(f"{RAW_DIR}/batch_{ts}.json", "w") as f:
#             for i in range(DEVICE_COUNT):
#                 device_id = f"DEV-{i:04d}"
#                 event = generate_event(device_id)

#                 producer.send(TOPIC, event)
#                 f.write(json.dumps(event) + "\n")

#         producer.flush()
#         logger.info("Batch sent")

#         time.sleep(5)

#     except Exception:
#         logger.exception("Generator failed")
#         time.sleep(5)
import json, time, random, uuid, os
from datetime import datetime
from kafka import KafkaProducer

RAW_DIR = "/app/data/raw"
TOPIC = "raw-server-metrics"
os.makedirs(RAW_DIR, exist_ok=True)

producer = KafkaProducer(
    bootstrap_servers="broker1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_power_detail():
    return {
        "AmbTemp": float(random.uniform(20, 40)),
        "Average": float(random.uniform(100, 300)),
        "CpuAvgFreq": int(random.randint(2000, 3500)),
        "CpuMax": int(random.randint(80, 100)),
        "CpuPwrSavLim": int(random.randint(50, 70)),
        "CpuUtil": int(random.randint(10, 90)),
        "CpuWatts": int(random.randint(100, 250)),
        "GpuWatts": int(random.randint(50, 150)),
        "Minimum": int(random.randint(50, 100)),
        "Peak": int(random.randint(200, 400)),
        "Time": datetime.utcnow().isoformat()
    }

def generate_record(i):
    return {
        "data": {
            "Id": f"ID-{i}",
            "Average": float(random.uniform(100, 300)),
            "Maximum": float(random.uniform(300, 500)),
            "Minimum": float(random.uniform(50, 100)),
            "Name": "Power",
            "PowerDetail": [generate_power_detail() for _ in range(5)]
        },
        "model": "PowerEdge",
        "tags": "prod",
        "status": True,
        "device_id": f"DEV-{i}",
        "report_id": str(uuid.uuid4()),
        "created_at": datetime.utcnow().isoformat(),
        "location_id": "LOC1",
        "report_type": "telemetry",
        "server_name": f"server-{i}",
        "error_reason": None,
        "location_city": "Bangalore",
        "location_name": "DC1",
        "location_state": "KA",
        "location_country": "India",
        "processor_vendor": "Intel",
        "server_generation": "Gen10",
        "platform_customer_id": "PLAT1",
        "application_customer_id": "APP1",
        "metric_type": "power",
        "inventory_data": {
            "cpu_count": 2,
            "socket_count": 2,
            "cpu_inventory": [{
                "model": "Xeon",
                "speed": 3000,
                "total_cores": 8
            }],
            "memory_inventory": [{
                "memory_size": 32,
                "operating_freq": 3200,
                "memory_device_type": "DDR4"
            }]
        }
    }

while True:
    file_path = f"{RAW_DIR}/data_{int(time.time())}.json"

    with open(file_path, "w") as f:
        for i in range(1000):
            record = generate_record(i)
            f.write(json.dumps(record) + "\n")
            producer.send(TOPIC, record)

    producer.flush()
    print("✅ Generated valid schema data")
    time.sleep(5)