# # import json
# # import time
# # import os
# # import random
# # from datetime import datetime, timedelta
# # from kafka import KafkaProducer

# # OUTPUT_DIR = "/app/data/raw"
# # DEVICE_COUNT = 1000

# # VIRTUAL_START = datetime.utcnow()
# # START_REAL = time.time()
# # TIME_MULTIPLIER = 60  # 1 min = 1 hour

# # os.makedirs(OUTPUT_DIR, exist_ok=True)

# # producer = KafkaProducer(
# #     bootstrap_servers='broker1:9092',
# #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# # )

# # def get_virtual_now():
# #     elapsed = time.time() - START_REAL
# #     return VIRTUAL_START + timedelta(seconds=elapsed * TIME_MULTIPLIER)

# # def generate_power_detail():
# #     records = []
# #     now = get_virtual_now()

# #     # 6 days history
# #     for i in range(6 * 24):
# #         t = now - timedelta(hours=i)
# #         records.append({
# #             "Time": t.isoformat() + "Z",
# #             "Average": float(random.uniform(200, 400)),
# #             "CpuUtil": int(random.randint(10, 90)),  # important
# #             "AmbTemp": float(random.uniform(20, 35)),
# #             "Minimum": float(random.uniform(150, 200)),
# #             "Peak": float(random.uniform(350, 450)),
# #             "is_fresh": False
# #         })

# #     # fresh (1 hour)
# #     for i in range(12):
# #         t = now - timedelta(minutes=i * 5)
# #         records.append({
# #             "Time": t.isoformat() + "Z",
# #             "Average": float(random.uniform(200, 400)),
# #             "CpuUtil": int(random.randint(10, 90)),
# #             "AmbTemp": float(random.uniform(20, 35)),
# #             "Minimum": float(random.uniform(150, 200)),
# #             "Peak": float(random.uniform(350, 450)),
# #             "is_fresh": True
# #         })

# #     return records

# # while True:
# #     ts = int(time.time())
# #     print("SIM TIME:", get_virtual_now())

# #     for i in range(DEVICE_COUNT):
# #         device_id = f"PLAT1-DEV-{i:04d}"

# #         payload = {
# #             "application_customer_id": "APPCUST0001",
# #             "device_count": 1,
# #             "devices": {
# #                 device_id: {
# #                     "device_id": device_id,
# #                     "platform_customer_id": "PLATCUST001",
# #                     "application_customer_id": "APPCUST0001",
# #                     "report_type": "power",
# #                     "data": {
# #                         "PowerDetail": generate_power_detail()
# #                     }
# #                 }
# #             }
# #         }

# #         with open(f"{OUTPUT_DIR}/data_{device_id}_{ts}.json", "w") as f:
# #             json.dump(payload, f)

# #     print("✅ Generated batch")
# #     time.sleep(300)

# import json
# import time
# import os
# import random
# from datetime import datetime, timedelta
# from kafka import KafkaProducer

# OUTPUT_DIR = "/app/data/raw"
# DEVICE_COUNT = 1000

# VIRTUAL_START = datetime.utcnow()
# START_REAL = time.time()
# TIME_MULTIPLIER = 60  # 1 min = 1 hour

# os.makedirs(OUTPUT_DIR, exist_ok=True)

# # ✅ Kafka Producer (optimized)
# producer = KafkaProducer(
#     bootstrap_servers='broker1:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     linger_ms=50,
#     batch_size=32768
# )

# def get_virtual_now():
#     elapsed = time.time() - START_REAL
#     return VIRTUAL_START + timedelta(seconds=elapsed * TIME_MULTIPLIER)

# def generate_power_detail():
#     records = []
#     now = get_virtual_now()

#     # 6 days history
#     for i in range(6 * 24):
#         t = now - timedelta(hours=i)
#         records.append({
#             "Time": t.isoformat() + "Z",
#             "Average": float(random.uniform(200, 400)),
#             "CpuUtil": int(random.randint(10, 90)),
#             "AmbTemp": float(random.uniform(20, 35)),
#             "Minimum": float(random.uniform(150, 200)),
#             "Peak": float(random.uniform(350, 450)),
#             "is_fresh": False
#         })

#     # fresh (1 hour)
#     for i in range(12):
#         t = now - timedelta(minutes=i * 5)
#         records.append({
#             "Time": t.isoformat() + "Z",
#             "Average": float(random.uniform(200, 400)),
#             "CpuUtil": int(random.randint(10, 90)),
#             "AmbTemp": float(random.uniform(20, 35)),
#             "Minimum": float(random.uniform(150, 200)),
#             "Peak": float(random.uniform(350, 450)),
#             "is_fresh": True
#         })

#     return records

# print("🟡 GENERATOR STARTED")

# while True:
#     try:
#         ts = int(time.time())
#         print("SIM TIME:", get_virtual_now())

#         for i in range(DEVICE_COUNT):
#             device_id = f"PLAT1-DEV-{i:04d}"

#             payload = {
#                 "application_customer_id": "APPCUST0001",
#                 "device_count": 1,
#                 "devices": {
#                     device_id: {
#                         "device_id": device_id,
#                         "platform_customer_id": "PLATCUST001",
#                         "application_customer_id": "APPCUST0001",
#                         "report_type": "power",
#                         "data": {
#                             "PowerDetail": generate_power_detail()
#                         }
#                     }
#                 }
#             }

#             # ✅ Send to Kafka
#             producer.send("raw-server-metrics", payload)

#             # ✅ Write to file (batch pipeline)
#             with open(f"{OUTPUT_DIR}/data_{device_id}_{ts}.json", "w") as f:
#                 json.dump(payload, f)

#         # ✅ Flush once per batch (important)
#         producer.flush()

#         print("✅ Generated batch")
#         time.sleep(300)

#     except Exception as e:
#         print("⚠️ Generator Error:", e)
#         time.sleep(10)

import json
import time
import random
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ---------------- CONFIG ----------------
DEVICE_COUNT = 100
TOPIC = "raw-server-metrics"
RAW_DIR = "/app/data/raw"

VIRTUAL_START = datetime.utcnow()
START_REAL = time.time()
TIME_MULTIPLIER = 60  # 1 min real = 1 hour virtual

os.makedirs(RAW_DIR, exist_ok=True)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='broker1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=50,
    batch_size=32768
)

def get_virtual_now():
    elapsed = time.time() - START_REAL
    return VIRTUAL_START + timedelta(seconds=elapsed * TIME_MULTIPLIER)

def generate_event(device_id):
    now = get_virtual_now()
    return {
        "device_id": device_id,
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "cpu": random.randint(10, 90),
        "mem": random.randint(20, 80)
    }

print("🟡 GENERATOR STARTED")

while True:
    try:
        virtual_time = get_virtual_now()
        print(f"🔥 Producing: {virtual_time}")

        ts = int(time.time())

        with open(f"{RAW_DIR}/batch_{ts}.json", "w") as f:
            for i in range(DEVICE_COUNT):
                device_id = f"DEV-{i:04d}"
                event = generate_event(device_id)

                # Kafka
                producer.send(TOPIC, event)

                # ✅ NDJSON (one record per line)
                f.write(json.dumps(event) + "\n")

        producer.flush()
        print("✅ Batch sent")

        # 5 sec real = 5 min virtual
        time.sleep(5)

    except Exception as e:
        print("⚠️ Generator error:", e)
        time.sleep(5)