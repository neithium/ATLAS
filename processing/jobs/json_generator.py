# # # import json
# # # import time
# # # import uuid
# # # import os
# # # from datetime import datetime, timedelta
# # # import random

# # # OUTPUT_DIR = "/app/data/raw"

# # # os.makedirs(OUTPUT_DIR, exist_ok=True)

# # # def generate_device(device_id):

# # #     power_details = []

# # #     for i in range(5):  # few points per file
# # #         power_details.append({
# # #             "Time": (datetime.utcnow() - timedelta(minutes=i)).isoformat() + "Z",
# # #             "Average": random.uniform(200, 400),
# # #             "Minimum": random.uniform(150, 200),
# # #             "Peak": random.uniform(400, 500),
# # #             "CpuUtil": random.randint(10, 95),
# # #             "CpuWatts": random.randint(80, 300),
# # #             "GpuWatts": random.randint(20, 200),
# # #             "AmbTemp": random.uniform(18, 35),
# # #             "is_fresh": True
# # #         })

# # #     return {
# # #         "device_id": device_id,
# # #         "application_customer_id": "APPCUST0001",
# # #         "platform_customer_id": "PLATCUST001",
# # #         "report_type": "power",
# # #         "data": {
# # #             "PowerDetail": power_details
# # #         }
# # #     }


# # # while True:

# # #     file_name = f"{OUTPUT_DIR}/data_{int(time.time())}.json"

# # #     payload = {
# # #         "devices": {
# # #             f"device_{i}": generate_device(f"device_{i}")
# # #             for i in range(10)
# # #         }
# # #     }

# # #     with open(file_name, "w") as f:
# # #         json.dump(payload, f)

# # #     print(f"Generated {file_name}")

# # #     time.sleep(30)

# # import json
# # import time
# # import os
# # import random
# # from datetime import datetime, timedelta
# # import uuid

# # OUTPUT_DIR = "/app/data/raw"
# # os.makedirs(OUTPUT_DIR, exist_ok=True)

# # DEVICE_ID = "PLAT1-DEV-0001-001"

# # def generate_power_detail():

# #     records = []

# #     now = datetime.utcnow()

# #     # 🔹 Historical (last 6 days)
# #     for i in range(6 * 24):  # hourly data
# #         t = now - timedelta(hours=i)

# #         records.append({
# #             "Time": t.isoformat() + "Z",
# #             "Average": random.uniform(200, 400),
# #             "CpuUtil": random.randint(10, 90),
# #             "AmbTemp": random.uniform(20, 35),
# #             "Minimum": random.uniform(150, 200),
# #             "Peak": random.uniform(350, 450),
# #             "is_fresh": False
# #         })

# #     # 🔹 Fresh (last few minutes)
# #     for i in range(6):
# #         t = now - timedelta(minutes=i * 5)

# #         records.append({
# #             "Time": t.isoformat() + "Z",
# #             "Average": random.uniform(200, 400),
# #             "CpuUtil": random.randint(10, 90),
# #             "AmbTemp": random.uniform(20, 35),
# #             "Minimum": random.uniform(150, 200),
# #             "Peak": random.uniform(350, 450),
# #             "is_fresh": True
# #         })

# #     return records


# # while True:

# #     payload = {
# #         "application_customer_id": "APPCUST0001",
# #         "device_count": 1,
# #         "devices": {
# #             DEVICE_ID: {
# #                 "device_id": DEVICE_ID,
# #                 "platform_customer_id": "PLATCUST001",
# #                 "application_customer_id": "APPCUST0001",
# #                 "report_type": "power",
# #                 "data": {
# #                     "PowerDetail": generate_power_detail()
# #                 }
# #             }
# #         }
# #     }

# #     file_path = f"{OUTPUT_DIR}/data_{int(time.time())}.json"

# #     with open(file_path, "w") as f:
# #         json.dump(payload, f)

# #     print(f"Generated: {file_path}")

# #     time.sleep(30)



# import json
# import time
# import os
# import random
# from datetime import datetime, timedelta

# OUTPUT_DIR = "/app/data/raw"
# DEVICE_COUNT = 10

# os.makedirs(OUTPUT_DIR, exist_ok=True)

# def generate_power_detail():
#     records = []
#     now = datetime.utcnow()

#     # 6 days historical (hourly)
#     for i in range(6 * 24):
#         t = now - timedelta(hours=i)
#         records.append({
#             "Time": t.isoformat() + "Z",
#             "Average": random.uniform(200, 400),
#             "CpuUtil": random.randint(10, 90),
#             "AmbTemp": random.uniform(20, 35),
#             "Minimum": random.uniform(150, 200),
#             "Peak": random.uniform(350, 450),
#             "is_fresh": False
#         })

#     # fresh data (last few minutes)
#     for i in range(6):
#         t = now - timedelta(minutes=i * 5)
#         records.append({
#             "Time": t.isoformat() + "Z",
#             "Average": random.uniform(200, 400),
#             "CpuUtil": random.randint(10, 90),
#             "AmbTemp": random.uniform(20, 35),
#             "Minimum": random.uniform(150, 200),
#             "Peak": random.uniform(350, 450),
#             "is_fresh": True
#         })

#     return records


# def generate_devices():
#     devices = {}
#     for i in range(DEVICE_COUNT):
#         device_id = f"PLAT1-DEV-{i:04d}"

#         devices[device_id] = {
#             "device_id": device_id,
#             "platform_customer_id": "PLATCUST001",
#             "application_customer_id": "APPCUST0001",
#             "report_type": "power",
#             "data": {
#                 "PowerDetail": generate_power_detail()
#             }
#         }

#     return devices


# while True:
#     payload = {
#         "application_customer_id": "APPCUST0001",
#         "device_count": DEVICE_COUNT,
#         "devices": generate_devices()
#     }

#     file_path = f"{OUTPUT_DIR}/data_{int(time.time())}.json"

#     with open(file_path, "w") as f:
#         json.dump(payload, f)

#     print(f"Generated: {file_path}")

#     time.sleep(30)



import json
import time
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = "/app/data/raw"
DEVICE_COUNT = 100

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