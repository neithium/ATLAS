import requests
import time

API_URL = "http://localhost:8001"
PCID = "PLATCUST001"
ACID = "APPCUST0001"
BATCH_SIZE = 10

# Load devices
with open("devices.txt") as f:
    devices = [line.strip() for line in f if line.strip()]

print(f"Total devices: {len(devices)}")

for i in range(0, len(devices), BATCH_SIZE):
    batch = devices[i:i+BATCH_SIZE]
    device_string = ",".join(batch)

    url = f"{API_URL}/pcid/{PCID}/acid/{ACID}/id/{device_string}/export"

    try:
        res = requests.post(url)
        print(f"Batch {i//BATCH_SIZE + 1} → {res.status_code}")
    except Exception as e:
        print(f"Batch {i//BATCH_SIZE + 1} failed:", e)

    time.sleep(40)  # small delay to avoid overload