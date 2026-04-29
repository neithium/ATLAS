import json
import time
import random
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s"
)
logger = logging.getLogger("ATLAS")

logging.getLogger("kafka").setLevel(logging.ERROR)

# ---------------- CONFIG ----------------
DEVICE_COUNT = 100
TOPIC = "raw-server-metrics"
RAW_DIR = "/app/data/raw"

VIRTUAL_START = datetime.utcnow()
START_REAL = time.time()
TIME_MULTIPLIER = 60

os.makedirs(RAW_DIR, exist_ok=True)

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

logger.info("GENERATOR STARTED")

while True:
    try:
        virtual_time = get_virtual_now()
        logger.info(f"Producing | virtual_time={virtual_time}")

        ts = int(time.time())

        with open(f"{RAW_DIR}/batch_{ts}.json", "w") as f:
            for i in range(DEVICE_COUNT):
                device_id = f"DEV-{i:04d}"
                event = generate_event(device_id)

                producer.send(TOPIC, event)
                f.write(json.dumps(event) + "\n")

        producer.flush()
        logger.info("Batch sent")

        time.sleep(5)

    except Exception:
        logger.exception("Generator failed")
        time.sleep(5)