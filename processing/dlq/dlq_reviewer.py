import json
import time

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from recovery_rules import recover_record

print("🚀 DLQ REVIEWER BOOTING...")

# =========================================================
# WAIT FOR KAFKA
# =========================================================

while True:

    try:

        print("⏳ Connecting to Kafka...")

        consumer = KafkaConsumer(

            "raw-server-metrics-dlq",

            bootstrap_servers="broker1:9092",

            auto_offset_reset="latest",

            group_id="dlq-reviewer-group",

            value_deserializer=lambda x:
                json.loads(x.decode())
        )

        retry_producer = KafkaProducer(

            bootstrap_servers="broker1:9092",

            value_serializer=lambda x:
                json.dumps(x).encode()
        )

        failure_producer = KafkaProducer(

            bootstrap_servers="broker1:9092",

            value_serializer=lambda x:
                json.dumps(x).encode()
        )

        print("✅ Connected to Kafka")

        break

    except NoBrokersAvailable:

        print("⚠️ Kafka not ready. Retrying in 5s...")

        time.sleep(5)

# =========================================================
# MAIN LOOP
# =========================================================

print("👂 Waiting for DLQ messages...")

recovered_count = 0
failed_count = 0

for msg in consumer:

    try:

        dlq_message = msg.value

        print(
            f"⚠️ Reviewing "
            f"{dlq_message['error_type']}"
        )

        success, repaired_record = recover_record(
            dlq_message
        )

        if success:

            retry_producer.send(

                "raw-server-metrics-retry",

                repaired_record
            )

            recovered_count += 1

            print(
                "✅ repaired and replayed"
            )

        else:

            failure_producer.send(

                "raw-server-metrics-failure",

                repaired_record
            )

            failed_count += 1

            print(
                "❌ permanent failure"
            )

        print(
            f"📊 recovered={recovered_count} "
            f"failed={failed_count}"
        )

    except Exception as e:

        print(f"❌ reviewer failure: {e}")