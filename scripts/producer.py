"""
TVMJNS — Kafka Producer (Telemetry Simulator)

Simulates IoT sensor telemetry and publishes to Kafka topic 'telemetry'.
Run this to generate test data for the streaming pipeline.

Usage:
    python scripts/producer.py
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "telemetry"
SENSORS = ["sensor_001", "sensor_002", "sensor_003", "sensor_004", "sensor_005"]
INTERVAL_SECONDS = 1  # Time between messages


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",  # Wait for all replicas to acknowledge
        retries=3,
    )


def generate_telemetry(sensor_id: str) -> dict:
    """Generate a fake telemetry reading."""
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2),
        "pressure": round(random.uniform(990.0, 1030.0), 2),
        "battery_level": round(random.uniform(10.0, 100.0), 1),
    }


def on_send_success(record_metadata):
    """Callback for successful message delivery."""
    print(
        f"✓ Sent to {record_metadata.topic} "
        f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
    )


def on_send_error(excp):
    """Callback for failed message delivery."""
    print(f"✗ Failed to send: {excp}")


def main():
    print("=" * 60)
    print("TVMJNS — Kafka Producer (Telemetry Simulator)")
    print("=" * 60)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Sensors: {', '.join(SENSORS)}")
    print(f"Interval: {INTERVAL_SECONDS}s")
    print("=" * 60)
    print("Press Ctrl+C to stop\n")

    try:
        producer = create_producer()
        print("✓ Connected to Kafka\n")
    except KafkaError as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        return

    message_count = 0
    try:
        while True:
            sensor_id = random.choice(SENSORS)
            telemetry = generate_telemetry(sensor_id)

            # Send with sensor_id as key (ensures same sensor goes to same partition)
            future = producer.send(TOPIC, key=sensor_id, value=telemetry)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            message_count += 1
            print(f"[{message_count}] {sensor_id}: temp={telemetry['temperature']}°C, "
                  f"humidity={telemetry['humidity']}%")

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\nStopping... sent {message_count} messages total.")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
