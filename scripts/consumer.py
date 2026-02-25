"""
TVMJNS — Kafka Consumer (Telemetry Reader)

Consumes telemetry messages from Kafka topic 'telemetry' and prints them.
Can optionally store to PostgreSQL.

Usage:
    python scripts/consumer.py
"""

import json
import signal
import sys
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "telemetry"
CONSUMER_GROUP = "telemetry-consumers"

# Global flag for graceful shutdown
running = True


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\n\nShutdown signal received...")
    running = False


def create_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",  # Start from beginning if no committed offset
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        consumer_timeout_ms=1000,  # Return from poll after 1s if no messages
    )


def format_telemetry(msg) -> str:
    """Format a telemetry message for display."""
    data = msg.value
    return (
        f"[{msg.partition}:{msg.offset}] "
        f"{data['sensor_id']} @ {data['timestamp'][:19]} | "
        f"temp={data['temperature']:5.1f}°C  "
        f"humidity={data['humidity']:5.1f}%  "
        f"pressure={data['pressure']:7.1f}hPa  "
        f"battery={data['battery_level']:5.1f}%"
    )


def main():
    print("=" * 80)
    print("TVMJNS — Kafka Consumer (Telemetry Reader)")
    print("=" * 80)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print("=" * 80)
    print("Press Ctrl+C to stop\n")

    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    try:
        consumer = create_consumer()
        print("✓ Connected to Kafka\n")
    except KafkaError as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        return

    message_count = 0
    try:
        while running:
            # Poll returns immediately if messages available, or after timeout
            for message in consumer:
                if not running:
                    break
                message_count += 1
                print(format_telemetry(message))

    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()
        print(f"\nConsumed {message_count} messages total.")
        print("Consumer closed.")


if __name__ == "__main__":
    main()
