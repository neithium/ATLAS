import os
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

try:
    from aiokafka import AIOKafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_READINGS = os.getenv("KAFKA_TOPIC_READINGS", "telemetry.readings")

_producer: Optional['AIOKafkaProducer'] = None

async def init_kafka():
    """Initialize the Kafka producer if enabled and available."""
    global _producer
    if not KAFKA_AVAILABLE:
        log.warning("[kafka] aiokafka not installed. Kafka integration disabled.")
        return

    try:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            compression_type="lz4",
            max_request_size=2097152,  # 2MB limit for large reading batches
            value_serializer=lambda v: v if isinstance(v, bytes) else str(v).encode('utf-8')
        )
        await _producer.start()
        log.info(f"[kafka] Connected to {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC_READINGS}")
    except Exception as e:
        log.error(f"[kafka] Failed to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        _producer = None

async def close_kafka():
    """Close the Kafka producer."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        log.info("[kafka] Producer closed.")
        _producer = None

async def push_to_kafka(device_id: str, reading: dict):
    """
    Push a single reading to Kafka.
    """
    if _producer is None:
        return

    # Add device_id to the reading payload so consumers know where it came from
    payload = dict(reading)
    if "device_id" not in payload:
        payload["device_id"] = device_id

    try:
        await _producer.send_and_wait(KAFKA_TOPIC_READINGS, value=payload, key=device_id.encode('utf-8'))
        log.debug(f"[kafka] Pushed {device_id} successfully")
    except Exception as e:
        log.error(f"[kafka] Failed to push {device_id} to Kafka: {e}")

async def push_history_batch_to_kafka(acid: str, history_data: dict, devices_registry: dict):
    """
    High-performance batch exporter. 
    Stitches raw Redis strings into metadata envelopes and fires to Kafka.
    """
    if _producer is None:
        log.warning("[kafka] Cannot export batch: Producer not initialized.")
        return

    start_time = datetime.now()
    tasks = []
    
    for device_id, raw_readings in history_data.items():
        # Metadata lookup from in-memory registry
        meta = devices_registry.get(device_id, {})
        pcid = meta.get("platform_customer_id", "UNKNOWN")
        server = meta.get("server_name", "UNKNOWN")
        
        # Zero-Parse Envelope Stitching (Blazing Fast)
        # Wrap the raw strings in a valid JSON envelope
        envelope = (
            f'{{"device_id":"{device_id}",'
            f'"platform_customer_id":"{pcid}",'
            f'"application_customer_id":"{acid}",'
            f'"server_name":"{server}",'
            f'"exported_at":"{datetime.now(timezone.utc).isoformat()}",'
            f'"readings":[{",".join(raw_readings)}]}}'
        )
        
        # Push to Kafka buffer (non-blocking)
        tasks.append(
            _producer.send(
                KAFKA_TOPIC_READINGS, 
                value=envelope.encode('utf-8'), 
                key=device_id.encode('utf-8')
            )
        )

    # Wait for all 1,660 messages to hit the Kafka buffer
    if tasks:
        await asyncio.gather(*tasks)
        
    duration = (datetime.now() - start_time).total_seconds()
    log.info(f"[kafka] Batch Export Complete: {len(tasks)} devices in {duration:.2f}s")
