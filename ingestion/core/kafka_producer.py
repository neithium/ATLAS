"""
kafka_producer.py — Unified Kafka Producer for ATLAS Ingestion
──────────────────────────────────────────────────────────────
Integration: atlas-ingestion → broker1 (Nandini's Kafka KRaft)
Topic: raw-server-metrics (12 partitions)

Schema Contract: MUST MATCH input_schema.py exactly via schema_builder.py
  - All messages must include 48 fields + inventory_data
  - All producers use unified schema builder for consistency
  - No exceptions to this schema ever
"""

import os
try:
    import orjson as json
except ImportError:
    import json
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

# Import unified schema builder
from schema_builder import build_48_field_golden_record, build_batch_power_detail

log = logging.getLogger(__name__)

# ─── Aligned with docker-compose.yml and Nandini's broker1 ───
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "broker1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-server-metrics")

_producer: Optional['AIOKafkaProducer'] = None

async def init_kafka():
    """Initialize the Kafka producer with retry logic to connect to broker1."""
    global _producer
    if not KAFKA_AVAILABLE:
        log.warning("[kafka] aiokafka not installed. Kafka integration disabled.")
        return

    retry_count = 0
    max_retries = 10
    retry_delay = 5

    while retry_count < max_retries:
        try:
            _producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                compression_type="lz4",
                linger_ms=50,                  # Batch optimization
                max_request_size=8388608,       # 8MB for large device bursts
                request_timeout_ms=120000,      # 2 minutes timeout
                connections_max_idle_ms=540000,
                value_serializer=lambda v: v if isinstance(v, bytes) else (
                    json.dumps(v) if isinstance(v, dict) else str(v).encode('utf-8')
                )
            )
            await _producer.start()
            log.info(f"🛰️ [KAFKA] Connected to {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
            return
        except Exception as e:
            retry_count += 1
            log.warning(f"[kafka] Attempt {retry_count}/{max_retries}: Failed to connect to {KAFKA_BOOTSTRAP_SERVERS} ({e}). Retrying in {retry_delay}s...")
            
            # Robustly close the failed producer to prevent unclosed resource errors
            if _producer:
                try:
                    await _producer.stop()
                except:
                    pass
            _producer = None
            await asyncio.sleep(retry_delay)

    log.error(f"[kafka] Critical: Could not connect to Kafka after {max_retries} attempts.")

async def close_kafka():
    """Close the Kafka producer."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        log.info("[kafka] Producer closed.")
        _producer = None

def is_connected():
    """Check if the Kafka producer is initialized and connected."""
    return _producer is not None

async def push_to_kafka(device_id: str, reading: dict):
    """
    Push a single reading to Kafka (raw-server-metrics topic).
    Sends the FULL 48-field Golden Schema matching input_schema.py.
    """
    if _producer is None:
        return

    from config.devices import DEVICES
    meta = DEVICES.get(device_id, {})
    inv = meta.get("inventory_data", {})

    payload = _build_48_field_record(device_id, reading, meta, inv)

    try:
        await _producer.send_and_wait(
            KAFKA_TOPIC, 
            value=payload, 
            key=device_id.encode('utf-8')
        )
        log.debug(f"[kafka] Pushed 48-field record for {device_id} to {KAFKA_TOPIC}")
    except Exception as e:
        log.error(f"[kafka] Failed to push {device_id}: {e}")

async def push_batch_to_kafka(device_readings_batch: list):
    """
    High-performance batch push. Accepts a list of (device_id, reading_dict) tuples.
    Sends full 48-field records then flushes once for maximum throughput.
    """
    if _producer is None:
        log.warning("[kafka] Cannot push batch: Producer not initialized.")
        return 0

    from config.devices import DEVICES
    
    sent = 0
    for device_id, reading in device_readings_batch:
        meta = DEVICES.get(device_id, {})
        inv = meta.get("inventory_data", {})
        payload = _build_48_field_record(device_id, reading, meta, inv)
        
        try:
            await _producer.send(KAFKA_TOPIC, value=payload, key=device_id.encode('utf-8'))
            sent += 1
        except Exception as e:
            log.error(f"[kafka] Batch push failed for {device_id}: {e}")
    
    await _producer.flush()
    log.info(f"[kafka] Batch pushed {sent}/{len(device_readings_batch)} 48-field records to {KAFKA_TOPIC}")
    return sent

async def push_history_batch_to_kafka(acid: str, history_data: dict, devices_registry: dict):
    """
    High-performance batch exporter for REST API triggered exports.
    Sends full 48-field Golden Schema records to Kafka using unified builder.
    """
    if _producer is None:
        log.warning("[kafka] Cannot export batch: Producer not initialized.")
        return

    start_time = datetime.now()
    total_devices = len(history_data)
    processed = 0
    
    for device_id, raw_readings in history_data.items():
        meta = devices_registry.get(device_id, {})
        inv = meta.get("inventory_data", {})
        
        # Build PowerDetail array from all readings using unified builder
        power_detail_list, avg_watts, max_watts, min_watts = build_batch_power_detail(raw_readings)
        
        # Build message using unified schema builder
        payload = build_48_field_golden_record(
            device_id=device_id,
            reading=raw_readings[-1] if raw_readings else {},
            device_metadata=meta,
            inventory_data=inv,
            power_detail_list=power_detail_list
        )
        
        # Override aggregates
        payload["data"]["Average"] = avg_watts
        payload["data"]["Maximum"] = max_watts
        payload["data"]["Minimum"] = min_watts
        
        for attempt in range(3):
            try:
                await _producer.send_and_wait(
                    KAFKA_TOPIC, 
                    value=payload, 
                    key=device_id.encode('utf-8')
                )
                break
            except Exception as e:
                if attempt == 2:
                    log.error(f"[kafka] Failed to push {device_id} after retries: {e}")
                else:
                    log.warning(f"[kafka] Timeout for {device_id}, retrying ({attempt+1}/3)...")
                    await asyncio.sleep(2)
        
        processed += 1
        if processed % 1000 == 0:
            log.info(f"[kafka] Export progress: {processed}/{total_devices} devices pushed")

    duration = (datetime.now() - start_time).total_seconds()
    log.info(f"[kafka] Batch Export Complete: {processed} devices ({total_devices} total) in {duration:.2f}s")


def _build_48_field_record(device_id: str, reading: dict, meta: dict, inv: dict) -> dict:
    """
    Builds the full 48-field Golden Schema record matching input_schema.py.
    NOW DELEGATES TO UNIFIED SCHEMA BUILDER FOR CONSISTENCY.
    """
    return build_48_field_golden_record(
        device_id=device_id,
        reading=reading,
        device_metadata=meta,
        inventory_data=inv if inv else None
    )

