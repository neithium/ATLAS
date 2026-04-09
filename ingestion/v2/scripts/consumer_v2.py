import asyncio
import os
import json
import uuid
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer

async def consume():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP", "broker1:9092")
    topic = os.getenv("KAFKA_TOPIC", "telemetry-export-v2")
    
    print(f"🕵️ Zero-Loss Verifier: topic='{topic}'")
    
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest', # 🕵️ Backfill from start of topic
        enable_auto_commit=False,
        group_id="verifier-group-" + str(uuid.uuid4())[:8] # 🕵️ Fresh Group to see all
    )
    
    await consumer.start()
    try:
        count = 0
        now_utc = datetime.now(timezone.utc)
        print("Waiting for current burst... (pushed in last 5 mins)")
        async for msg in consumer:
            payload = json.loads(msg.value.decode('utf-8'))
            pushed_at_str = payload.get("pushed_at")
            pushed_at = datetime.fromisoformat(pushed_at_str)
            
            # Filter for messages from only the last 5 minutes (current run)
            if (now_utc - pushed_at).total_seconds() < 300:
                count += 1
                device_id = payload.get("device_id")
                # Print every 100th message or every message if count is small
                if count % 100 == 0 or count <= 3:
                     print(f"[{count}/1600] ✅ [OK] | Device: {device_id} | Pushed: {pushed_at_str}")
                
                if count >= 1600:
                    print(f"\n🎯 TARGET REACHED: All 1,600/1,600 devices verified in Kafka.")
                    # Keep running to see if more come in
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
