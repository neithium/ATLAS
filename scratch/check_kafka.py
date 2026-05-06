import asyncio
from aiokafka import AIOKafkaConsumer
import orjson
import os

async def consume_one():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw-server-metrics")
    
    print(f"Connecting to {bootstrap} for topic {topic}...")
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id="verifier-group"
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=10)
        print("\n=== MESSAGE HEADERS ===")
        print(f"Key: {msg.key}")
        print(f"Offset: {msg.offset}")
        print("\n=== MESSAGE VALUE (Prettified) ===")
        print(json.dumps(orjson.loads(msg.value), indent=2))
    except asyncio.TimeoutError:
        print("Timeout: No messages found in topic.")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    import json
    asyncio.run(consume_one())
