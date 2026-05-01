from aiokafka import AIOKafkaConsumer
import asyncio
import orjson
import os

async def check_msg():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "broker1:9092")
    topic = "raw-server-metrics"
    
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset='latest',
        value_deserializer=lambda v: orjson.loads(v)
    )
    
    await consumer.start()
    try:
        print(f"📡 Waiting for next message on topic: {topic}...")
        async for msg in consumer:
            payload = msg.value
                
            print("\n--- Kafka Message Metadata ---")
            print(f"Partition: {msg.partition}")
            print(f"Offset:    {msg.offset}")
            print(f"Key:       {msg.key.decode() if msg.key else 'None'}")
            
            print("\n--- Golden Record Payload (Top Level) ---")
            for k, v in payload.items():
                if k != "data" and k != "inventory_data":
                    print(f"{k}: {v}")
            
            data_obj = payload.get("data", {})
            p_detail = data_obj.get("PowerDetail", [])
            print(f"\nPowerDetail points count: {len(p_detail)}")
            
            if p_detail:
                print("\n--- First PowerDetail Point ---")
                print(orjson.dumps(p_detail[0], option=orjson.OPT_INDENT_2).decode())
                
            break # Found our target
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(check_msg())
