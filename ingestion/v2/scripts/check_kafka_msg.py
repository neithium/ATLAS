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
        auto_offset_reset='earliest',
        value_deserializer=lambda v: orjson.loads(v)
    )
    
    await consumer.start()
    try:
        print(f"📡 Waiting for next message on topic: {topic}...")
        async for msg in consumer:
            payload = msg.value
            created_at = payload.get("created_at", "")
            
                
            print("\n--- Kafka Message Metadata ---")
            print(f"Partition: {msg.partition}")
            print(f"Offset:    {msg.offset}")
            print(f"Key:       {msg.key.decode() if msg.key else 'None'}")
            
            print("\n--- Golden Record Payload (Full Schema) ---")
            for k, v in payload.items():
                if k not in ["data", "inventory_data"]:
                    print(f"{k}: {v}")
            
            print("\n--- Inventory Data ---")
            print(orjson.dumps(payload.get("inventory_data", {}), option=orjson.OPT_INDENT_2).decode())
            
            data_obj = payload.get("data", {})
            print("\n--- Data Aggregates ---")
            print(f"Average: {data_obj.get('Average')}")
            print(f"Maximum: {data_obj.get('Maximum')}")
            print(f"Minimum: {data_obj.get('Minimum')}")
            
            p_detail = data_obj.get("PowerDetail", [])
            print(f"\nPowerDetail points count: {len(p_detail)}")
            
            if p_detail:
                print("\n--- First PowerDetail Point (PascalCase Check) ---")
                print(orjson.dumps(p_detail[0], option=orjson.OPT_INDENT_2).decode())
                
            break # Found our target
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(check_msg())
