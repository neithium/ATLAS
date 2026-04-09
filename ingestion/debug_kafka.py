import json, asyncio, os
from aiokafka import AIOKafkaConsumer

async def run():
    # 🛰️ Switched to 'latest' to see the Golden Schema
    consumer = AIOKafkaConsumer('telemetry-export-v2', bootstrap_servers='broker1:9092', auto_offset_reset='latest')
    await consumer.start()
    print('🛰️ Waiting for a NEW Golden Message... (TRIGGER THE API NOW!)')
    try:
        msg = await consumer.getone()
        data = json.loads(msg.value)
        
        # ── Verification of NESTING ──
        summary = {
            'HEADER': 'GOLDEN SPARK SCHEMA TEST',
            'device_id': data.get('device_id'),
            'report_id': data.get('report_id'), 
            'HAS_DATA_STRUCT': 'data' in data,
            'HAS_INVENTORY_STRUCT': 'inventory_data' in data,
            'data_struct_keys': list(data.get('data', {}).keys()),
            'powerdetail_sample': data.get('data', {}).get('PowerDetail', [{}])[0]
        }
        print('\n🚀 SUCCESS! MSG RECEIVED:')
        print(json.dumps(summary, indent=2))
        
        print('\n--- TOP 500 CHARS OF RAW PAYLOAD ---')
        print(json.dumps(data, indent=2)[:500] + '...')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(run())
