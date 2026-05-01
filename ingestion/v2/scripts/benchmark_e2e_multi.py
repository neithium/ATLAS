import asyncio
import httpx
import time
import argparse
import orjson
from aiokafka import AIOKafkaConsumer

async def run_e2e_multi_benchmark(platform_count: int, heavy_pcid: str = None):
    topic = "raw-server-metrics"
    bootstrap = "127.0.0.1:9064"
    
    # 1. Setup targets
    if heavy_pcid:
        pcids = [heavy_pcid]
        if heavy_pcid == "PLATCUST10K":
            expected_per_platform = 10000
        else:
            # Fallback check for any other heavy hitter
            expected_per_platform = 4000 # Default for "4k" request if we mock it
        platform_count = 1
    else:
        pcids = [f"PLATCUST{i:04d}" for i in range(1, platform_count + 1)]
        expected_per_platform = 11
        
    total_expected = platform_count * expected_per_platform
    
    print(f"\n" + "="*60)
    print(f"--- MULTI-PLATFORM E2E BENCHMARK (API -> DB -> KAFKA) ---")
    if heavy_pcid:
        print(f"Target: Heavy Hitter {heavy_pcid} ({total_expected} devices)")
    else:
        print(f"Target: {platform_count} Platforms ({total_expected} devices total)")
    print("="*60)
    
    # 2. Setup Kafka Consumer
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset='latest'
    )
    await consumer.start()
    print("Kafka Consumer Online (Waiting for start signal...)")
    await asyncio.sleep(2)
    
    msg_count = 0
    
    # 3. Consumer Task
    async def consume_messages():
        nonlocal msg_count
        while msg_count < total_expected:
            try:
                # Use a larger timeout for massive bursts
                msg = await asyncio.wait_for(consumer.getone(), timeout=300.0)
                msg_count += 1
                if msg_count % 100 == 0 or msg_count == total_expected:
                    print(f"   ...received {msg_count}/{total_expected} in Kafka...")
            except asyncio.TimeoutError:
                print(f"\n[TIMEOUT] Still waiting for {total_expected - msg_count} messages...")
                break

    consume_task = asyncio.create_task(consume_messages())
    
    # 4. Trigger API Exports
    # Load registry to find correct ACID
    import orjson
    with open("d:/PowerPulse/atlas/ingestion/device_configs.json", "rb") as f:
        registry = orjson.loads(f.read())
    
    # Map PCID to its first found ACID
    pcid_to_acid = {}
    for meta in registry.values():
        p = meta['platform_customer_id']
        a = meta['application_customer_id']
        if p not in pcid_to_acid:
            pcid_to_acid[p] = a

    async def trigger_export(client, pcid):
        acid = pcid_to_acid.get(pcid, "APPCUST0001")
        url = f"http://localhost:8001/pcid/{pcid}/acid/{acid}/telemetry/latest/export"
        
        try:
            resp = await client.post(url, timeout=120)
            if resp.status_code != 200:
                print(f"   [!] Trigger failed for {pcid}: {resp.status_code} {resp.text}")
            return resp.status_code
        except Exception as e:
            print(f"   [!] Trigger error for {pcid}: {e}")
            return 500

    print(f"\nTriggering {platform_count} API requests...")
    t_start = time.monotonic()
    
    async with httpx.AsyncClient() as client:
        trigger_tasks = [trigger_export(client, pcid) for pcid in pcids]
        responses = await asyncio.gather(*trigger_tasks)
        
    t_api_trigger = time.monotonic() - t_start
    success_triggers = sum(1 for s in responses if s == 200)
    print(f"DONE {success_triggers}/{platform_count} Exports Triggered in {t_api_trigger:.3f}s")
    print(f"Waiting for data flow to Kafka...")
    
    # 5. Wait for Kafka Completion
    await consume_task
    t_total = time.monotonic() - t_start
    
    await consumer.stop()
    
    # 6. Report
    points_per_device = 2016
    total_points = msg_count * points_per_device
    throughput = total_points / t_total if t_total > 0 else 0
    
    print("-" * 60)
    print(f"FINAL E2E RESULTS ({platform_count} Platforms):")
    print(f"   Total Time:        {t_total:.3f}s")
    print(f"   Trigger Overhead:  {t_api_trigger:.3f}s")
    print(f"   DB + Kafka Sync:   {t_total - t_api_trigger:.3f}s")
    print(f"   Devices Received:  {msg_count}/{total_expected}")
    print(f"   System Throughput: {throughput:,.0f} points/sec")
    print("="*60 + "\n")
    
    # Append to results file
    import os
    txt_file = "ingestion/v2/scripts/benchmark_results.txt"
    with open(txt_file, "a", encoding="utf-8") as f:
        f.write("="*60 + "\n")
        f.write(f"E2E MULTI-PLATFORM BENCHMARK\n")
        f.write(f"Platforms: {platform_count} | Total Devices: {total_expected}\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("-" * 60 + "\n")
        f.write(f"   Total Time:        {t_total:.3f}s\n")
        f.write(f"   Devices Received:  {msg_count}/{total_expected}\n")
        f.write(f"   System Throughput: {throughput:,.0f} points/sec\n")
        f.write("="*60 + "\n\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", type=int, default=10)
    parser.add_argument("--heavy", type=str, default=None, help="PCID of heavy hitter (e.g. PLATCUST10K)")
    args = parser.parse_args()
    
    asyncio.run(run_e2e_multi_benchmark(args.platforms, args.heavy))
