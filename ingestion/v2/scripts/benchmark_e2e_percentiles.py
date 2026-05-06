import asyncio
import httpx
import time
import argparse
import orjson
from aiokafka import AIOKafkaConsumer

async def run_percentile_benchmark(platform_count: int, bootstrap: str):
    # 1. Setup
    topic = "raw-server-metrics"
    devices_per_platform = 11
    total_expected = platform_count * devices_per_platform
    
    # Load registry
    with open("/app/device_configs.json", "rb") as f:
        registry = orjson.loads(f.read())
    
    # Target standard platforms
    pcids = [f"PLATCUST{i:04d}" for i in range(1, platform_count + 1)]
    pcid_to_acid = {}
    for meta in registry.values():
        p = meta['platform_customer_id']
        if p in pcids and p not in pcid_to_acid:
            pcid_to_acid[p] = meta['application_customer_id']

    print(f"\n" + "="*60)
    print(f"E2E PERCENTILE BENCHMARK (CLEAN RUN): {platform_count} Platforms")
    print("="*60)

    # 2. Start Consumer
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset='latest')
    await consumer.start()
    await asyncio.sleep(2)
    print("Kafka Consumer Online.")

    platform_completion_times = {}
    platform_msg_counts = {pcid: 0 for pcid in pcids}
    
    async def consume_messages():
        received_total = 0
        while received_total < total_expected:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=300.0)
                val = msg.value.decode()
                for pcid in pcids:
                    if f'"{pcid}"' in val:
                        platform_msg_counts[pcid] += 1
                        if platform_msg_counts[pcid] == devices_per_platform:
                            platform_completion_times[pcid] = time.monotonic()
                        received_total += 1
                        break
                
                if received_total % 100 == 0 or received_total == total_expected:
                    print(f"   ...received {received_total}/{total_expected} in Kafka...")
            except asyncio.TimeoutError:
                break

    consume_task = asyncio.create_task(consume_messages())

    # 3. Trigger API
    async def trigger_export(client, pcid):
        acid = pcid_to_acid.get(pcid, "APPCUST0001")
        url = f"http://localhost:8001/pcid/{pcid}/acid/{acid}/telemetry/latest/export"
        start_trigger = time.monotonic()
        try:
            await client.post(url, timeout=120)
            return pcid, start_trigger
        except:
            return pcid, None

    print(f"Triggering {platform_count} API requests...")
    trigger_times = {}
    
    async with httpx.AsyncClient() as client:
        trigger_tasks = [trigger_export(client, pcid) for pcid in pcids]
        for completed_task in asyncio.as_completed(trigger_tasks):
            pcid, start_time = await completed_task
            if start_time:
                trigger_times[pcid] = start_time

    # 4. Wait for Kafka
    await consume_task
    await consumer.stop()

    # 5. Calculate Latencies
    latencies = []
    for pcid in pcids:
        if pcid in platform_completion_times and pcid in trigger_times:
            latencies.append(platform_completion_times[pcid] - trigger_times[pcid])
    
    if not latencies:
        print("Error: No latencies collected.")
        return

    latencies.sort()
    avg_latency = sum(latencies) / len(latencies)
    p50 = latencies[int(len(latencies) * 0.50)]
    p90 = latencies[int(len(latencies) * 0.90)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print("-" * 60)
    print(f"CLEAN RUN RESULTS ({platform_count} Platforms):")
    print(f"   P50:    {p50:.3f}s")
    print(f"   P90:    {p90:.3f}s")
    print(f"   P99:    {p99:.3f}s")
    print(f"   Avg:    {avg_latency:.3f}s")
    print("="*60 + "\n")

    # Append to file
    with open("/app/v2/scripts/final_ingestion_benchmarks.txt", "a") as f:
        f.write(f"\n--- {platform_count} Platforms CLEAN RUN PERCENTILES ---\n")
        f.write(f"P50: {p50:.3f}s | P90: {p90:.3f}s | P99: {p99:.3f}s\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", type=int, default=50)
    parser.add_argument("--broker", default="broker1:9092")
    args = parser.parse_args()
    asyncio.run(run_percentile_benchmark(args.platforms, args.broker))
