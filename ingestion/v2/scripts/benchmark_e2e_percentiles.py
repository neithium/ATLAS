import asyncio
import httpx
import time
import argparse
import orjson
from aiokafka import AIOKafkaConsumer

async def run_percentile_benchmark(platform_count: int):
    # 1. Setup
    topic = "raw-server-metrics"
    bootstrap = "127.0.0.1:9064"
    devices_per_platform = 11
    total_expected = platform_count * devices_per_platform
    
    # Load registry
    with open("d:/PowerPulse/atlas/ingestion/device_configs.json", "rb") as f:
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
    
    pcids_set = set(pcids)
    
    async def consume_messages():
        received_total = 0
        print(f"DEBUG: Monitoring for {len(pcids_set)} PCIDs. Sample: {list(pcids_set)[:5]}")
        while received_total < total_expected:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=300.0)
                data = orjson.loads(msg.value)
                pcid = data.get('platform_customer_id')
                
                if received_total < 10:
                    print(f"DEBUG: Received msg for PCID={pcid}")

                if pcid in pcids_set:
                    platform_msg_counts[pcid] += 1
                    if platform_msg_counts[pcid] == devices_per_platform:
                        platform_completion_times[pcid] = time.monotonic()
                    received_total += 1
                else:
                    if received_total < 100:
                        print(f"DEBUG: PCID {pcid} not in target set!")
                
                if received_total % 100 == 0 or received_total == total_expected:
                    print(f"   ...received {received_total}/{total_expected} in Kafka...")
            except asyncio.TimeoutError:
                print("DEBUG: Kafka Consumer Timeout!")
                break

    consume_task = asyncio.create_task(consume_messages())

    # 3. Trigger API
    async def trigger_export(client, pcid):
        acid = pcid_to_acid.get(pcid, "APPCUST0001")
        url = f"http://localhost:8001/pcid/{pcid}/acid/{acid}/telemetry/latest/export"
        start_trigger = time.monotonic()
        try:
            resp = await client.post(url, timeout=120)
            if resp.status_code != 200:
                print(f"DEBUG: API Failed for {pcid}: {resp.status_code} {resp.text}")
                return pcid, None
            return pcid, start_trigger
        except Exception as e:
            print(f"DEBUG: API Exception for {pcid}: {e}")
            return pcid, None

    print(f"Triggering {platform_count} API requests...")
    trigger_times = {}
    
    # Increase limits for high-concurrency triggering
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:
        trigger_tasks = [trigger_export(client, pcid) for pcid in pcids]
        for completed_task in asyncio.as_completed(trigger_tasks):
            pcid, start_time = await completed_task
            if start_time:
                trigger_times[pcid] = start_time

    # 4. Wait for Kafka
    print("Waiting for all Kafka messages to be received...")
    await consume_task
    await consumer.stop()

    # 5. Calculate Latencies
    latencies = []
    print(f"DEBUG: Total platforms with completion times: {len(platform_completion_times)}")
    print(f"DEBUG: Total platforms with trigger times: {len(trigger_times)}")
    
    for pcid in pcids:
        if pcid in platform_completion_times and pcid in trigger_times:
            latencies.append(platform_completion_times[pcid] - trigger_times[pcid])
        elif pcid in trigger_times:
            # Check why it didn't complete
            pass
    
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
    with open("d:/PowerPulse/atlas/ingestion/v2/scripts/final_ingestion_benchmarks.txt", "a") as f:
        f.write(f"\n--- {platform_count} Platforms CLEAN RUN PERCENTILES ---\n")
        f.write(f"P50: {p50:.3f}s | P90: {p90:.3f}s | P99: {p99:.3f}s\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(run_percentile_benchmark(args.platforms))
