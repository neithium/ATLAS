import asyncio
import os
import httpx
import time
import argparse
import orjson
from aiokafka import AIOKafkaConsumer

async def run_e2e_multi_benchmark(platform_count: int, heavy_pcid: str = None):
    topic = "raw-server-metrics"
    bootstrap = "broker1:9092"
    
    # 1. Load registry to find correct ACID and device counts
    import orjson
    # Use relative path to work both on host and inside container
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "device_configs.json")
    if not os.path.exists(config_path):
        # Fallback for container absolute path
        config_path = "/app/device_configs.json"
        
    with open(config_path, "rb") as f:
        registry = orjson.loads(f.read())
    
    # Map all unique PCID/ACID pairs
    hierarchy_counts = {} # Key: (pcid, acid) -> count
    for meta in registry.values():
        pcid = meta.get('platform_customer_id')
        acid = meta.get('application_customer_id')
        if pcid and acid:
            h_key = (pcid, acid)
            hierarchy_counts[h_key] = hierarchy_counts.get(h_key, 0) + 1
            
    # Sort hierarchies by device count descending
    sorted_hierarchies = sorted(hierarchy_counts.keys(), key=lambda x: hierarchy_counts[x], reverse=True)
    
    # Selection logic: prioritize heavy platform if requested
    targets = []
    if heavy_pcid:
        # Find all hierarchies for this heavy PCID
        heavy_hs = [h for h in sorted_hierarchies if h[0] == heavy_pcid]
        for h in heavy_hs:
            targets.append((h[0], h[1], hierarchy_counts[h]))
        
        remaining = [h for h in sorted_hierarchies if h[0] != heavy_pcid]
        for h in remaining:
            if len(targets) >= platform_count: break
            targets.append((h[0], h[1], hierarchy_counts[h]))
    else:
        for h in sorted_hierarchies[:platform_count]:
            targets.append((h[0], h[1], hierarchy_counts[h]))
            
    total_expected = sum(t[2] for t in targets)
    print(f"🚀 Starting E2E Multi-Platform Benchmark | Platforms: {len(targets)} | Total Devices: {total_expected}")
    for pcid, acid, count in targets:
        print(f"  - {pcid} ({acid}): {count} devices")
    print("-" * 60)

    # 2. Start Kafka Consumer to track end-to-end delivery
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset='latest'
    )
    await consumer.start()
    
    # Warm up: skip any existing messages
    try:
        await asyncio.wait_for(consumer.getmany(timeout_ms=1000), timeout=2.0)
    except:
        pass

    # 3. Trigger API Exports
    t_start = time.monotonic()
    async with httpx.AsyncClient(timeout=120.0) as client:
        trigger_tasks = []
        for pcid, acid, _ in targets:
            url = f"http://localhost:8001/pcid/{pcid}/acid/{acid}/telemetry/latest/export"
            trigger_tasks.append(client.post(url))
        
        print(f"📡 Triggering {len(targets)} exports in parallel...")
        responses = await asyncio.gather(*trigger_tasks)
        
        for i, r in enumerate(responses):
            if r.status_code != 200:
                print(f"  ❌ Failed to trigger {targets[i][0]}: {r.status_code}")
            else:
                print(f"  ✅ Triggered {targets[i][0]}")

    # 4. Wait for Kafka Messages
    print(f"⌛ Waiting for {total_expected} devices to reach Kafka...")
    received_count = 0
    start_wait = time.monotonic()
    
    try:
        while received_count < total_expected:
            # Check timeout (5 minutes max)
            if time.monotonic() - start_wait > 300:
                print("❌ Timeout reached waiting for Kafka messages.")
                break
                
            msg_batch = await consumer.getmany(timeout_ms=1000)
            for tp, messages in msg_batch.items():
                received_count += len(messages)
            
            if received_count > 0:
                print(f"  📥 Received {received_count}/{total_expected} ({received_count/total_expected*100:.1f}%)", end="\r")
    finally:
        await consumer.stop()

    t_end = time.monotonic()
    total_time = t_end - t_start
    
    # 5. Report Results
    print("\n" + "=" * 60)
    print("E2E MULTI-PLATFORM BENCHMARK")
    print(f"Platforms: {len(targets)} | Total Devices: {total_expected}")
    print(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    print(f"   Total Time:        {total_time:.3f}s")
    print(f"   Devices Received:  {received_count}/{total_expected}")
    if total_time > 0:
        throughput = (received_count * 2016) / total_time
        print(f"   System Throughput: {throughput:,.0f} points/sec")
    print("=" * 60)
    
    # Save results to file
    with open("/app/v2/scripts/benchmark_results.txt", "a") as f:
        f.write(f"\n============================================================\n")
        f.write(f"E2E MULTI-PLATFORM BENCHMARK\n")
        f.write(f"Platforms: {len(targets)} | Total Devices: {total_expected}\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"------------------------------------------------------------\n")
        f.write(f"   Total Time:        {total_time:.3f}s\n")
        f.write(f"   Devices Received:  {received_count}/{total_expected}\n")
        if total_time > 0:
            f.write(f"   System Throughput: {throughput:,.0f} points/sec\n")
        f.write(f"============================================================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2E Multi-Platform Benchmark")
    parser.add_argument("--platforms", type=int, default=5, help="Number of platforms to test")
    parser.add_argument("--heavy", type=str, default=None, help="PCID of a heavy platform to include")
    args = parser.parse_args()
    
    asyncio.run(run_e2e_multi_benchmark(args.platforms, args.heavy))
