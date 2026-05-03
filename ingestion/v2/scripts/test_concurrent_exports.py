import asyncio
import httpx
import time
import argparse

# --- TOGGLE: MULTI-TENANT STRESS TEST (2000 devices per platform) ---
PCIDS = [f"PLATCUST{i:04d}" for i in range(1, 6)]
ACIDS = [] # Will be populated dynamically for the test

async def trigger_export(client, pcid, acid):
    url = f"http://localhost/pcid/{pcid}/acid/{acid}/telemetry/latest/export"
    start = time.monotonic()
    try:
        response = await client.post(url, timeout=300)
        elapsed = time.monotonic() - start
        return {
            "pcid": pcid,
            "latency": elapsed,
            "status": response.status_code,
            "error": None
        }
    except Exception as e:
        elapsed = time.monotonic() - start
        return {
            "pcid": pcid,
            "latency": elapsed,
            "status": 500,
            "error": str(e)
        }

async def run_benchmark(platform_count: int):
    # Load registry to find exact device count for this platform
    import orjson
    with open("d:/PowerPulse/atlas/ingestion/device_configs.json", "rb") as f:
        registry = orjson.loads(f.read())
    
    # Discovery: Find all ACIDs for the target platforms
    target_pcids = PCIDS[:platform_count]
    found_acids = []
    for pcid in target_pcids:
        acids = sorted(list(set(meta["application_customer_id"] for meta in registry.values() if meta["platform_customer_id"] == pcid)))
        for acid in acids:
            found_acids.append((pcid, acid))
    
    device_count = len([did for did, meta in registry.items() if meta.get("platform_customer_id") == PCIDS[0]])
    
    print(f"\n" + "="*60)
    print(f"STREAMING BENCHMARK: {device_count} DEVICES PER PLATFORM ({len(found_acids)} Requests)")
    print("="*60)
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for pcid, acid in found_acids:
            tasks.append(trigger_export(client, pcid, acid))
        
        t_batch_start = time.monotonic()
        results = await asyncio.gather(*tasks)
        t_batch_total = time.monotonic() - t_batch_start
        
        success_count = sum(1 for r in results if r["status"] == 200)
        failed_count = len(results) - success_count
        latencies = sorted(r["latency"] for r in results) if results else [0]
        avg_latency = sum(latencies) / len(latencies)
        max_latency = latencies[-1]
        min_latency = latencies[0]
        
        # Calculate percentiles
        p50 = latencies[int(len(latencies) * 0.50)]
        p90 = latencies[int(len(latencies) * 0.90)]
        p99 = latencies[int(len(latencies) * 0.99)]
        
        for r in results:
            status_symbol = "[OK]" if r["status"] == 200 else "[FAIL]"
            error_msg = f" | Error: {r['error']}" if r['error'] else ""
            print(f"{status_symbol} {r['pcid']}: {r['latency']:.3f}s{error_msg}")
            
        print("-" * 60)
        print(f"THROUGHPUT: {device_count} devices processed in {t_batch_total:.3f}s")
        print(f"LATENCY   : Min: {min_latency:.3f}s | Avg: {avg_latency:.3f}s | Max: {max_latency:.3f}s")
        print(f"PERCENTILE: Median (p50): {p50:.3f}s | p90: {p90:.3f}s | p99: {p99:.3f}s")
        print(f"SUCCESS   : {success_count}/{len(results)} platforms")
        print("="*60 + "\n")
        
        # Save exact text printout to file
        import os
        txt_file = os.path.join(os.path.dirname(__file__), "benchmark_results.txt")
        with open(txt_file, "a", encoding="utf-8") as f:
            f.write("="*60 + "\n")
            f.write(f"STREAMING BENCHMARK: {device_count} DEVICES ({platform_count} Platforms)\n")
            f.write("-" * 60 + "\n")
            f.write(f"THROUGHPUT: {device_count} devices processed in {t_batch_total:.3f}s\n")
            f.write(f"LATENCY   : Min: {min_latency:.3f}s | Avg: {avg_latency:.3f}s | Max: {max_latency:.3f}s\n")
            f.write(f"PERCENTILE: Median (p50): {p50:.3f}s | p90: {p90:.3f}s | p99: {p99:.3f}s\n")
            f.write(f"SUCCESS   : {success_count}/{len(results)} platforms\n")
            f.write("="*60 + "\n\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Benchmark Tool")
    parser.add_argument("--platforms", type=int, default=10, choices=range(1, 501), 
                        help="Number of platforms to trigger concurrently (1-500)")
    args = parser.parse_args()
    
    asyncio.run(run_benchmark(args.platforms))
