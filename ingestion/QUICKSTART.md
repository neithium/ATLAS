# PowerPulse V3: Quick Start Guide 🚀

Follow these steps in order to go from a fresh clone to a fully-warmed, high-performance ingestion engine in minutes.

---

## 1. Spin up Infrastructure
Initialize the TimescaleDB, Redis, and Kafka containers:
```bash
docker compose up -d
```

## 2. Generate a Simulated Fleet
Create a virtual registry of 10,000 devices (or adjust as needed):
```bash
docker exec atlas-ingestion python3 /app/v2/scripts/generate_registry.py --devices 10000
```

> [!TIP]
> **Customizing Your Scale**: You can define your own fleet hierarchy by adjusting the parameters. For example, to create a 5,000-device fleet across 10 platforms:
> `docker exec atlas-ingestion python3 /app/v2/scripts/generate_registry.py --pcids 10 --acids 5 --devices 100`

## 3. Prefill Sample Telemetry (Simulate History)
Populate TimescaleDB with 7 days of historical telemetry for the simulated fleet:
```bash
docker exec atlas-ingestion python3 /app/v2/scripts/prefill_tsdb.py --days 7
```

## 4. Warm the High-Performance Cache
"Freeze" the TSDB data into the local Parquet cache for sub-second API discovery. This is the secret to the system's speed!
```bash
docker exec -it atlas-ingestion python3 /app/v2/scripts/backfill_cache.py --days=7
```

## 5. Run an End-to-End Benchmark
Verify that the system is hitting the production-grade **163k pts/sec** threshold:
```bash
docker exec atlas-ingestion python3 /app/v2/scripts/benchmark_e2e_multi.py --platforms 10
```

---

## 🛠 Useful Commands

### Inspect Live Kafka Streams
```bash
docker exec atlas-ingestion python3 /app/v2/scripts/check_kafka_msg.py
```

### Manual Lakehouse Consolidation
```bash
docker exec -it atlas-ingestion python3 /app/v2/scripts/bench_daily_job.py
```

### System Health
```bash
curl http://localhost:8001/health
```

---
> **Target Performance**: ~163,000 pts/sec  
> **Target Latency**: < 20s for 1k Device Cluster  
> 🚀🏁🏆🥇
