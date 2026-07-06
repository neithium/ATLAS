# PowerPulse V3 Unified Ingestion Engine (80,000+ Devices)

![Ingestion Architecture](../../ingestion/assets/Ingestion%20(1).png)

This service implements a production-grade **Unified Telemetry Ingestion Architecture** designed to handle **80,000 devices** at 5-minute intervals. It leverages a multi-tier persistence strategy to achieve extreme throughput while maintaining strict data durability.

## Core Purpose
The **Ingestion Layer** is the critical "front door" of the PowerPulse architecture. Its main purposes are:
1. **Shock Absorption:** Safely absorb massive bursts of telemetry traffic (e.g., thousands of servers reporting their power metrics at the exact same second) without dropping data.
2. **Data Normalization:** Clean and structure chaotic incoming metrics into a unified 48-field "Golden Schema" before it enters the data lake.
3. **Decoupled Hydration:** Merge fast-changing power metrics with static hardware configurations (Intel/AMD profiles) efficiently in-memory.
4. **Downstream Delivery:** Act as a reliable bridge, pushing pristine, fully hydrated data into **Kafka** (for real-time ML anomaly detection) and **Parquet** (for cold storage Analytics).

## Core Technology Stack
- **API Framework**: `FastAPI` (Python 3) + `Uvicorn`
- **Concurrency**: `asyncio` + `ThreadPoolExecutor` (to bypass the GIL during I/O)
- **Serialization**: `orjson` (Zero-copy Rust-based JSON serialization)
- **In-Memory Cache & Discovery**: `PyArrow` (Parquet columnar reads)
- **Time-Series Database**: `TimescaleDB` (PostgreSQL extension for live metrics)
- **Message Broker**: `Apache Kafka` (High-throughput downstream streaming)
- **High-Speed Cache**: `Redis` (Fast access memory buffer)
- **Reverse Proxy**: `NGINX`

---
### [Click Here for the Quick Start Guide (5 Minutes to 163k pts/sec)](./QUICKSTART.md)
---

## Key Performance Wins
- **System Throughput**: Engineered for **163,000+ points/sec** (verified with multi-platform stress tests).
- **Vectorized Hydration**: Utilizes **PyArrow** and asynchronous Kafka publishing to bypass the Python GIL, enabling high-concurrency data processing.
- **Dynamic Hardware Hydration**: Decouples fast-moving telemetry metrics from static hardware profiles, merging them in-memory only when exporting to Kafka or Delta Lake.
- **Unified Schema**: Guarantees compliance with the **48-Field Golden Record** (PascalCase) across all paths (API, Cache, and Archive).

---

## The Ingestion Pipeline Workflow

The V3 architecture breaks the ingestion lifecycle into four distinct phases to maximize disk I/O and CPU efficiency:

### 1. Device Fleet Generation (Registry)
Instead of forcing the ingestion API to parse bulky hardware configurations on every request, the system uses a **Hardware Registry** (`device_configs.json`).
- You can generate a massive simulated fleet using `generate_registry.py`.
- The registry assigns diverse, real-world hardware profiles (Intel Xeon, AMD EPYC, DDR4/DDR5) and geographic locations across India to thousands of devices.
- When the API boots up, it caches this entire JSON registry into high-speed RAM.

### 2. Time-Series Telemetry Ingestion (Hot Path)
Raw, rapidly-changing telemetry data (e.g., `cpu_watts`, `amb_temp`, `cpu_util`) flows into **TimescaleDB** (PostgreSQL).
- **5-Minute API Job Requirement**: The system expects an external polling job or script to push telemetry to the API exactly every 5 minutes (`INTERVAL_SEC = 300`). 
- **TimescaleDB** is strictly used as the "Hot Storage" for the last 7 days of data.
- It does **not** store hardware profiles—only raw metrics. This keeps the database lean and prevents Out-Of-Memory (OOM) crashes.
- You can rapidly backfill this TSDB with realistic 5-minute interval sine-wave patterns and anomalies using `prefill_tsdb.py`.

### 3. Dynamic Hydration & Kafka Streaming
When the downstream system requires data (either via the API or the background Poller), the system:
1. Queries the raw metrics from TimescaleDB.
2. Instantly **hydrates** those metrics by merging them with the cached hardware profiles in RAM.
3. Streams the fully-packed JSON objects into the **Kafka** topic (`raw-server-metrics`).
- This design allows the machine learning models (like Isolation Forest) further down the pipeline to receive full hardware context without slowing down the database.

### 4. Daily Archival & Consolidation (Cold Path)
To prevent TimescaleDB from bloating over time, a daily archival job (`bench_daily_job.py`) runs in the background.
- It pulls 7 days of historical data, hydrates it with the hardware profiles, and compresses it into **Snappy Parquet** files.
- **Snappy Compression**: Because Parquet uses columnar dictionary encoding, 5.2 Million rows of JSON telemetry shrinks from ~1 GB down to highly efficient **~48 MB** files.
- These files are written to both `/app/data/raw/` (for downstream Delta Lake/Spark processing) and `/app/data/archive/` (for permanent cold storage).

---

## The API Execution Flow & Endpoints
All traffic is proxied through **Nginx on Port 80** and handled by the high-concurrency FastAPI application.

### How the API Works Under the Hood
When a client triggers an export request, the API does not just perform a simple database query. It orchestrates a high-speed data merger:
1. **Concurrency Management**: The request is routed to a tuned `ThreadPoolExecutor` wrapped via `asyncio`. This prevents Python's GIL from locking up the application during heavy processing.
2. **Dual-Discovery (The Merger)**: 
   - The API uses `PyArrow` to instantly fetch the bulk of the 7-day historical data from the local `telemetry-cache/` Parquet volume (O(1) lookup).
   - Simultaneously, it queries **TimescaleDB** using optimized composite B-Tree indexes to grab the most recent, live telemetry points that haven't been cached yet.
3. **In-Memory Hydration**: The two datasets are merged in-memory and heavily hydrated with the static hardware profiles loaded from the Registry.
4. **Zero-Copy Serialization**: The finalized data is serialized into the 48-field Golden JSON Schema using `orjson.Fragment` (zero-copy stitching) and immediately streamed out to the Kafka `raw-server-metrics` topic.
5. **Memory Reclamation**: Upon completion, the API aggressively calls `pa.default_memory_pool().release_unused()` to ensure the C++ allocators immediately return RAM back to the host operating system.

### Export Endpoints:
- **Latest Sync (Catchup)**: `POST /pcid/{pcid}/acid/{acid}/telemetry/latest/export`
- **Single Device Export**: `POST /pcid/{pcid}/acid/{acid}/id/{device_string}/export`

### Management Endpoints:
- **Register New Device**: `POST /register/device`
- **Force Cache Refresh**: `POST /telemetry/manual-cache-refresh`
- **Force Daily Archive**: `POST /telemetry/manual-archive`
- **Health & Monitoring**: `GET http://localhost:8001/health`

## Observability & Monitoring
- **Real-Time Benchmarking**:
  - Run E2E Benchmark: `docker exec atlas-ingestion python3 /app/v2/scripts/benchmark_e2e_multi.py --platforms 10`
  - Inspect Kafka Output: `docker exec atlas-ingestion python3 /app/v2/scripts/check_kafka_msg.py`

## Performance Warming & Backfilling
To achieve peak discovery performance immediately after deployment, use the **Cache Backfill** tools:

### 1. Prefill TimescaleDB (Simulate 7-Days of History)
```bash
docker exec -it atlas-ingestion python3 /app/v2/scripts/prefill_tsdb.py --days=7
```

### 2. Prefill Historical Cache (For API Speed)
```bash
docker exec -it atlas-ingestion python3 /app/v2/scripts/backfill_cache.py --days=7
```

### 3. Daily Archival Job (Raw & Archive Dumps)
```bash
docker exec -it atlas-ingestion python3 /app/v2/scripts/bench_daily_job.py
```

## Project Structure
- `v2/api/`: Core FastAPI logic with hierarchical stream workers and vectorized hydration.
- `v2/scripts/`: Performance benchmarking, archival jobs, and fleet generation tools.
- `schema/`: Unified Golden Record schema definitions (PascalCase).
- `data/`: Local storage for the Raw and Archive Parquet silos.
- `telemetry-cache/`: Optimized hourly Parquet partitions for API acceleration.

---
**Baseline Throughput**: Engineered to handle **163,000+ points/sec** on localized infrastructure.

---

### 📖 Detailed Architecture & Technical Decisions
For a deep dive into the engineering journey, performance bottlenecks, database architectures (TimescaleDB, Parquet Lakehouse), and exactly how we scaled this system over 8 distinct phases, please read the full **[Implementation Summary](../../ingestion/IMPLEMENTATION_SUMMARY.md)**.
