# Requirements for Ingestion Microservice

- The ingestion system manages telemetry data using a hierarchy of Platform Customer ID → Application Customer ID → Servers/Devices.
- Each server/device generates telemetry datapoints every 5 minutes.
- The system handles up to 7 days of historical datapoints for each server/device.
- Supports both Stream Processing and Batch Processing workflows.
- Ingestion is API-driven and occurs only when the client requests device data.
- For every API call, the system retrieves and streams 7 days of telemetry data for the requested devices.
- Separate Raw and Archive directories are maintained for batch processing and cold storage.
- Raw data is pushed periodically, either once or twice per day.
- The ingestion pipeline is designed to efficiently handle concurrent and parallel API requests at large scale.

---

# Architecture Preface: Initial Discovery

## Overview
- Redis stores only the latest 1 hour of telemetry data as a temporary buffer.
- MinIO pulls datapoints from Redis every 5 minutes and stores up to 6 days and 23 hours of historical data.
- On every API request:
  - 6 days 23 hours of data is fetched from MinIO.
  - Latest 1 hour of data is fetched from Redis.
  - Both datasets are merged to create a complete 7-day telemetry dataset and streamed to downstream systems.

## Problem Faced
- **High RAM Utilization**: Relying heavily on Redis (a purely in-memory data store) as a temporary buffer for up to an hour of telemetry across tens of thousands of devices caused massive, unsustainable spikes in RAM consumption.
- Real-time ingestion needed to be lightning fast, but downstream systems like Apache Spark required data in bulk (batch processing).
- The REST API struggled to serve historical requests because it had to query multiple disparate storage locations (MinIO and Redis) and computationally stitch the JSON together on the fly.
- Merging gigabytes of historical data on every single API call caused massive CPU bottlenecks, leading to slow response times and timeouts under high concurrency.

---

# Phase 1: Architectural Evolution (March 12 - March 19)

## Overview
During this period, the architecture utilized Redis as a high-speed transient buffer for the 55,000-device ingestion path.

### 1. Ingestion Engine
- Implemented a high-concurrency poller with AsyncIO thread pools and staggered startups to handle massive IPMI bursts.
- Heavy IPMI reading operations were offloaded to a thread pool (run_in_executor) to maintain event loop responsiveness.

### 2. Redis Ring-Buffer Optimization
- Telemetry was stored in a strict 24-hour circular buffer per device (using LTRIM).
- Scaled the Redis connection pool to 5,000 concurrent workers to handle bursty traffic from thousands of parallel threads.
- Implemented Redis pipelines (pipe.rpush, pipe.ltrim, pipe.expire) to ensure data consistency and minimize network round-trips.

### 3. Data Archiving & Storage
- MinIO Integration: An hourly archival process moved the oldest 12 readings (1 hour of data) from Redis to MinIO.
- Maintaining a 7-day rolling window across both storage layers.

---

## Rationale for the Architectural Shift
The transition from the pure Redis/MinIO approach to a structured Medallion architecture was driven by three primary production bottlenecks:

1. **High RAM Utilization**: Storing 24 hours of telemetry for 55,000+ devices in Redis caused unsustainable memory spikes and hit a vertical scaling wall, requiring a shift toward disk-backed strategies to reduce memory overhead.
2. **Small File Problem**: The hourly Redis-to-MinIO archival created thousands of tiny Parquet files. This "Small File Problem" severely degraded Spark performance and increased S3 metadata overhead, demanding a larger, more efficient data batching process.
3. **Data Durability & Persistence**: Utilizing an in-memory buffer as a primary store introduced risks during service restarts or cache evictions, exposing the need for true ACID compliance and long-term persistence.

## Learnings
- **In-Memory Limits**: Scaling a pure in-memory datastore horizontally for primary telemetry storage is cost-prohibitive and dangerous. Redis is excellent for transient queuing, but unfit for multi-day raw data durability at the scale of 50,000+ devices.

---

# Phase 2: Transition to Medallion Storage (March 19 - March 26)

## Overview
This phase focused on evolving the storage architecture from a simple buffer to a structured Medallion-style data lake in MinIO to handle long-term scalability and downstream analytics.

### 1. Medallion Bucket Architecture
- Created dedicated tiered storage buckets in MinIO to separate processing workflows:
  - **`telemetry-raw` (The Staging/Processing Tier)**: Acts as the landing zone where API-driven batch data and micro-batches are pushed. Here, Apache Airflow triggers downstream Apache Spark jobs to take over the data for batch processing and run daily summaries.
  - **`telemetry-archive` (The Cold Storage Tier)**: Reserved for long-term retention and compliance. Once raw data is processed, it is compacted into dense, highly-compressed historical blocks and moved here. This tier is optimized strictly for infrequent, massive historical lookups.
- Implemented a unified partitioning strategy (`YYYY/MM/DD/HH`) for all cold-path storage to optimize Spark discovery and query performance.

### 2. Storage Format Standardization
- Standardized on Snappy-compressed Parquet files for all archival tasks.
- This shift addressed the "Small File Problem" by allowing for larger batch sizes (e.g., 2016 points per device) before flushing to MinIO.

---

# Phase 3: Production Hardening with TimescaleDB (March 26 - April 10)

## Overview
To handle the massive influx of telemetry data, we transitioned from in-memory Redis buffers to **TimescaleDB**, an advanced time-series database built on PostgreSQL.

### Key Database Concepts Introduced:
- **Hypertables**: We configured our main storage as a TimescaleDB Hypertable. Hypertables automatically partition massive amounts of data into smaller, hidden "chunks" based on time intervals. This allows the database to instantly filter out irrelevant time ranges (e.g., efficiently isolating the "last 7 days" of data) and heavily optimizes continuous high-volume inserts.
- **Hierarchical Indexes**: Implemented three highly optimized composite B-Tree indexes: `(device_id, metric_time DESC)`, `(application_customer_id, metric_time DESC)`, and `(platform_customer_id, application_customer_id, metric_time DESC)`. These act like a book's table of contents; instead of forcing the database to check every single row sequentially (a slow "sequential scan"), these composite indexes allow the engine to instantly jump to the exact location of the requested telemetry data regardless of which level of the customer hierarchy the API queries.

### Pipeline Workflow:
- The synthetic generator creates telemetry datapoints for each registered server every 5 minutes and pushes them directly into the highly-compressed Hypertable.
- When a client calls the API, a background job uses the indexes to rapidly fetch 7 days of history for the requested devices.
- The fetched data is serialized into JSON format and streamed to Kafka for downstream processing.

## Problem Faced
- **Slow Exports (TimescaleDB Overload)**: While TimescaleDB perfectly solved the Redis RAM bottleneck, fetching 7 days of historical data for thousands of devices concurrently caused massive index-scan contention. This shifted the bottleneck from RAM to CPU, resulting in severe latency and timeouts during API exports.

## Solution Implemented
- **Direct TimescaleDB Querying**: Re-engineered all telemetry export endpoints to query TimescaleDB hypertables directly.
- **Optimized SQL Projections**: Switched to high-performance SQL features (e.g., `json_agg`) to fetch and format data into the 48-field Golden Record schema in a single pass.
- **Kafka Schema Integrity**: Ensured every message pushed to the `raw-server-metrics` Kafka topic is a complete, serialized JSON record (including historical context and `inventory_data`).
- **Storage Efficiency**: Enabled native TimescaleDB compression policies to optimize disk usage.

## Learnings
- **The Value and Limits of Indexing**: Indexes are incredibly useful for accelerating targeted queries—they allow TimescaleDB to instantly locate specific device events or short time-windows without scanning the entire database (avoiding slow sequential scans). However, there is a strict trade-off: querying massive 7-day historical datasets for thousands of devices simultaneously puts an enormous strain on those same indexes. High concurrency inevitably triggers index-scan contention, shifting the system bottleneck from RAM directly to CPU. A purely database-driven architecture is excellent for targeted lookups but fundamentally unsuited for massive concurrent bulk fetches without a cache layer.

---

# Phase 4: Telemetry Cache Layer & Local FS Lakehouse (April 11 - April 21)

## Historical Problem: TimescaleDB Overload
During early testing, the system relied entirely on real-time TimescaleDB fetches for all historical requests (7-day windows). 
- **The Bottleneck**: High concurrency during device exports triggered massive index-scan contention in PostgreSQL.
- **The Result**: CPU spikes on the DB host and severe latency degradation (80s+ for 1k devices).
- **Baseline Performance**: A pure DB-driven approach peaked at ~120,594 pts/sec but was highly unstable under memory pressure.

## The Solution: Vectorized Parquet Side-Loading
To resolve the database contention, we transitioned the API's discovery layer from compute-bound SQL queries to a high-performance **Local Telemetry Cache**.

### 1. The Telemetry Cache Layer
We built a background engine that pre-aggregates 7-day telemetry windows into localized Parquet files.
- **API Acceleration**: When a client requests 7-day data, the API fetches the bulk historical data from the `telemetry-cache/` volume and only queries TimescaleDB for the most recent, real-time metrics, computationally merging them.
- **O(1) Discovery**: Utilized `PyArrow` for vectorized hydration, reading massive historical datasets directly from local disk, bypassing Python's GIL for extreme concurrency.
- **Impact**: API response times dropped from 80s down to <20s for 1,000-device clusters.

### 2. Dual-Write Medallion Architecture
We addressed critical network latency (previously caused by MinIO object storage) by shifting to **Local NVMe Storage** with strict data silos:
- **`telemetry-cache/`**: The hot-path cache for sub-second 7-day historical API lookups.
- **`data/raw/`**: Dedicated local storage for Apache Spark Streaming ingestion.
- **`data/archive/`**: Reserved for Long-term Compliance (cold path), storing consolidated daily Parquet silos.

### Post-Cache Performance Results (Final V3)
With the Telemetry Cache layer active, the system achieved a massive performance breakthrough, shifting the bottleneck entirely from software to physical hardware limits.

| Metric | Baseline (Pre-Cache) | Optimized (V3 Cache) | Result |
| :--- | :--- | :--- | :--- |
| **1k Device Export** | 167s (Initial v2) | **19.4s** | **8.6x Faster** |
| **10k Device Export** | 167s (Single Plat) | **123.1s** | **163k pts/sec** |
| **Peak Throughput** | ~120k pts/sec | **~163,685 pts/sec** | **+36% Gain** |
| **Memory Stability** | Spike-Prone | **Capped @ 6.07GB** | **Production-Stable** |

---

# Phase 5: Aggressive Memory Reclamation (April 22 - April 30)

## Problem: C++ Allocator Retention
After heavy benchmark runs (23 consecutive 10k-device exports), the container's idle memory remained at ~4.8 GB instead of settling back to ~3.4 GB. 
- **Root Cause**: PyArrow's internal C++ allocator (jemalloc/mimalloc) retains freed memory pages as a performance optimization, preventing the OS from reclaiming them.
- **Solution**: Added `pa.default_memory_pool().release_unused()` to the post-export cleanup phase in `api_v2.py`, combined with the existing `malloc_trim(0)`.
- **Result**: Idle container memory successfully drops from ~4.8 GB down to ~3.4 GB immediately after export completion, allowing the host to reclaim resources.
- **Trade-off**: ~5ms of CPU time per cleanup cycle, which is completely negligible against a 3,000ms+ export operation.

---

# Phase 6: Pipeline Optimization Update (May 1 - May 15)

The PowerPulse V3 Ingestion Engine has been successfully finalized and verified at production scale. This update marks the completion of the performance hardening phase.

### Key Deliverables:
- **Throughput Breakthrough**: Reached a stable peak of **163,685 points/sec**, effectively tripling the initial v3 baseline and saturating 12-core host capacity.
- **Architecture Finalization**: Successfully pivoted to a **Local FS Lakehouse** model, eliminating MinIO overhead and achieving sub-second Parquet discovery.
- **Serialization Efficiency**: Implemented **Zero-Copy JSON Stitching** (`orjson.Fragment`), slashing GC pauses and memory pressure under high-concurrency 10,000-device loads.
- **Container Hardening**: Resolved all environment-specific bottlenecks including Docker volume sync locks and graphics dependency issues for the visualization layer.

---

# Phase 7: Kafka Resiliency & Refactoring (May 16 - May 30)

## Overview
This phase focused on making the ingestion API fully independent of Kafka's lifecycle and introducing aggressive memory reclamation to reduce idle container footprint. Additionally, the Kafka cluster switching workflow was unified via `single.bat` / `cluster.bat` with automatic environment injection.

### 1. Kafka-Independent API Startup (`api_v2.py`, `main.py`)
- **Before**: If Kafka was unreachable at boot, `AIOKafkaProducer.start()` threw a fatal `KafkaConnectionError` and crashed the entire FastAPI process. This made the API unusable during Kafka maintenance windows.
- **After**: `get_kafka()` now uses a lazy-connect pattern with a `KAFKA_STARTED` flag. On failure, it logs a warning and lets the API boot normally. The producer automatically retries connection on the next export request.
- Removed the unsafe `await producer.start()` from `main.py`'s startup handler that was independently crashing the app.


### 3. Dynamic Kafka Bootstrap (`docker-compose.yml`)
- Changed `KAFKA_BOOTSTRAP` from a hardcoded value to `${KAFKA_BOOTSTRAP:-broker1:9092}`.
- Defaults to single-broker mode for development. `cluster.bat` overrides to `broker1:9092,broker2:9092,broker3:9092`.
- Eliminates manual `docker-compose.yml` edits when switching between dev and cluster modes.

### 4. Safe Cluster Switching Scripts (`single.bat`, `cluster.bat`)
Both scripts now perform three operations:
1. Stop and remove existing Kafka containers.
2. Set `KAFKA_BOOTSTRAP` explicitly for their respective mode.
3. Run `docker-compose up -d --force-recreate atlas-ingestion` to inject the updated env var.

| Script | Brokers Started | `KAFKA_BOOTSTRAP` Value | Fault Tolerance |
| :--- | :--- | :--- | :--- |
| `single.bat` | `broker1` only | `broker1:9092` | None (dev mode) |
| `cluster.bat` | `broker1`, `broker2`, `broker3` | `broker1:9092,broker2:9092,broker3:9092` | 1 broker can fail |

**Data Safety**: TSDB, Redis, and MinIO volumes are never touched by either script. `--force-recreate` only rebuilds the container process, not the underlying data volumes.

### 5. Kafka Fault Tolerance (Cluster Mode)
With the 3-broker cluster properly configured:
- **2 of 3 brokers alive**: Writes succeed (meets `min.insync.replicas=2`).
- **1 of 3 brokers alive**: Writes rejected (cannot satisfy ISR requirement).
- The `AIOKafkaProducer` handles failover transparently — no code changes or restarts required.

### 6. Documentation (`ingestion/QUICKSTART.md`)
- Added section `1a. Managing Kafka Modes` explaining `single.bat` vs `cluster.bat` usage.
- Included a critical warning against using `docker compose down -v` to prevent accidental data loss.

### Files Changed

| File | Change |
| :--- | :--- |
| `v2/api/api_v2.py` | Lazy Kafka connect + PyArrow memory pool release |
| `main.py` | Removed unsafe `producer.start()` from startup |
| `docker-compose.yml` | `KAFKA_BOOTSTRAP` now env-var driven with default |
| `single.bat` | Added `KAFKA_BOOTSTRAP` + force-recreate ingestion |
| `cluster.bat` | Added `KAFKA_BOOTSTRAP` + force-recreate ingestion |
| `QUICKSTART.md` | Added Kafka mode switching docs |

---

# Phase 8: V3 Finalization & Concurrency Hardening (June 1 - June 7)

## Overview
This phase marked the absolute finalization of the PowerPulse V3 architecture, shifting from deadlock-prone multi-processing to high-stability thread pools, implementing realistic telemetry modeling for downstream machine learning, and completely overhauling the documentation suite.

### 1. ThreadPool & Async IO Deadlock Resolution
- **Problem**: Heavy concurrent requests under `ProcessPoolExecutor` paired with Uvicorn frequently resulted in ghost-hangs and deadlocks due to memory mapping issues.
- **Solution**: Completely stripped out `ProcessPoolExecutor` in `api_v2.py`. Replaced it with a tuned `ThreadPoolExecutor` wrapped via `asyncio`.
- **Result**: Validated extreme stability under load, achieving **147,000+ points/sec** for 10 concurrent heavy-hitter exports.

### 2. Realistic Telemetry Modeling for ML
- **Sinusoidal Curve Generation**: Upgraded `prefill_tsdb.py` to generate realistic 5-minute interval power metrics using sinusoidal waves with randomized anomalous spikes and drops. This was necessary to properly train the downstream `IsolationForest` models.
- **Hardware Diversity**: Upgraded `generate_registry.py` to inject diverse architectures (Intel Xeon, AMD EPYC, DDR4/DDR5) and geographic locations across India for better anomaly context.
- **Role-Aware Workload Profiling (`tags`)**: Expanded the central registry database schema (`init_db.py`) to include explicit server roles (using the `tags` field to define UI, Database, AI, Spark, etc.). This categorization is critical for the ML model, allowing it to establish distinct baselines per workload type (e.g., ignoring naturally high network traffic on Backup servers) rather than applying a blanket threshold.

### 3. Strict Local FS Lakehouse & Silo Tuning
- **Silo Sizing**: Increased `SILO_SIZE` to 7,000 records in `api_v2.py` and `bench_daily_job.py`. 7,000 devices exactly hit the sweet spot of ~128MB per Parquet file when snappy-compressed.
- **Dual-Write Architecture**: Cemented the cold path to strictly write local files to `/app/data/raw/` (for Spark streaming) and `/app/data/archive/` (for compliance), entirely dropping the MinIO abstraction overhead from this layer.

### 4. TimescaleDB Networking & Observability Patch
- **`fix_pg.sh`**: Created a networking patch to automatically update `postgresql.conf` (`listen_addresses = '*'`) and `pg_hba.conf` (`0.0.0.0/0 md5`). This opened the TSDB ports to external tools like Grafana and DBeaver.
- **Tool Pruning**: Completely removed legacy references and configurations for Jaeger UI and Kafka UI, keeping the environment strictly focused on Grafana.

### 5. Documentation Synchronization
- **Consistency**: All primary docs (`README.md`, `BENCHMARKING_GUIDE.md`, `architecture_v3.md`, `QUICKSTART.md`) were audited and perfectly synchronized to the actual measured benchmarks (147k pts/sec).
- **Projections**: Updated math projections for large fleets (a 100,000-device daily Parquet archive now calculates to complete in just ~1.3 hours).

---

## Benchmark Results & Scripts

All benchmark scripts, raw output logs, and historical throughput data are maintained in a dedicated directory for reproducibility and regression testing.

 **Location**: [v2/scripts/benchmarks/](./v2/scripts/benchmarks/)

| File | Description |
| :--- | :--- |
| `benchmark_e2e_multi.py` | End-to-end multi-platform benchmark script |
| `bench_daily_job.py` | Daily archival consolidation benchmark |
| `benchmark_latest.txt` | Latest benchmark run outputs |
| `benchmark_results.txt` | Historical benchmark results archive |
| `final_ingestion_benchmarks.txt` | Final production throughput numbers |

### 6. Quality Assurance & Automated Testing (`test_v2_ingestion.py`)
- **Execution & Validation**: The test suite was successfully executed with 100% success (`6 passed`), validating critical API and processing behavior.
- **Key Assertions Tested**:
  - **Golden Schema Validation**: Verified that `build_48_field_golden_record` flawlessly injects missing inventory data and preserves expected data types without corrupting the historical payloads.
  - **Parquet Silo Engine**: Validated the `flush_to_parquet` logic, specifically confirming that the engine attempts exactly 2 distinct directory creations (`raw` and `archive`) and completes exactly 4 file write operations (Parquet tables + `_SUCCESS` validation markers).
  - **PyArrow Integrity**: Confirmed that `ParquetWriter` correctly mounts, writes the table chunk, and properly triggers `.close()` to ensure no memory leak occurs during long-running background loops.
- **Reporting Integration**: Full PyTest execution logs are physically preserved in `ingestion/test_results.txt`, and verified statuses have been recorded directly in `ingestion/v2/tests/test_results.md` for fast developer reference.
