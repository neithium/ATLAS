# 🚀 ATLAS Ingestion: Benchmarking & Operations Guide

This guide contains the essential commands and endpoints for managing, benchmarking, and archiving telephony data in the ATLAS pipeline.

## 🔗 Monitoring Dashboards
| Service | Local URL | Purpose |
| :--- | :--- | :--- |
| **Ingestion API**| [http://localhost:8001/docs](http://localhost:8001/docs) | Interactive Swagger documentation. |
| **Grafana**| [http://localhost:3000](http://localhost:3000) | Live Telemetry Dashboard. |

---

## 🌐 Network & Port Bindings
If you need to connect external tools directly to the services, here are the exposed local ports:
- **`80`** - Production Gateway (Nginx/API)
- **`8001`** - Ingestion Discovery & Stats Interface

---

## 🛠️ API Endpoints

### 1. Trigger Telemetry Export (7-Day Latest)
Kicks off the background worker to hydrate 48-field records and push to Kafka.
- **Method:** `POST`
- **URL:** `http://localhost:8001/pcid/{platform_id}/acid/{app_id}/telemetry/latest/export`
- **Example:** `http://localhost:8001/pcid/PLATCUST0001/acid/APPCUST0001/telemetry/latest/export`

### 2. Register New Device
Dynamically adds a new device to the fleet registry without restarting services.
- **Method:** `POST`
- **URL:** `http://localhost:8001/register/device`
- **Payload Example:**
  ```json
  {
    "device_id": "NEW-DEVICE-001",
    "platform_customer_id": "PLATCUST10K",
    "application_customer_id": "APPCUST10K",
    "server_name": "host-001",
    "location_city": "Mumbai",
    "location_country": "India"
  }
  ```

### 3. Service Health Check
- **Method:** `GET`
- **URL:** `http://localhost:8001/health`

---

## 🧪 Benchmarking Commands

All commands should be run from the root directory `d:\PowerPulse\atlas`.

### 1. Multi-Platform E2E Flow (Total Throughput)
Measures the total time from API trigger to the last message hitting Kafka for multiple platforms.
```powershell
# Targets 50 platforms (550 devices)
python d:\PowerPulse\atlas\ingestion\v2\scripts\benchmark_e2e_multi.py --platforms 50

# Targets the 10k Heavy Hitter
python d:\PowerPulse\atlas\ingestion\v2\scripts\benchmark_e2e_multi.py --heavy PLATCUST10K
```

### 2. Platform-Level Percentiles (Latency Distribution)
Measures the specific P50, P90, and P99 latency distribution across independent platform flows.
```powershell
# Captures latency distribution for 100 platforms
python d:\PowerPulse\atlas\ingestion\v2\scripts\benchmark_e2e_percentiles.py --platforms 100
```

### 3. API Streaming Trigger (Concurrent Stress Test)
Stress tests the API triggering mechanism for massive multi-tenant bursts.
```powershell
# Triggers 500 platforms concurrently
python d:\PowerPulse\atlas\ingestion\v2\scripts\test_concurrent_exports.py --platforms 500
```

### 4. Heavy Hitter Targets
| Fleet Size | PCID Target | Total Data Points |
| :--- | :--- | :--- |
| **10,000 Devices** | `PLATCUST10K` | ~20.1 Million |
| **4,000 Devices** | `PLATCUST9999` | ~8.0 Million |

---

## 📦 Archival Operations

### 1. Stable Sequential Archival (Production Mode)
Pushes the 7-day telemetry fleet to Local FS Parquet silos one batch at a time. Designed for maximum reliability and low memory footprint.
```powershell
docker exec -it atlas-ingestion python3 /app/v2/scripts/bench_daily_job.py
```

---

## 🔍 Verification Tools

### 1. Kafka Message Inspector
Scans the `raw-server-metrics` topic and prints the first available nested payload.
```powershell
docker exec atlas-ingestion python3 /app/v2/scripts/check_kafka_msg.py
```

### 2. Kafka Schema Validator
Samples 50 messages and validates every field against the 48-field Golden Schema.
```powershell
docker exec atlas-ingestion python3 /app/ingestion/validate_schema.py --sample-count=50
```

### 3. Clear Kafka Topic (Factory Reset)
Deletes and recreates the topic to reset offsets to zero.
```powershell
docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic raw-server-metrics
docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic raw-server-metrics --partitions 12 --replication-factor 1
```

---

## 🏆 Production Best Practices

### 1. Optimal Archival Scheduling
- **Recommended Time:** `23:59:00 UTC`
- **Reason:** Running at the very end of the day ensures the 7-day window captures the absolute latest telemetry from the current date before the date-partition rolls over.
- **Impact:** Captures a perfect `[Day-7 23:59] -> [Today 23:59]` window (168 hours of data).

### 2. Monitoring Performance
- Check the **Total Flow Time** in `benchmark_results.txt` weekly.
- If **Database + Kafka** time starts to exceed **120 seconds** for a 10k batch, consider migrating to the Parallel Archival model with higher RAM allocation (16GB+).

---

## 📈 Architecture & Scale Estimations

### Archival Strategy: Buffered Parquet Streaming (Active)
Due to standard Docker VM limitations (8GB RAM), the **Parallel Archival** approach caused out-of-memory (OOM) lockups when hydrating massive 140,000-record batches. 

To ensure 100% stability, the current script (`bench_daily_job.py`) uses **Buffered Parquet Streaming**:
1. Fetches data in tiny **100-device micro-chunks**.
2. Aggregates chunks using `pyarrow.parquet.ParquetWriter`.
3. Uploads precisely structured **Parquet Silos** (approx. 48MB each) to the local `/app/data/archive/`.
4. **Result:** Completely flat memory usage and perfectly sized files for Spark.

### Baseline Metrics (Live Verified Results)
Based on live local runs, generating a 7-day archive (2016 points per device) processes at a speed of **~1,228 devices per minute**.
- **1 Silo (7,000 Devices)** ≈ 14.1 Million Data Points
- **Size per 7,000 Device Silo** ≈ 130 MB (Snappy Compressed)
- **Time per 7,000 Device Silo** ≈ 5.7 minutes
- **Actual Run Result:** Processed 10,000 devices (20.1 Million data points) into 4 optimized Parquet files in exactly **7 minutes 41 seconds** with flat memory usage.

### Time Projections (Streaming Mode)
| Fleet Size | Total Data Points | Silos Generated | Estimated Completion Time | Recommended Usage |
| :--- | :--- | :--- | :--- | :--- |
| **14,000 Devices** | ~28 Million | 2 | ~11.5 Minutes | Day-time ad-hoc updates |
| **65,000 Devices** | ~131 Million | 10 | ~53 Minutes | Nightly CRON Job |
| **80,000 Devices** | ~161 Million | 12 | ~1 Hour 5 Mins | Target Production Load |
| **100,000 Devices** | ~201 Million | 15 | ~1.3 Hours | High-Scale CRON Job |

*Note: This is highly efficient. If triggered at midnight (23:59), a massive 100k-device lakehouse will be completely archived by 1:20 AM using less than 2GB of active memory.*

### High-Performance Alternative (Parallel Mode)
If the ingestion service is migrated to a dedicated bare-metal server or heavy EC2 instance (e.g., 32+ Cores, 32GB+ RAM):
- **Strategy:** Revert to `asyncio.Semaphore` based Parallel Archival.
- **Estimated Time (100k Devices):** **< 15 minutes**.
- **Tradeoff:** Massive I/O saturation and memory spikes (requires 16GB+ RAM dedicated solely to Python hydration).

---
> [!TIP]
> Always ensure the `atlas-ingestion` container is GREEN in Docker Desktop before running benchmarks.
---

## 🏆 Final Optimized Benchmarks (Record Breaking)

After applying AsyncIO ThreadPool processing and PyArrow vectorization, the system achieved its highest performance:

| Metric | Result |
| :--- | :--- |
| **10k Device E2E Flow** | **~137 seconds** |
| **Raw Throughput** | **~350 MB/s** |
| **Points per Second** | **~147,000 pts/sec** |
| **Error Rate** | **0%** (Zero batch delivery errors) |

### 🛠️ Production Tuning Applied
1. **Kafka Buffer:** Increased `buffer_memory` to `128MB` in `kafka_producer.py` to handle telemetry bursts without backpressure.
2. **ThreadPool Executor:** Migrated to ThreadPoolExecutor to prevent Uvicorn worker deadlocks, improving trigger stability.
3. **Archival Engine:** Refactored `bench_daily_job.py` into a buffered streaming Parquet writer, ensuring 100% stability for a large-scale fleet within an 8GB RAM budget.
---
*Last Updated: 2026-06-07 - Full Pipeline (DB -> Kafka -> Local FS) Verified.*
