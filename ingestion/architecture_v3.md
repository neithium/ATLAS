# PowerPulse V3: High-Performance Architecture Blueprint 🚀

This document outlines the state-of-the-art ingestion and discovery architecture that enables PowerPulse to manage a global fleet of **80,000 devices** through highly concurrent, multi-tenant API exports, achieving peak throughputs of **147,000 points/sec**.

---

## 🏗️ Architecture Diagram

![V3 Architecture Diagram](./assets/Ingestion%20(1).png)

---

## 🛰️ Detailed Component Workflows

### 🚀 1. The Fleet Registry (Metadata Layer)
To prevent the ingestion API from parsing bulky hardware configurations on every request, the system uses a **Hardware Registry** (`device_configs.json`).
1. The registry assigns realistic Intel/AMD configurations and geographic data to up to 80,000 devices.
2. At boot time, the API loads this entire JSON registry into high-speed Python RAM to decouple static data from live telemetry.

### ⚡ 2. Multi-Tenant API Ingestion (The Hot Path)
The API does not export all 80,000 devices in one single payload. Instead, it utilizes an **API-Based Multi-Tenant Model**:
1.  **Tenant Fetching**: Downstream systems query the API on a per-customer basis (e.g., fetching exactly 1,000 devices under a specific `application_customer_id`).
2.  **Blazing Speed**: A standard fetch for 1,000 devices (with 7 days of historical data) completes in **< 20 seconds**.
3.  **Parallel Concurrency**: The API utilizes a tuned 24-worker `ThreadPoolExecutor`, safely supporting **10+ heavy concurrent API requests** at the exact same time without locking up or dropping throughput.

### 💎 3. Dynamic Hydration & Kafka Streaming
When the downstream ML pipeline or API requires real-time data:
1.  **Metric Fetch**: The engine queries the raw metrics from TimescaleDB.
2.  **In-Memory Hydration**: Utilizing a `ThreadPoolExecutor` (to prevent deadlocks) and `PyArrow`, the engine instantly merges the TSDB metrics with the cached hardware profiles in RAM.
3.  **Kafka Publish**: The fully structured Golden Schema JSON payloads are asynchronously published to the `raw-server-metrics` Kafka topic.

### 📦 4. Lakehouse Consolidation (The Cold Path)
To maintain performance and prevent TSDB bloat:
1.  **Daily Cron Job**: The `bench_daily_job.py` fetches the last 7 days of raw telemetry in chunks.
2.  **Compression**: It merges the metrics with the hardware registry and converts them into **Snappy-compressed Parquet** files.
3.  **Dual-Silo Write**: The 48MB files are saved simultaneously to `/app/data/raw/` (for Spark/Delta Lake processing) and `/app/data/archive/` (for permanent cold storage).

---

## 📈 System Performance Thresholds
| metric | threshold | status |
| :--- | :--- | :--- |
| **Ingestion Throughput** | ~147,000 pts/sec | ✅ Verified |
| **API Response (1k Devices)** | < 20 Seconds | ✅ Optimized |
| **Memory Ceiling** | 5.8 GB Stable | ✅ Verified |
| **Concurrency Limit** | 10 Parallel Exports | ✅ Verified |

---
> **Blueprint Version**: 4.0  
> **Last Updated**: 2026-06-07 ✅
