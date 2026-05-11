# PowerPulse V3: High-Performance Architecture Blueprint 🚀

This document outlines the state-of-the-art ingestion and discovery architecture that enables PowerPulse to handle **80,000 devices** at **163,000 points/sec**.

---

## 🏗️ The 10,000-Foot View

```mermaid
graph LR
    %% Layout Configuration
    Poller([🟢 Poller/Generator: 5min Poll])
    Device([📡 Fleet: 80,000 Devices])
    
    subgraph Ingestion [🚀 1. INGESTION]
        API_Ingest["<img src='https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png' width='40' height='20' /><br/>FastAPI"]
        Schema_B[Unified Builder]
        Kafka_P["<img src='https://kafka.apache.org/images/logo.png' width='40' height='20' /><br/>Kafka"]
    end

    subgraph Fast_Storage [⚡ 2. ACCELERATION]
        Redis["<img src='https://redis.io/images/redis-white.png' width='30' height='30' /><br/>Redis Metadata"]
        TSDB["<img src='https://www.timescale.com/static/timescale-logo-79dd9296e622415d862e31e33095034c.svg' width='40' height='20' /><br/>TimescaleDB"]
        P_Cache["<img src='https://parquet.apache.org/images/Apache_Parquet_logo.png' width='40' height='20' /><br/>Telemetry-Cache"]
    end

    subgraph Lakehouse [📊 3. LAKEHOUSE]
        Local_Raw[📁 raw/]
        Local_Archive[📚 archive/]
    end

    subgraph Discovery [💎 4. DISCOVERY]
        API_v2["<img src='https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png' width='40' height='20' /><br/>API v2"]
        M_Cache[🔍 Metadata Cache]
        V_Merge["<img src='https://arrow.apache.org/img/arrow.png' width='40' height='20' /><br/>Vectorized Merge"]
    end

    %% Ingestion Flows
    Poller -->|Polls every 5min| TSDB
    Device --> API_Ingest
    API_Ingest --> Schema_B
    Schema_B --> Redis
    Schema_B --> TSDB
    Schema_B --> Kafka_P
    
    %% Storage & Consolidation
    TSDB -.->|Hourly Push| P_Cache
    TSDB -.->|Update Index| Redis
    TSDB -.->|Daily Push| Local_Raw
    TSDB -.->|Mirror| Local_Archive
    
    %% Discovery Flows
    User((👤 User)) -->|Requests Export| API_v2
    API_v2 -->|Process Started ACK| User
    API_v2 -->|Requested Devices| Redis
    API_v2 -->|Enrich| M_Cache
    Redis -->|0(1) search for ACID in cache| P_Cache
    P_Cache -->|Historical Data| V_Merge
    TSDB -->|Query Delta Recent Data| V_Merge
    M_Cache --> V_Merge
    V_Merge -->|stream Batch| Kafka_Sink["<img src='https://kafka.apache.org/images/logo.png' width='40' height='20' /><br/>Kafka"]
    Kafka_Sink --> Downstream((🏁 Downstream))

    %% Styling
    style Ingestion fill:#f0f7ff,stroke:#007bff
    style Fast_Storage fill:#fff9e6,stroke:#ffc107
    style Lakehouse fill:#f4fdf4,stroke:#28a745
    style Discovery fill:#fff5f5,stroke:#dc3545
    style TSDB fill:#7000FF,color:#fff
    style Redis fill:#FF4B4B,color:#fff
    style P_Cache fill:#00FF94,color:#000
```

---

## 🛰️ Detailed Component Workflows

### 🚀 1. The Ingestion Engine (The Hot Path)
The ingestion engine is designed to handle **IPMI/HTTPS bursts** from 80,000 devices with zero packet loss.
1.  **Packet Arrival**: Data enters via the **FastAPI** `post_telemetry` endpoint.
2.  **Poller Trigger**: The **Poller/Generator** polls devices every 5 minutes and pushes directly to TimescaleDB.
3.  **Immediate Validation**: The engine checks the payload against the `input_schema.py`.
4.  **Parallel Multi-Sink**: 
    *   **Redis**: Updates the "Latest" heartbeat and status flags (sub-ms).
    *   **TimescaleDB**: Batch-inserts the raw telemetry into disk-backed hypertables.
    *   **Kafka**: Forwards the enriched record to the `raw-server-metrics` topic.

### ⚡ 2. The Acceleration Strategy (Background Work)
To achieve sub-20s response times, the system pre-computes the heavy lifting.
1.  **Hourly Push**: A background worker queries **TimescaleDB** for the last hour of telemetry and pushes to **Telemetry-Cache**.
2.  **Vectorized Compaction**: It uses **PyArrow** to compress this data into partitioned Parquet files.
3.  **Daily Push/Mirror**: Every midnight, the job extracts the full 24h data and mirrors it into both **`raw/`** and **`archive/`**.

### 💎 3. Accelerated Discovery (The Retrieval)
When a user requests a 7-day export, the API follows the **"Fast-Path"**:
1.  **Cache Index Check**: The API asks **Redis** for the ACID index in the cache ($O(1)$ search).
2.  **Vectorized Merge Logic**: 
    *   The engine retrieves **Historical Data** from the **Telemetry-Cache**.
    *   The engine calculates the **Delta Window**: 
        > `Delta = 168hrs (7 Days) - (Number of hours present in Telemetry-Cache)`
    *   It queries the **Delta Recent Data** from **TimescaleDB**.
3.  **Metadata Enrichment**: The **Metadata Cache** provides the inventory specs for final enrichment.
4.  **Final Push**: The **Vectorized Merge** joins these streams and pushes the result to **Kafka** via **stream Batch**.

---

## 📈 System Performance Thresholds
| metric | threshold | status |
| :--- | :--- | :--- |
| **Ingestion Throughput** | ~163,000 pts/sec | ✅ Verified |
| **API Response (1k Devices)** | < 20 Seconds | ✅ Optimized |
| **Memory Ceiling** | 6.1 GB Stable | ✅ Verified |
| **Concurrency Limit** | 10 Parallel Exports | ✅ Throttled |

---
> **Blueprint Version**: 3.3  
> **Last Updated**: May 9, 2026 ✅
