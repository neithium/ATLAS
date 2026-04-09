# PowerPulse V3: High-Scale IoT Ingestion Architecture

This document defines the production architecture for the **80,000 Device Fleet** ingestion engine, achieving a **238-Million Row** historical baseline with **90%+ storage efficiency**.

## 📐 Unified System Visual Flow

```mermaid
graph TD
    subgraph "🌐 GATEWAY LAYER (Port 80)"
        NGX[NGINX Reverse Proxy]
        API_GW[API Gateway / Auth]
        UI_GW[MinIO Console Proxy]
        
        NGX --> API_GW
        NGX --> UI_GW
    end

    subgraph "📡 IoT FLEET LAYER (80,000 Devices)"
        D1[Device Group 001]
        DN[Device Group 80k]
    end

    subgraph "⚙️ INGESTION ENGINE (Poller V3 / main.py)"
        SCH[APScheduler: 5-min Intervals]
        REG[Registry: device_configs.json <br/> 57MB Metadata]
        WP[ThreadPoolExecutor: 100 Workers]
        
        subgraph "🛠️ THE HARVESTER (ipmi_reader.py)"
            DCMI[DCMI: Real-time Watts]
            SDR[SDR: Ambient Temp]
            FRU[FRU: CPU/Memory Inventory]
        end

        SCH --> WP
        REG --> WP
        WP -- "Fast Batch Dispatch" --> DCMI
        DCMI -- "28-Field Row" --> WP
        FRU -- "Inventory Struct" --> WP
        WP -- "High-Speed COPY Stream" --> TSDB
    end

    subgraph "🗄️ HOT STORAGE (TimescaleDB)"
        TSDB[(telemetry_live <br/> 238M+ Rows)]
        IDX[Hierarchical Indexes <br/> PCID/ACID/DID]
        COMP[Compression Policy: <br/> 1-Day Auto-Crunch]
        
        TSDB --- COMP
        COMP -- "93% Space Savings" --> TSDB
        TSDB -- "Daily Mega-Compaction" --> COMPACT[Midnight Compactor]
    end

    subgraph "❄️ COLD PATH (MinIO Archival)"
        RAW[Bucket: telemetry-raw <br/> Partitioned Parquet <br/> FOR SPARK]
        ARC[Bucket: telemetry-archive <br/> Recovery Vault <br/> IMMUTABLE]
        
        UI_GW --> RAW
        COMPACT --> RAW
        COMPACT --> ARC
    end

    subgraph "🌊 STREAMING API (V3.1)"
        API[FastAPI: V2 Hierarchical API]
        TASK[Background Parallel Streamer]
        SEM[Semaphore: 15 Concurrency]
        TRANS[Golden Schema Transformer]

        API_GW --> API
        API -- "Trigger PCID/ACID" --> REG
        REG -- "Instant Discovery" --> TASK
        TASK -- "Parallel DB Fetch" --> TSDB
        TASK --> SEM --> TRANS
    end

    subgraph "🛰️ DATA OUTGRESS (Kafka / Lakehouse)"
        KAFKA[[Redpanda: 5MB Payloads]]
        SPARK[Spark Ingestion / Lakehouse]
        
        TRANS -- "Nested Golden JSON (LZ4)" --> KAFKA
        KAFKA -- "input_schema.py" --> SPARK
    end

    style NGX fill:#fdd,stroke:#333,stroke-width:2px
    style TSDB fill:#f96,stroke:#333,stroke-width:4px
    style KAFKA fill:#6cf,stroke:#333,stroke-width:2px
    style WP fill:#cfc,stroke:#333,stroke-width:2px
    style REG fill:#fff,stroke:#333,stroke-dasharray: 5 5
    style RAW fill:#aaf,stroke:#333,stroke-width:2px
    style ARC fill:#aaf,stroke:#333,stroke-width:2px
```

## 🚀 Architectural Breakdown (By Folder Complexity)

### 1. The Gateway (`nginx-allinone.conf`)
Every call to the system is intercepted by **NGINX**. It acts as the traffic controller, routing requests to the **FastAPI Ingestion Interface** or the **MinIO Storage Console**.

### 2. High-Density Harvesting (`core/ipmi_reader.py`)
This is the ingestion frontline. It doesn't just "ping" servers; it performs deep hardware harvesting:
- **DCMI**: Power readings.
- **SDR**: Thermal/Chassis health.
- **FRU**: Hardware specifications (CPU cores, memory freq) stored in your **Inventory Data** struct.

### 3. Hot-Storage Optimization (`v2/init_db.py`)
At a baseline of **238,000,000 rows**, we use TimescaleDB's native **Columnar Compression**. This keeps the "Hot Path" lean (1.5GB total) while maintaining sub-second query performance for historical exports.

### 4. Metadata-Resident Discovery (`device_configs.json`)
By storing device metadata (PCID, ACID, Model) in a local JSON registry, we avoid the "O(n) Discovery Problem." The system knows which 1,600 devices belong to a customer **before** ever touching the multi-billion row table.

### 5. The "Golden" Kafka Transformer (`v2/api/api_v2.py`)
The final outgress layer performs an on-the-fly **ETL**. It translates flat database records into the **Nested Spark Schema** (aggregating Max/Min/Avg), ensuring 100% compatibility with your downstream Lakehouse jobs.

---
**Verified V3 High-Scale Baseline 🛰️**

## 🔄 Operational Flow (Quick Reference)

```mermaid
graph LR

%% =========================
%% 1. SOURCE LAYER
%% =========================
GEN[Generator / 5-min Polling]

PL1[PLAT 1]
PL2[PLAT 2]

APP_A[APP A]
APP_B[APP B]
APP_C[APP C]
APP_D[APP D]

DEV1[Devices Group 1]
DEV2[Devices Group 2]

GEN --> PL1
GEN --> PL2

PL1 --> APP_A
PL1 --> APP_B

PL2 --> APP_C
PL2 --> APP_D

APP_A --> DEV1
APP_B --> DEV1
APP_C --> DEV2
APP_D --> DEV2

%% =========================
%% 2. HOT STORAGE
%% =========================
TSDB[(TimescaleDB - Hot)]

DEV1 -- Fast Ingest --> TSDB
DEV2 -- Fast Ingest --> TSDB

%% =========================
%% 3. COLD STORAGE
%% =========================
SCHED[Daily Batch Scheduler]
MINIO[MinIO - Cold Parquet]

TSDB --> SCHED
SCHED -- 24h Compaction --> MINIO

%% =========================
%% 4. ARCHIVE LAYER
%% =========================
ARCH[Archive Storage - Backup]

MINIO -- Retention Policy --> ARCH

%% =========================
%% 5. API LAYER
%% =========================
API[API Service]
TASK[Background Streamer]
CLIENT[Client]

CLIENT --> API
API -- Async Trigger --> TASK

%% =========================
%% 6. KAFKA FLOW
%% =========================
KAFKA[Redpanda / Kafka]

TASK --> TSDB
TSDB -- Stream Transformation --> KAFKA

%% =========================
%% 7. ANALYTICS
%% =========================
SPARK[Spark / Lakehouse]

MINIO --> SPARK
KAFKA --> SPARK
```
