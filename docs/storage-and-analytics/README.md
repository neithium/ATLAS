# Storage & Analytics Engine

> **Module Owner:** Analytics Team · **Runtime:** Python 3.11 on Debian Bookworm · **Orchestrator:** Apache Airflow 2.7.1  
> **Last Updated:** July 2026

---

## Overview

The **Storage & Analytics Engine** is the terminal serving layer of the ATLAS pipeline. It is a stateless, Python-based processing engine that bridges refined Delta Lake Parquet files into ClickHouse for sub-millisecond analytical queries powering real-time dashboards and an AI-driven diagnostic copilot.

This module is deployed as a **single unified container** (`atlas-analytics`) running four co-located services via `supervisord`:

| Service | Process | Scheduling | Purpose |
|---------|---------|------------|---------|
| PostgreSQL 16 | `postgresql` | Always-on | Metadata store, watermarks, device registry, chat history |
| ClickHouse 24.3 | `clickhouse-server` | Always-on | Columnar analytics backend |
| Delta Loader | `delta_loader.py` | One-shot (Airflow-triggered) | Delta Lake → ClickHouse batch pipeline |
| ML Loader | `ml_loader.py` | Persistent (300s micro-batch) | ML predictions → ClickHouse continuous loader |

### Key Performance Characteristics

| Metric | Value | How |
|--------|-------|-----|
| **Peak Ingestion Throughput** | 309,000 rows/sec | Native binary protocol, 10K-row batch inserts |
| **Dashboard Query Latency** | < 14 ms | `AggregatingMergeTree` pre-computed rollups |
| **Read I/O Reduction** | 99% | Per-device watermark + 5-level Hive partition pruning |
| **Processing Guarantee** | Exactly-once | `ReplacingMergeTree` + PostgreSQL watermark idempotency |
| **ML Predictions TTL** | 30-day auto-purge | ClickHouse native `TTL` partition constraint |
| **Telemetry Retention** | 90 days (raw) / 3 years (daily) | Tiered TTL across table engines |

---

## Architecture

The Storage & Analytics Engine sits at the terminus of the ATLAS Lambda Architecture pipeline:

<div align="center">

![Storage & Analytics Engine — 6-Step Pipeline Architecture](./architecture-diagram.png)

</div>

The diagram illustrates the complete data flow: **Airflow** initiates the job hourly, the **Analytics Engine** executes the 6-step pipeline (Load Watermarks → Partition Pruning → Type Conversion → Dedup Filter → Binary Insert → Update Metadata), with PostgreSQL providing the watermark feedback loop and ClickHouse receiving high-speed binary inserts at **309K rows/sec**. The Databases & Output layer shows how raw telemetry feeds into **AggregatingMergeTree** hourly/daily rollups, powering dashboard queries in **< 0.014 seconds**.

### Data Flow Summary

```
Delta Lake (/data/refined)
    │
    ├─── delta_loader.py ──→ telemetry_refined (ReplacingMergeTree)
    │                            ├──→ telemetry_hourly (AggregatingMergeTree, via MV)
    │                            └──→ telemetry_daily  (AggregatingMergeTree, via MV)
    │
    └─── ml_loader.py ────→ telemetry_ml_predictions (ReplacingMergeTree, 30-day TTL)
```

---

## Directory Structure

```
storage/
├── app.py                          # Streamlit observability dashboard (5-page UI)
├── ml_app.py                       # ML dashboard + AI RCA copilot (Phi-4-Mini)
├── Dockerfile                      # Unified container (Debian Bookworm + CH + PG + Python)
├── entrypoint.sh                   # Container init (DB bootstrap, credential validation)
├── loader-start.sh                 # Delta loader startup wrapper (health checks)
├── ml-loader-start.sh              # ML loader startup wrapper (health checks)
├── supervisord.conf                # Process manager (4 services)
├── requirements.txt                # Python dependencies
├── clickhouse/
│   ├── init.sql                    # ClickHouse DDL (7 tables/views)
│   ├── delta_loader.py             # Delta Lake → ClickHouse pipeline (768 lines)
│   ├── ml_loader.py                # ML predictions → ClickHouse loader (446 lines)
│   ├── validation_queries.sql      # 6 data integrity checks
│   ├── override-listen.xml         # IPv4-only listener config
│   └── tests/                      # Unit tests
├── postgres/
│   └── init.sql                    # PostgreSQL DDL (7 tables, 8 indexes)
├── assets/
│   └── img/                        # Dashboard avatar assets
└── prompts/
    └── rca_system_prompt.txt       # AI copilot system prompt
```

---

## Quick Start

### Prerequisites

- Docker Desktop with ≥ 4 GB RAM allocated
- The upstream pipeline must have written refined Parquet to the shared `delta-refined` volume
- An `.env` file at the project root (copy from `.env.example`)

### 1. Start the Full ATLAS Stack

```bash
docker compose up -d
```

This brings up all services including `atlas-analytics` (the storage engine container).

### 2. Start Only the Analytics Container

```bash
docker compose up -d atlas-analytics
```

### 3. Trigger a One-Shot Pipeline Run (via Airflow)

Open the Airflow UI at `http://localhost:8081` and trigger the `atlas_batch_pipeline` DAG, or:

```bash
# Trigger via CLI
docker exec atlas-airflow-scheduler \
  airflow dags trigger atlas_batch_pipeline
```

### 4. Trigger a Direct Loader Run (bypassing Airflow)

```bash
# Delta loader (one-shot)
docker exec atlas-analytics python3 /app/delta_loader.py

# ML loader (one-shot)
docker exec atlas-analytics python3 /app/ml_loader.py
```

### 5. Access the Dashboard

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Streamlit Dashboard | `http://localhost:8501` | None |
| ClickHouse HTTP | `http://localhost:8124` | `atlas` / (from `.env`) |
| ClickHouse Native | `localhost:9002` | `atlas` / (from `.env`) |
| PostgreSQL | `localhost:5433` | `atlas` / (from `.env`) |

### 6. Verify Data Integrity

```bash
# Run all 6 validation checks
docker exec atlas-analytics \
  clickhouse-client --multiquery < /app/init-scripts/validation_queries.sql
```

---

## Related Documentation

| Document | Description |
|----------|-------------|
| [Architecture & Pipeline](./architecture-and-pipeline.md) | Deep dive into the 6-step pipeline, watermark logic, and fault tolerance |
| [Database Schema](./database-schema.md) | ClickHouse tables, AggregatingMergeTree engines, PostgreSQL metadata schema |
| [ML Loader Service](./ml-loader-service.md) | Asynchronous ML micro-batch loader, TTL policies, bulk-insert strategy |
| [Configuration Reference](./configuration-reference.md) | All environment variables, ports, Docker Compose, and supervisord config |
| [Operations Runbook](./operations-runbook.md) | Validation queries, health checks, troubleshooting, and startup sequence |

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|------------|---------|---------|
| Analytics DB | ClickHouse | 24.3 | Columnar OLAP, sub-ms queries |
| Metadata DB | PostgreSQL | 16 | Watermarks, device registry, chat history |
| Data Format | Apache Parquet | — | Columnar storage with Snappy/zstd compression |
| Table Format | Delta Lake | 0.14+ | ACID transactions, time travel |
| Runtime | Python | 3.11 | Pipeline logic, dashboard |
| Orchestration | Apache Airflow | 2.7.1 | DAG scheduling, `@hourly` triggers |
| Dashboard | Streamlit | 1.34+ | Real-time observability UI |
| AI Engine | Ollama (Phi-4-Mini) | — | Root Cause Analysis copilot |
| Process Manager | Supervisord | 4.2+ | Multi-service container orchestration |
| Container | Docker (Debian Bookworm) | — | Unified deployment |

---

## Container Resource Allocation

```yaml
deploy:
  resources:
    limits:
      cpus: '3.0'
      memory: 4G
```

The `atlas-analytics` container is the heaviest in the ATLAS stack due to co-locating two database engines and the Streamlit UI. The 4 GB memory limit is sized for:
- ClickHouse buffer pool and merge operations
- PostgreSQL shared buffers
- Pandas DataFrames during batch processing (10K-row batches)
- Streamlit session state and Plotly rendering

---

<div align="center">

**[Architecture →](./architecture-and-pipeline.md)** · **[Schema →](./database-schema.md)** · **[ML Loader →](./ml-loader-service.md)** · **[Config →](./configuration-reference.md)** · **[Runbook →](./operations-runbook.md)**

</div>
