# ATLAS Airflow Orchestration Layer

Apache Airflow DAGs that coordinate the ATLAS Lambda architecture — batch path, stream path, and operational supervisors.

**Author:** Nandini

**Related docs:** Kafka broker setup in [`kafka/README.md`](../kafka/README.md). Spark job internals in [`processing/readme.md`](../processing/readme.md).

---

# Overview

Airflow is the **control plane** for ATLAS. It does not run heavy compute itself — it triggers and supervises work inside Docker containers via the mounted Docker Unix socket:

- **Batch path:** Ingestion export → Spark batch → parquet sensor → ClickHouse load → verify
- **Stream path:** Kafka export → ensure Spark streaming → ensure Delta livewire

```text
┌──────────────────────────────────────────────────────────┐
│              Apache Airflow (orchestration)               │
│  atlas_batch_pipeline  │  atlas_stream_pipeline          │
│  supervisors + DLQ monitor + kafka health                 │
└──────────────────────────────────────────────────────────┘
         │ docker exec via Unix socket
         ▼
  atlas-ingestion │ atlas-processor │ atlas-lakehouse │ atlas-analytics
```

---

# Services (docker-compose)

| Service | Host port | Role |
|---------|-----------|------|
| `airflow-db` | internal | PostgreSQL metadata store |
| `airflow-webserver` | `8081 → 8080` | DAG UI |
| `airflow-scheduler` | internal | Executes scheduled DAGs |

Volumes mounted into Airflow containers:

- `./orchestration/dags` → `/opt/airflow/dags`
- `./orchestration/plugins` → `/opt/airflow/plugins`
- `/var/run/docker.sock` → Docker API for container exec

---

# Required Airflow Connection

Create once in the UI (`Admin → Connections`):

| Conn Id | Type | Host |
|---------|------|------|
| `atlas_ingestion_api` | HTTP | `http://atlas-ingestion:8001` |

---

# DAG Inventory

| DAG ID | Schedule | Purpose |
|--------|----------|---------|
| `atlas_batch_pipeline` | `@hourly` | Full batch: export → Spark → sensor → ClickHouse → verify |
| `atlas_stream_pipeline` | `*/15 * * * *` | Stream: export → streaming supervisor → livewire supervisor |
| `telemetry_master_pipeline` | `@hourly` | Customer-scoped batch (PCID/ACID export) |
| `atlas_streaming_supervisor` | `*/10 * * * *` | Restarts `kafka_streaming.py` if stopped |
| `atlas_dlq_monitor` | `@hourly` | Reports DLQ topic offsets |
| `atlas_kafka_health` | Manual | Broker reachability check |

---

# Batch Pipeline Workflow

**DAG:** `atlas_batch_pipeline`  
**File:** `dags/dag_master_pipeline.py`

```text
trigger_ingestion_export
        │  POST /fleet/telemetry/export
        ▼
trigger_spark_batch_processing
        │  spark-submit /app/jobs/batch_job.py
        ▼
check_refined_parquet_exists
        │  PythonSensor — waits for *.parquet in /data/refined
        ▼
trigger_clickhouse_load
        │  python3 /app/delta_loader.py
        ▼
verify_data_load
        │  ClickHouse HTTP sanity check
        ▼
log_pipeline_status
```

### Design decisions

- **`@hourly` schedule** — aligns with rolling archive windows
- **PythonSensor before ClickHouse** — loader never runs on empty refined data
- **Detached Docker exec** — long Spark jobs don't false-fail with "up for retry"
- **ClickHouse verify via HTTP** — no shell `bc` dependency inside Airflow
- **Loader path:** `/app/delta_loader.py`

Extended batch steps (RAW sensor, lakehouse MERGE) live in `dags/atlas_pipeline_ops.py`.

---

# Stream Pipeline Workflow

**DAG:** `atlas_stream_pipeline`  
**File:** `dags/dag_stream_pipeline.py`

```text
trigger_stream_kafka_export
        ▼
ensure_kafka_streaming_job
        ▼
ensure_lakehouse_livewire
        ▼
log_stream_health
```

ClickHouse loading is handled by the batch DAG, not the stream DAG.

---

# Docker Exec Helper

**File:** `dags/atlas_utils.py`

Airflow calls the Docker Unix socket via `curl` subprocess (avoids breaking the Airflow image with the `docker` Python SDK):

| Function | Use |
|----------|-----|
| `_docker_exec()` | Run command, poll until exit |
| `docker_exec_or_raise()` | Same, raises on non-zero exit |
| `docker_exec_fire_and_forget()` | Start long-running jobs |
| `container_is_running()` | Container state check |
| `container_top_contains()` | Process pattern check |
| `wait_for_container_process()` | Poll until process up/down |

Shared task callables: `dags/atlas_pipeline_ops.py`.

---

# Module Structure

```text
orchestration/
├── README.md                    # This document
├── dags/
│   ├── atlas_utils.py           # Docker socket exec helpers
│   ├── atlas_pipeline_ops.py    # Shared batch/stream task callables
│   ├── dag_master_pipeline.py   # atlas_batch_pipeline
│   ├── dag_stream_pipeline.py   # atlas_stream_pipeline
│   ├── telemetry_pipeline.py    # telemetry_master_pipeline
│   ├── dag_streaming_supervisor.py
│   ├── dag_dlq_monitor.py
│   └── dag_kafka_health.py
└── deprecated_dags/               # Superseded prototypes
```

Stack startup (includes Airflow services) uses `kafka/scripts/single.bat` or `cluster.bat` — see [`kafka/README.md`](../kafka/README.md).

---

# Execution Guide

## 1. Start stack (includes Airflow)

From repo root:

```powershell
.\single.bat
```

## 2. First-time Airflow setup

```powershell
docker compose run --rm airflow-scheduler airflow db init

docker compose run --rm airflow-scheduler airflow users create `
  --username admin --password admin --firstname Admin --lastname User `
  --role Admin --email admin@atlas.local
```

Add the `atlas_ingestion_api` connection (see above).

## 3. Open Airflow UI

http://localhost:8081 — unpause `atlas_batch_pipeline` and `atlas_stream_pipeline`.

## 4. Trigger runs manually

```powershell
docker exec airflow-scheduler airflow dags trigger atlas_stream_pipeline
docker exec airflow-scheduler airflow dags trigger atlas_batch_pipeline
docker exec airflow-scheduler airflow dags trigger atlas_kafka_health
```

---

# Testing

## Import check

```powershell
docker exec airflow-scheduler airflow dags list-import-errors
docker exec airflow-scheduler airflow dags list | findstr atlas_
```

**Expect:** No import errors; all six DAGs listed.

## Stream path (fast, ~2 min)

```powershell
docker exec airflow-scheduler airflow dags trigger atlas_stream_pipeline
```

**Expect:** All four tasks green.

## Batch path (slow, 30–90 min)

```powershell
# Seed data first
docker exec atlas-ingestion python3 /app/v2/scripts/prefill_tsdb.py --days 1 --limit 200 --skip-archive
docker exec atlas-ingestion python3 /app/v2/scripts/manual_archive.py

docker exec airflow-scheduler airflow dags trigger atlas_batch_pipeline
```

**Expect:** Through `verify_data_load` with ClickHouse count > 0:

```powershell
curl "http://127.0.0.1:8124/?user=atlas&password=atlas_secure_pwd" -Method POST -Body "SELECT count() FROM atlas.telemetry_refined"
```

---

# Troubleshooting

## Export task HTTP / connection errors

Connection must be `http://atlas-ingestion:8001` (not port 80 or 8000).

## Spark task "up for retry"

Ensure latest `atlas_utils.py` uses detached exec + polling.

## `ModuleNotFoundError: atlas_utils`

```python
import sys
sys.path.append('/opt/airflow/dags')
```

## `verify_data_load` fails (avg = 0)

Check Spark logs and refined parquet count inside `atlas-analytics`.

## `telemetry_master_pipeline` step 1 fails (404)

DAG endpoint may need updating to `/telemetry/latest/export` — check `dags/telemetry_pipeline.py`.

---

# Technologies

- Apache Airflow 2.7.1 (LocalExecutor)
- Python 3.10
- Docker Unix socket API (`curl`)
- ClickHouse HTTP (verification)
- SimpleHttpOperator → ingestion API

---

# Integration

| Module | Integration |
|--------|-------------|
| `kafka/` | Brokers buffer exported telemetry; health/DLQ DAGs monitor Kafka |
| `ingestion/` | HTTP export endpoints triggered by DAGs |
| `processing/` | Airflow runs `batch_job.py`, supervises `kafka_streaming.py` |
| `delta_lake/` | Stream DAG ensures `run_livewire.py` |
| `storage/` | Batch DAG triggers `delta_loader.py`, verifies ClickHouse |

---

# Output

- Scheduled, auditable pipeline runs
- Verified ClickHouse loads after batch cycles
- Continuous stream path supervision
- DLQ and broker health visibility via supervisor DAGs
