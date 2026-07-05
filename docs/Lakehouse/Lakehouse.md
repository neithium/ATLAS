

#  ATLAS Lakehouse: Delta Lake Deduplication Engine

The **ATLAS Lakehouse Module** is the immutable Source of Truth for the ATLAS telemetry platform. Built on **Apache Spark** and **Delta Lake**, this module bridges the gap between chaotic, high-volume upstream streaming ingestion and highly structured, read-optimized downstream analytics.

It executes aggressive, ACID-compliant `MERGE` (Upsert) operations to strip massive rolling-window duplicates from raw telemetry, enforces strict 5-level deep partitioning, and continuously optimizes storage for sub-second analytical queries.

---

##  Core Features Listing

1. **ACID-Compliant `MERGE` Deduplication:** Mathematically strips up to 85.7% duplicate data originating from 7-day rolling window ingestion patterns using a Triple-Hash composite primary key.
2. **Schema-Defensive Streaming (Livewire):** Continuously ingests near-real-time data, automatically resolving upstream schema drift and dynamically coercing disparate temporal columns into a unified timeline.
3. **5-Level Deep Partitioning:** Organizes data physically on disk by `report_type -> partition_date -> platform_customer_id -> application_customer_id -> device_id` for extreme data skipping during query execution.
4. **Z-ORDER Clustering & Zstd Compression:** Utilizes Z-standard compression (~30% better than Snappy) and Z-ORDER clustering (by temporal/device keys) to optimize Parquet block reads.
5. **Micro-SLA Observability Dashboard:** Silently records pipeline performance metrics (read latency, merge latency, row throughput) directly to a dedicated Delta table (`system_metrics`), decoupling Lakehouse SLA reporting from upstream API bottlenecks.
6. **Exponential Backoff Resilience:** Implements self-healing retry logic to survive transient `SparkFileNotFoundException` race conditions when `OPTIMIZE` tasks collide with micro-batch writes.
7. **Fault-Tolerant Checkpointing:** Leverages strict JSON state-tracking (Batch) and Spark Structured Streaming Checkpoints (Livewire) to guarantee exactly-once processing even after complete container destruction.

---

##  Enterprise Data Engineering Principles

The ATLAS Lakehouse is engineered around four core pillars of data engineering to guarantee stability at scale (tested up to 100,000+ devices).

### 1. Scalability

* **Optimized Data Generation:** The native `generate_data.py` uses Spark DataFrame operations (`explode()` and `sequence()`) combined with `broadcast()` joins for metric profiles. This eliminated Python row-by-row iteration, reducing 100k-device generation time from 55 seconds to 35 seconds.
* **The "Small File" Solution:** High-frequency streaming creates thousands of tiny Parquet files that destroy read performance. The Lakehouse implements periodic `OPTIMIZE` tasks (configurable via `OPTIMIZE_EVERY_N_BATCHES`) to compact micro-batches into optimal **128 MB Target File Sizes**, drastically reducing file metadata overhead for ClickHouse readers.
* **Vertical Scaling Configuration:** Explicitly controls JVM footprints (`SPARK_EXECUTOR_CORES=6`, `SPARK_EXECUTOR_MEMORY=8g`) to maximize local processing efficiency while preventing host-OS starvation.

### 2. Reliability (Schema Evolution & Data Integrity)

* **Schema-Defensive Programming:** Upstream data often drifts (e.g., streaming uses `window_start` while batch uses `event_date`). The `run_livewire.py` script dynamically inspects the schema of every micro-batch, safely resolving temporal columns via `coalesce()` and falling back to `current_timestamp()` to prevent `AnalysisException` crashes.
* **Isolated Storage Volumes:** The Lakehouse decouples computational processing from storage. Upstream producers write to a shared volume (`delta-refined`) mapped to `/stream_raw`. The Lakehouse reads from this queue, executes the `MERGE`, and writes to `/refined`. Downstream consumers (ClickHouse) mount `/refined` in strictly **Read-Only (`:ro`)** mode, ensuring the analytics layer can never corrupt the Source of Truth.

### 3. Fault Tolerance

* **ACID Transaction Log Reconciliation:** In high-concurrency environments, background `VACUUM` or `OPTIMIZE` operations can alter physical Parquet files while a micro-batch is processing, leading to `SparkFileNotFoundException`. The `execute_merge_deduplication` function mitigates this via explicit Spark Catalog invalidation (`spark.catalog.refreshByPath()`) and exponential backoff (1s, 2s, 4s), allowing the Delta transaction log to resynchronize without crashing the pipeline.
* **Stateless Recovery:** Whether running in Livewire or Benchmark mode, if a Spark executor runs Out-Of-Memory (OOM) or the container restarts, the `CheckpointManager` guarantees the pipeline resumes exactly at the last unprocessed file offset. No duplicates are inserted, and no data is skipped.

### 4. Latency & Observability

* **Sub-Second Targeted MERGE:** By partitioning the target Delta table deeply and deduplicating the *source* DataFrame in-memory using `row_number() over Window` before executing the `MERGE`, the pipeline minimizes shuffle overhead, achieving P95 `MERGE` latencies of < 7.5 seconds on multi-million row datasets.
* **Zero-Overhead Metrics:** The `LatencyTracker` calculates P50, P95, and P99 metrics in memory. At the end of a micro-batch, a single-row DataFrame is created and appended to `delta./refined/system_metrics`. This `try/except` wrapped block ensures observability never crashes the core data pipeline.

---

##  Operational Modes

The pipeline operates in distinct modes based on deployment needs.

### 1. Livewire Mode (`run_livewire.py`)

**Purpose:** Infinite, near-real-time streaming deduplication for production environments.

* **Mechanics:** Bypasses traditional batch loading by using `spark.readStream` targeted at wildcard paths (`/stream_raw/*/*.parquet`). This allows it to dynamically ingest both `batch/` and `stream/` partitioned data generated upstream.
* **Custom `foreachBatch`:** Spark Structured Streaming doesn't natively support Delta `MERGE` on streaming inputs. Livewire circumvents this by passing each 5-second micro-batch to a custom `foreachBatch` wrapper, converting the stream into a static DataFrame, executing schema alignment, and triggering the ACID `MERGE`.

### 2. Benchmark Mode (`run_benchmark.py`)

**Purpose:** Extreme scale load-testing and historical data backfilling.

* **Rolling Window Simulation:** Real-world telemetry arrives with massive overlap (e.g., if a server loses connection, it sends historical logs when reconnected). The data generator builds `NUM_DAYS` of daily files. Each file contains a 7-day rolling window of timestamps.
* *The Math:* 1 Day = 288 ticks (5-min intervals). 7 Days = 2016 ticks. When Day 2 arrives, 1728 rows are identical to Day 1, and 288 are new.
* *The Result:* Benchmark mode proves the pipeline can identify and strip the expected **85.7% duplicate overlap** at scale.


* **Incremental Processing:** Discovers all `file_date` partitions, filters out previously completed batches using `pipeline_state.json`, and incrementally merges them into the Lakehouse, printing real-time throughput metrics.

---

##  Deep Dive: The Delta Core Engine (`delta_core.py`)

The heartbeat of the Lakehouse is the `execute_merge_deduplication()` function.

1. **Source Deduplication:** Before touching disk, the incoming DataFrame is deduplicated in memory. If the upstream API sent two identical packets in the same 5-second window, a `Window` function sorts by `metric_time DESC` and drops the duplicate.
2. **Triple-Hash Key Resolution:** The pipeline matches the source to the target using a strict composite key:
```sql
target.device_id = source.device_id 
AND target.metric_time = source.metric_time 
AND target.application_customer_id = source.application_customer_id

```


3. **Transaction Execution:** `whenNotMatchedInsertAll()` fires, skipping the 85% overlap and appending only the net-new telemetry to the Parquet files.

---

##  Micro-SLA Dashboard & Internal Metrics

To prove the Lakehouse operates at sub-second latency independent of upstream Kafka/FastAPI bottlenecks, `run_livewire.py` maintains an autonomous System Metrics table.

**Schema (`/refined/system_metrics`):**

| Column | Type | Description |
| --- | --- | --- |
| `batch_id` | LONG | Structured Streaming micro-batch identifier |
| `timestamp` | TIMESTAMP | UTC execution time |
| `total_time` | DOUBLE | End-to-end processing latency |
| `merge_time` | DOUBLE | Isolated latency of the ACID `MERGE` transaction |
| `row_count` | LONG | Rows ingested in the micro-batch |

*By querying this table via ClickHouse, SREs can instantly view pipeline throughput (Rows/Sec) and diagnose if latency spikes are caused by Delta Lake transactions or upstream network congestion.*

---

##  Hard Limits & Hardware Ceilings (April 4th RCA)

During extreme-scale testing, the pipeline was pushed to simulate **100,000 devices** concurrently generating 7-day rolling windows (yielding exactly **201,600,000 raw rows** in a single file).

* **Generator Result:** The PySpark `generate_data.py` successfully wrote all 201.6M rows to ZSTD-compressed Parquet in just 414.97 seconds (~500,000 rows/sec).
* **Pipeline Result:** The pipeline suffered a fatal `java.lang.OutOfMemoryError`.
* **RCA:** A single, local Spark Executor constrained to `8g` of RAM cannot hold the shuffle buffers, string object metadata, and Delta file indexing required to execute a Sort-Merge Join on 200M+ rows simultaneously.
* **Conclusion:** The partitioning logic is mathematically sound, but local execution is strictly bound by RAM. For 100,000+ device bursts without micro-batching, this codebase requires deployment to a distributed cloud cluster (AWS EMR / Databricks) with a minimum of 16GB-32GB of RAM per executor. Local Docker benchmarks are certified stable up to 10,000 concurrent devices per batch.

---

##  Commands & Execution Reference

All operations are executed via Docker Compose within the `atlas-lakehouse` container.

**1. Run Livewire (Production Streaming)**

```bash
# Starts the continuous streaming daemon
docker compose run -e RUN_PIPELINE=y -e PIPELINE_MODE=livewire atlas-lakehouse

```

**2. Run Benchmark (End-to-End Test)**

```bash
# Generates 75k devices over 1 day, then merges them with OPTIMIZE enabled
docker compose run --rm \
  -e RUN_GENERATOR=y \
  -e RUN_PIPELINE=y \
  -e GENERATOR_MODE=benchmark \
  -e PIPELINE_MODE=benchmark \
  -e DEVICE_COUNT=75000 \
  -e BATCH_SIZE=10000 \
  -e NUM_DAYS=1 \
  -e START_DATE=2026-03-01 \
  -e OPTIMIZE_EVERY=9 \
  atlas-lakehouse

```

**3. Cleanup & Reset State**
If corrupt transaction logs occur during local dev testing, safely wipe the environment:

```bash
# Nuke raw queues and refined lakehouse to reset checkpoints
rm -rf ./data/raw/* ./data/refined/*
docker compose restart atlas-lakehouse

```