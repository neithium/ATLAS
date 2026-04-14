# ATLAS Delta Lake - Livewire Mode Implementation

This document details the architecture, implementation, and troubleshooting history of the `livewire` execution mode for the ATLAS Delta Lake Deduplication Engine.

## Overview

The `livewire` mode is designed for continuous, near-real-time synchronization and deduplication of telemetry data arriving from the upstream Spark processing layers. Unlike the benchmark or legacy batch modes which process finite historical datasets, `livewire` operates indefinitely. 

It actively monitors a shared volume mount (`/stream_raw`) and ingests incoming files as they are written by the upstream processor containers, executing aggressive Delta `MERGE` operations on micro-batches to update the primary refined Lakehouse table.

## How it Works

The implementation relies heavily on **Spark Structured Streaming**, but uses a custom `foreachBatch` function to hook into Delta Lake's native ACID `MERGE` commands.

### 1. Dual-Ingestion Array (Batch & Stream Support)
The upstream processor writes outputs into either `batch/` or `stream/` partition folders. The pipeline utilizes a wildcard path to ingest from both dynamically without running two separate stream consumers:
```python
stream_df = spark.readStream.schema(schema).parquet(f"{source_path}/*/*.parquet")
```
*Reference: [delta_merge_pipeline.py](../delta_merge_pipeline.py)*

### 2. Schema Alignment (`coalesce`)
Because the folder structures hold slightly different schemas (streaming uses `window_start`/`window_end` whereas batch uses `event_date`), the pipeline performs a runtime schema alignment using Spark's `coalesce` function mapping both to `metric_time`:
```python
aligned_df = (
    batch_df
    .withColumn("metric_time", coalesce(col("window_start"), to_timestamp(col("event_date")), current_timestamp()))
    # ...
)
```

### 3. Continuous Micro-Batches
The pipeline employs a fast `5 seconds` trigger interval. In every interval, whatever data has landed in the folder is passed to `process_livewire_batch` as a static `DataFrame`. 
If `rows == 0`, it skips processing. If `rows > 0`, it executes the full Deduplication and Update `MERGE` transaction.

### 4. Fault Tolerance (Checkpointing)
To guarantee "Exactly-Once" semantics and ensure no data loss (even if the container crashes or lags), the state is backed by Checkpoints.
```python
.option("checkpointLocation", f"{PipelineConfig.CHECKPOINT_PATH}/livewire")
```
As micro-batches process successfully, Spark records the exact file offsets safely persisted to the checkpoint directory. 

---

## Technical Challenges & Resolutions

During the initial implementation, multiple issues were encountered and resolved.

### Problem 1: Silent Pipeline (No Logs in Docker)
**Issue:** The streaming pipeline was silently processing in the background, appearing frozen. No logs validating micro-batch iterations were printed to the terminal.
**Diagnosis:** Python buffers `stdout` natively. When running inside detached Docker containers, these logs are not flushed to the daemon unless explicitly forced or until the buffer is full.
**Resolution:** Added `flush=True` to the native Python `print()` statements monitoring the loop.
```python
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏳ Waiting for stream data...", flush=True)
```

### Problem 2: Python `UnboundLocalError` Crashing the Stream
**Issue:** When the first files hit the directory and `rows > 0`, the stream crashed with `UnboundLocalError: local variable 'datetime' referenced before assignment`.
**Diagnosis:** A `from datetime import datetime` import was mistakenly injected directly inside the conditional `if rows == 0:` block. In Python, if a variable is assigned anywhere inside a function, the compiler treats it as local to the *entire* function. When `rows > 0`, the `if` block was bypassed, leaving `datetime` unassigned globally when the code attempted to access it later to log the micro-batch start.
**Resolution:** Removed the scoped local import inside the `if` statement block and relied on the global namespace.

### Problem 3: Docker-Compose Generating Ghost Folders
**Issue:** Folders named `streaming_data_producer.py`, `streaming_merge_pipeline.py`, and `data/` were appearing inside the `delta_lake` codebase locally as hollow directories whenever the container spun up.
**Diagnosis:** The `docker-compose.yml` contained volume mount definitions mirroring files into the container. Because the target files didn't actually exist locally, Docker interpreted the paths as intended directories, automatically spawning empty directories to satisfy the mount constraint.
**Resolution:** Scrubbed the non-existent targets from the `volume` mappings in `docker-compose.yml` and deleted the local ghost directories.

### Problem 4: "Batch is falling behind" Warning
**Issue:** Spark threw warnings indicating: `Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 5098 milliseconds`. The concern was potential data loss.
**Diagnosis:** This is entirely benign. Spark tries to loop every 5 seconds, but heavily resourced operations (like loading data, evaluating the execution plan, and running an atomic `MERGE`) might occasionally take `5.1` seconds.
**Resolution:** Documented that Spark queues incoming parquet files during delays. It does not drop them. The checkpointing architecture natively compensates for trailing process completion, completely avoiding data loss.
