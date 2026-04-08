# ATLAS Livewire Mode - Real-Time Streaming Integration

## Overview

Livewire mode enables real-time integration of streaming Parquet data from the upstream processing layer into the Refined Delta Lake table. Unlike benchmark mode which processes pre-generated static data, livewire mode continuously monitors an input directory and processes new files as they arrive using Spark Structured Streaming.

**Key Characteristics:**
- Real-time micro-batch processing (default: 60-second windows)
- Automatic schema validation and alignment
- Delta MERGE deduplication with exactly-once semantics
- Fault-tolerant checkpointing
- Live metrics tracking and reporting

---

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│  ATLAS Processor Container                                  │
│  (Upstream Spark Batch/Streaming Job)                       │
│                                                              │
│  Outputs: Flattened Parquet files                          │
│  Location: /app/data/processed/stream                      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ Parquet Files (Streaming Source)
                     │ [Year, Month, Device, Metric, ...]
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  LIVEWIRE MODE (Delta Lake Container)                       │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Step 1: Read Stream                                  │  │
│  │ - Monitor /app/data/processed/stream for new files   │  │
│  │ - Trigger micro-batch every 60 seconds              │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                │
│                            ▼                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Step 2: Validate & Align Schema                     │  │
│  │ - Compare incoming schema vs Refined Layer contract  │  │
│  │ - Map column name variations                         │  │
│  │ - Auto-cast data types                               │  │
│  │ - Fill missing optional columns with NULL            │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                │
│                            ▼                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Step 3: Delta MERGE Deduplication                    │  │
│  │ - Key: (device_id, metric_time, app_customer_id)    │  │
│  │ - WHEN NOT MATCHED: INSERT ALL (new rows)            │  │
│  │ - WHEN MATCHED: Skip (already exists)                │  │
│  │ - Update partition columns (partition_date)          │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                │
│                            ▼                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Step 4: Checkpoint & Metrics                         │  │
│  │ - Record batch success in checkpoint dir             │  │
│  │ - Track rows processed, merged, inserted             │  │
│  │ - Calculate throughput and latency metrics           │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │ /refined       │
                    │ (Refined Layer)│
                    │ Delta Table    │
                    │ 5-Level Parts  │
                    └────────────────┘
```

### Refined Layer Schema

The Refined Layer expects 35 fields in a flat structure (already flattened by upstream processor):

**Core Identity** (Triple-Hash Key):
- `device_id` (String) - Device identifier
- `metric_time` (String) - YYYY-MM-DD HH:mm:ss format
- `application_customer_id` (String) - Application customer

**Partitioning** (5-Level):
- `report_type` (String) - Metric category
- `partition_date` (String) - YYYY-MM-DD (derived from metric_time)
- `platform_customer_id` (String) - Platform customer
- `application_customer_id` (String) - Application customer
- `device_id` (String) - Device

**Metric Values**:
- `MetricValue` (Double) - Primary metric
- `avg_metric_value` (Double)
- `max_metric_value` (Double)
- `min_metric_value` (Double)
- `amb_temp` (Double)
- `co2_factor` (Double)
- `energy_cost_factor` (Double)

**Metadata**:
- `report_id` (String)
- `error_reason` (String)
- `status` (String)
- `model` (String)
- `tags` (String)
- `location_state` (String)
- `location_country` (String)
- `location_city` (String)
- `location_name` (String)
- `location_id` (String)

**Timestamps**:
- `datetime` (Double) - Unix timestamp
- `timeRangeEnd` (String)
- `Insertiontime` (String)
- `max_metric_time` (String)

**Inventory**:
- `cpu_inventory` (String)
- `memory_inventory` (String)
- `socket_count` (String)
- `pcie_devices_count` (String)

**Dates** (NEW - added by livewire):
- `file_date` (String) - YYYY-MM-DD
- `inventory_date` (String)
- `invention_date` (String)
- `location_date` (String)

---

## Running Livewire Mode

### Option 1: Local Testing (Delta Lake Module)

```bash
cd delta_lake

# Start livewire mode container
docker compose up -d

# Or with explicit environment:
docker compose run --rm \
  -e PIPELINE_MODE=livewire \
  -e RUN_GENERATOR=n \
  -e RUN_PIPELINE=y \
  spark
```

The container will:
1. Skip data generation (no local benchmark data needed)
2. Start Structured Streaming from `/app/data/processed/stream`
3. Wait for upstream data (appears to be idle until files arrive)
4. Process each micro-batch as files appear

### Option 2: Full ATLAS Stack Integration

```bash
cd ATLAS  # Root directory

# Ensure atlas-processor is running first:
docker-compose up -d atlas-processor

# Then start the livewire lakehouse:
docker-compose run --rm \
  --service-ports \
  -e PIPELINE_MODE=livewire \
  -e RUN_GENERATOR=n \
  -e RUN_PIPELINE=y \
  atlas-lakehouse
```

### Option 3: Direct Python Execution

```bash
# Activate environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run livewire mode
python3 delta_merge_pipeline.py --mode livewire --output /refined
```

**Options for direct execution:**
```
python3 delta_merge_pipeline.py --mode livewire [OPTIONS]

Options:
  --input TEXT                    Input parquet directory
                                  (default: /app/data/processed/stream)
  --output TEXT                   Output refined delta directory
                                  (default: /refined)
  --dynamic-allocation            Enable Spark dynamic allocation
  --executors N                   Number of Spark executors (default: 2)
  --executor-cores N              Cores per executor (default: 2)
  --executor-memory TEXT          Memory per executor (default: 2g)
```

---

## Configuration

### Environment Variables

Set via `-e` flag or in docker-compose.yml:

```bash
# Pipeline mode selection
PIPELINE_MODE=livewire              # or 'benchmark'

# Generator (skipped in livewire mode)
RUN_GENERATOR=n                     # Don't generate data
DEVICE_COUNT=100                    # Ignored in livewire

# Pipeline execution
RUN_PIPELINE=y                      # Must be 'y' to start streaming
RUN_VACUUM=n                        # VACUUM disabled for streaming mode

# Compression
COMPRESSION_CODEC=zstd              # or 'snappy', 'gzip'

# Spark execution
SPARK_DYNAMIC_ALLOCATION=false      # Enable dynamic scaling if needed
SPARK_EXECUTOR_INSTANCES=1          # For local[*] mode
SPARK_EXECUTOR_CORES=6              # Cores per executor

# Keep-alive mode (for docker-compose)
KEEP_ALIVE=y                        # Container stays running
```

### Configuration Class (LivewireConfig)

Modify in `livewire_streaming.py`:

```python
class LivewireConfig:
    # Paths
    STREAM_INPUT_PATH = "/app/data/processed/stream"
    REFINED_OUTPUT_PATH = "/refined"
    CHECKPOINT_PATH = "/refined/_streaming_checkpoints/livewire"
    
    # Streaming behavior
    TRIGGER_INTERVAL_SECONDS = 60      # Micro-batch window (↓ for faster, ↑ for fewer)
    AWAIT_TERMINATION_TIMEOUT = None   # None = run forever
    
    # Validation
    VALIDATE_SCHEMA = True             # Always validate incoming data
    INFER_SCHEMA_ON_START = False      # Debug mode for schema discovery
    
    # Optimization
    OPTIMIZE_DELTA_TABLE = False       # Disable for streaming (data always new)
    COMPRESSION_CODEC = "zstd"
    ZORDER_COLUMN = "metric_time"
```

---

## Monitoring & Metrics

### Console Output

Livewire mode prints real-time metrics for each batch:

```
================================================================================
  ATLAS LIVEWIRE MODE - REAL-TIME STREAMING INTEGRATION
================================================================================

Configuration:
  Input path:         /app/data/processed/stream
  Output path:        /refined
  Checkpoint path:    /refined/_streaming_checkpoints/livewire
  Trigger interval:   60 seconds
  Schema validation:  True

  ✓ Livewire streaming query started (ID: <query-id>)
  ✓ Waiting for data... (^C to stop)

         ℹ Batch 0: 0 rows (Empty batch - no new data)
         ✓ Batch 1: MERGE COMPLETE in 0.34s | Match: 500 | Insert: 2500
         ✓ Batch 2: MERGE COMPLETE in 0.28s | Match: 1200 | Insert: 1800
         ℹ Batch 3: 0 rows (Empty batch - no new data)
         ✓ Batch 4: MERGE COMPLETE in 0.45s | Match: 800 | Insert: 2200
         ℹ Throughput: 500 rows/sec (cumulative)

[User presses Ctrl+C]

================================================================================
  LIVEWIRE STREAMING SUMMARY
================================================================================

  Batches processed:           5
  Total rows processed:        4,000
  Rows merged (duplicates):    2,500
  Rows inserted (new):         4,200
  Schema mismatches encountered: 0
  Total merge time:            1.20s
  Total elapsed time:          245s
  Throughput:                  16 rows/sec
  Errors:                      0

================================================================================
```

### Metrics Structure

**Per-Batch Metrics** (printed after each batch):
- Batch ID
- Rows processed from current batch
- Rows matched (deduplicated)
- Rows inserted (new)
- Merge time (seconds)
- Throughput (rows/sec)

**Summary Metrics** (on shutdown):
- Total batches processed
- Cumulative rows processed
- Total duplicates removed (merged)
- Total new rows inserted
- Schema changes encountered
- Total merge time
- Total elapsed time
- Average throughput
- Error count

---

## Schema Validation & Alignment

Livewire includes intelligent schema handling for real-world integration scenarios.

### Column Name Mapping

Maps upstream column variations to canonical names:

```python
COLUMN_NAME_MAPPING = {
    # Upstream Name → Refined Layer Name
    "value": "MetricValue",
    "metric_value": "MetricValue",
    "event_time": "metric_time",
    "timestamp": "metric_time",
    "device": "device_id",
    ...
}
```

### Validation Flow

1. **Exact Match**: Schema matches perfectly → `status: PASS_EXACT_MATCH`
2. **Mapped Match**: Missing columns found and mapped → `status: ALIGNED_WITH_TRANSFORMATIONS`
3. **Type Mismatch**: Column exists but type differs → Auto-cast if possible
4. **Missing Optional**: Optional columns absent → Fill with NULL

### Fallback Strategy

If schema drastically differs:
- Validation report is logged with detailed mismatch information
- Data is **NOT skipped** — livewire continues with alignment
- Error logged but batch processing continues
- Application admin notified for schema evolution decision

---

## Checkpointing & Fault Tolerance

### How Checkpointing Works

Spark Structured Streaming uses the checkpoint directory to:
1. Track which files have been processed
2. Record batch state on successful completion
3. Enable recovery from last successful batch on restart

**Checkpoint Location:** `/refined/_streaming_checkpoints/livewire/`

**Contents:**
```
_streaming_checkpoints/livewire/
├── metadata/                    # Query metadata
├── sources/                     # Source offset tracking
├── offsets/                     # Per-batch offset state
└── batchMetadata/               # Batch execution logs
```

### Exactly-Once Delivery

Livewire guarantees that each row is processed **exactly once**:

1. **Micro-batch Isolation**: Each batch is atomic (all-or-nothing)
2. **Delta Transaction Log**: MERGE operation writes to Delta log atomically
3. **Idempotency**: If a batch fails during MERGE, it's safe to retry (same triple-hash key)
4. **Checkpoint Recovery**: On restart, skips already-processed files

### Recovery Scenarios

| Scenario | Behavior |
|----------|----------|
| **File Processing Fails** | Checkpoint is NOT advanced; batch retried next cycle |
| **MERGE Fails** | Delta log remains unchanged; processed batch remains in source |
| **Schema Mismatch** | Alignment attempted; batch still processed (logged) |
| **Connection Lost** | Streaming query automatically pauses; resumes on reconnection |
| **Container Restart** | Resumes from last committed checkpoint (no data loss) |

### Manual Checkpoint Management

```bash
# View checkpoint location
ls -la /refined/_streaming_checkpoints/livewire/

# Reset checkpoint (start fresh scan from first file)
rm -rf /refined/_streaming_checkpoints/livewire/

# Archive old checkpoints
tar -czf /refined/_streaming_checkpoints/livewire.tar.gz /refined/_streaming_checkpoints/livewire/

# Note: Resetting invalidates previously-processed state. Use with caution!
```

---

## Troubleshooting

### 1. "Waiting for data... (^C to stop)" - No Files Arriving

**Symptom**: Livewire started but shows empty batches.

**Cause**: Upstream processor hasn't written files yet.

**Solution**:
```bash
# Check if stream directory exists and has files
ls -la /app/data/processed/stream/

# Verify atlas-processor is running
docker ps | grep atlas-processor

# Check processor logs
docker logs atlas-processor

# If using benchmark data, place test files in stream directory:
cp /raw/benchmark_data/.../*.parquet /app/data/processed/stream/
```

### 2. "ERROR: livewire_streaming module not found"

**Symptom**: Python ImportError when starting livewire mode.

**Cause**: `livewire_streaming.py` or `livewire_schema_validator.py` missing.

**Solution**:
```bash
# Verify files exist
ls -la delta_lake/livewire_streaming.py
ls -la delta_lake/livewire_schema_validator.py

# Check docker volume mounts
docker compose config | grep -A 5 "livewire"

# Rebuild container with latest source
docker compose build --no-cache spark
```

### 3. "Schema Mismatch - Missing Columns: [...]"

**Symptom**: Validation report shows missing columns.

**Cause**: Upstream schema differs from expected (column name variation).

**Solution**:
1. Check validation report for suggested mappings
2. Update `COLUMN_NAME_MAPPING` in `livewire_schema_validator.py`:
```python
COLUMN_NAME_MAPPING = {
    "upstream_column_name": "MetricValue",  # Add your mapping
    ...
}
```
3. Restart livewire mode

### 4. "Partition Directory Does Not Exist"

**Symptom**: "path does not exist" error when running MERGE.

**Cause**: Delta table doesn't exist yet (first batch creating new partitions).

**Solution**: This is expected behavior for first batch. No action needed.

### 5. High Latency / Low Throughput

**Symptom**: Batches taking > 1 second, throughput < 100 rows/sec.

**Cause**: Insufficient Spark resources or slow disk I/O.

**Solution**:
```bash
# Increase executor resources
docker compose run --rm \
  -e SPARK_EXECUTOR_CORES=8 \
  -e SPARK_EXECUTOR_MEMORY=8g \
  -e SPARK_EXECUTOR_INSTANCES=3 \
  spark
```

---

## Performance Tuning

### Micro-Batch Window Size

```python
# In docker-compose.yml or direct execution
# Smaller window = lower latency but more overhead
# Larger window = higher throughput but higher latency

docker compose run --rm \
  -e PIPELINE_MODE=livewire \
  spark
# Default: 60 seconds

# To change, modify in livewire_streaming.py:
TRIGGER_INTERVAL_SECONDS = 30  # Faster batching
```

### Spark Executor Configuration

```bash
# For high throughput (4 Lakh+ devices)
docker compose run --rm \
  -e SPARK_EXECUTOR_CORES=8 \
  -e SPARK_EXECUTOR_MEMORY=8g \
  -e SPARK_EXECUTOR_INSTANCES=4 \
  -e SPARK_SHUFFLE_PARTITIONS=24 \
  spark
```

### Z-ORDER Optimization

Z-ORDER clustering is applied per MERGE (if enabled). For streaming:
- Enables fast point-in-time queries (metric_time range scans)
- Adds ~5-10% latency per batch
- Highly recommended for analytics workloads

```python
# In LivewireConfig
ZORDER_COLUMN = "metric_time"  # Cluster by this column
```

---

## Comparison: Benchmark vs Livewire

| Aspect | Benchmark | Livewire |
|--------|-----------|----------|
| **Input Source** | Pre-generated daily Parquet files | Real-time streaming files |
| **Processing** | Batch MERGE per date window | Micro-batch MERGE per trigger |
| **Checkpointing** | Resume from last date batch | Resume from file offset |
| **Schema Stability** | Fixed known schema | Dynamic with validation/alignment |
| **Latency** | Hours (batch window) | Minutes (micro-batch window) |
| **Throughput** | 10K+ rows/sec | 100+ rows/sec (depends on upstream) |
| **Use Case** | Testing, validation, backfill | Production streaming integration |
| **Fault Tolerance** | Date-level checkpoints | Batch-level atomic commits |
| **Data Generator** | Required (generate_data.py) | Not used (upstream processor) |
| **Deduplication Ratio** | 73.5% (7-day rolling window) | Varies (upstream data dependent) |

---

## Example: End-to-End Integration

### Full Stack Setup

```bash
# Terminal 1: Start full ATLAS stack
docker-compose up -d

# Terminal 2: Monitor atlas-processor (upstream)
docker logs -f atlas-processor

# Terminal 3: Monitor atlas-lakehouse (livewire)
docker logs -f atlas-lakehouse

# Terminal 4: Check refined volume (outputs)
watch -n 5 'find /refined -type f -name "*.parquet" | wc -l'
```

### Expected Flow

1. **atlas-processor** streams files to `/app/data/processed/stream/`
2. **atlas-lakehouse livewire mode** detects files every 60 seconds
3. **For each batch:**
   - Read parquet files
   - Validate schema (align if needed)
   - Execute MERGE deduplication
   - Checkpoint progress
4. **Refined Layer** accumulates deduplicated records
5. **atlas-analytics** reads from refined via delta-refined volume

### Verify Integration

```bash
# Check refined table structure
docker exec atlas-lakehouse pyspark
>>> from delta import DeltaTable
>>> dt = DeltaTable.forPath(spark, "/refined")
>>> dt.toDF().printSchema()
>>> dt.toDF().show(5)

# Check partition layout
>>> spark.read.parquet("/refined").show()

# Count final records
>>> spark.read.format("delta").load("/refined").count()
```

---

## Appendix: Key Classes & Functions

### LivewireConfig
Central configuration class for all streaming parameters.

### StreamingMetrics
Tracks per-batch and cumulative metrics for monitoring and debugging.

### livewire_merge_batch()
Core function executed per micro-batch:
1. Validate + align schema
2. Prepare partition columns
3. Execute Delta MERGE
4. Record metrics

### run_livewire_streaming()
Main entry point:
1. Initialize Structured Streaming source
2. Create streaming DataFrame with foreachBatch
3. Start query with checkpoint management
4. Print summary on shutdown

---

## Support

For issues or feature requests related to livewire mode:

1. **Schema Integration**: Update `COLUMN_NAME_MAPPING` in `livewire_schema_validator.py`
2. **Streaming Configuration**: Modify `LivewireConfig` in `livewire_streaming.py`
3. **Container Integration**: Update docker-compose environment variables
4. **Metrics/Monitoring**: Extend `StreamingMetrics` class

Default contact: ATLAS Team
