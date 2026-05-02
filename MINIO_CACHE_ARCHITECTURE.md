# MinIO Sliding Window Cache Architecture

## High-Performance Telemetry Pipeline Redesign

**Current Problem:**

- 110 seconds to fetch 20M rows (7 days × 10K devices × 2016 points)
- 74 seconds for 500 concurrent platforms
- Full TSDB scan on every API request
- System does not scale

**Solution: MinIO Sliding Window Cache**

- Pre-computed 7-day rolling cache in MinIO (Parquet)
- API reads from MinIO (1-5 seconds instead of 110s)
- Background job keeps cache fresh with delta updates only
- Minimal TSDB load

---

## 1. HIGH-LEVEL ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────┐
│                        EXISTING SYSTEM                      │
│  Real-time data → Kafka → Direct DB writes → TSDB (hot)    │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                    NEW: CACHE LAYER                         │
│  Background Job (every 5 min):                             │
│  1. Fetch delta from TSDB (last 5 min)                     │
│  2. Append to MinIO (no recompute)                         │
│  3. Cleanup data older than 7 days                         │
│  4. Compact small files periodically                       │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                      MinIO (S3-compatible)                  │
│  Path: s3://telemetry-cache/7day-rolling/                 │
│  Format: Parquet (columnar, compressed)                    │
│  Partition: By date/hour (yyyy/mm/dd/HH)                  │
│  Files: 100-200 MB each (avoid small file problem)         │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                      API READ PATH                          │
│  GET /telemetry/{pcid}/{acid}                              │
│  1. Identify required partitions (7-day window)            │
│  2. Read Parquet directly from MinIO                       │
│  3. Optional: Merge fresh delta (< 5 min old) from TSDB   │
│  4. Stream to Kafka (downstream systems)                   │
│  Response time: 2-5 seconds (vs 110 seconds)               │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. MinIO FOLDER STRUCTURE & NAMING

### Base Path

```
s3://telemetry-cache/
├── 7day-rolling/              # Main cache volume
│   ├── 2026/05/01/00/         # Date partitions (hourly)
│   │   ├── data_0001.parquet  # 100-200 MB each
│   │   ├── data_0002.parquet
│   │   ├── _metadata.json     # (metadata for hour)
│   │   └── _compaction.log
│   ├── 2026/05/01/01/
│   ├── 2026/05/01/02/
│   └── ...
│
├── metadata/
│   ├── last_sync.json         # Last processed timestamp per hierarchy
│   ├── cleanup_log.json       # Cleanup history
│   └── compaction_schedule.json
│
└── temp/                       # Staging for safe writes
    ├── pending_writes/
    └── compaction_work/
```

### File Naming Convention

```
data_{partition_id:04d}.parquet
├── partition_id = sequential counter per hour
└── Example: data_0001.parquet, data_0002.parquet, ...

_metadata.json format:
{
  "hour": "2026-05-01T00:00:00Z",
  "files": ["data_0001.parquet", "data_0002.parquet"],
  "row_count": 250000,
  "byte_size": 180000000,
  "last_device_id": "DEVICE_10000",
  "checksum": "sha256:abc123...",
  "schema_version": "1.0"
}
```

---

## 3. PARQUET SCHEMA & PARTITIONING

### Schema (Match 48-field golden record)

```python
# Column-level optimization
schema = {
  "metric_time": "timestamp[ns]",              # Index for range queries
  "device_id": "string",                        # Index for filtering
  "platform_customer_id": "string",            # Index (PCID)
  "application_customer_id": "string",         # Index (ACID)

  # Metrics (Snappy compressed)
  "avg_watts": "float32",
  "peak_watts": "float32",
  "min_watts": "float32",
  "cpu_watts": "float32",
  "gpu_watts": "float32",

  # Dimensions
  "amb_temp": "float32",
  "cpu_avg_freq": "float32",
  "cpu_max": "float32",
  "cpu_pwr_sav_lim": "float32",
  "cpu_util": "float32",

  # Device metadata
  "server_name": "string",
  "model": "string",
  "processor_vendor": "string",
  "server_generation": "string",

  # Status
  "status": "boolean",
  "report_type": "string",
  "metric_type": "string",
  "error_reason": "string",

  # Inventory
  "cpu_count": "int32",
  "socket_count": "int32",
  ... (remaining fields)
}

# Partition columns (NOT stored in file, only in path)
partition_columns = ["year", "month", "day", "hour"]
# Example: s3://bucket/7day-rolling/2026/05/01/00/data_0001.parquet
```

### Parquet Write Settings

```python
parquet_config = {
  "compression": "snappy",      # Fast + good ratio
  "compression_level": 7,
  "row_group_size": 50000,      # 50K rows per group (not whole file)
  "page_size": 1048576,         # 1 MB pages
  "dictionary_encoding": True,
  "statistics": True,           # Enable min/max stats for filter pushdown
  "data_page_version": "V2",
}
```

---

## 4. WORKFLOW: Delta Fetch & Append

### Background Job Schedule

- **Runs every 5 minutes**
- **Processes last 10 minutes of data** (5 min overlap for late arrivals)
- **Target file size: 100-200 MB** (≈ 500K-1M rows per file)

### Pseudocode: Delta Fetch & Append

```python
async def cache_maintenance_job():
    """
    Background job that runs every 5 minutes.
    Fetches only NEW data from TSDB, appends to MinIO cache.
    """
    while True:
        try:
            # Step 1: Get last sync timestamp
            last_sync = await load_metadata("metadata/last_sync.json")
            current_time = datetime.utcnow()

            # Step 2: Fetch DELTA from TSDB (only last 10 minutes)
            # This avoids full 7-day scan
            delta_data = await fetch_delta_from_tsdb(
                start_time=last_sync - timedelta(minutes=5),  # 5 min overlap
                end_time=current_time,
                batch_size=100000  # Fetch in chunks
            )

            if not delta_data:
                log.info("No new data")
                await asyncio.sleep(300)  # Wait 5 min
                continue

            # Step 3: Deduplicate with existing cache
            # (in case of overlaps)
            deduped_data = await deduplicate_with_cache(delta_data)

            # Step 4: Aggregate into 100-200 MB files
            file_batches = aggregate_into_batches(
                deduped_data,
                target_size_mb=150
            )

            # Step 5: Upload to MinIO with safe writes
            for batch_idx, batch_df in enumerate(file_batches):
                await safe_upload_batch(batch_df, current_time, batch_idx)

            # Step 6: Update metadata (atomic)
            await update_sync_metadata(
                last_sync=current_time,
                rows_processed=len(delta_data),
                timestamp=datetime.utcnow()
            )

            # Step 7: Cleanup files older than 7 days
            await cleanup_old_partitions(days=7)

            # Step 8: Compact small files (hourly)
            if should_compact(current_hour):
                await compact_hour_partitions()

            log.info(f"✅ Cache sync complete: {len(delta_data)} rows")

        except Exception as e:
            log.error(f"Cache sync failed: {e}")
            # Retry in 5 minutes

        await asyncio.sleep(300)  # Run every 5 minutes


async def fetch_delta_from_tsdb(
    start_time: datetime,
    end_time: datetime,
    batch_size: int = 100000
) -> List[dict]:
    """
    Fetch only NEW data (delta) from TSDB.
    Streams in chunks to avoid memory overhead.
    """
    pool = await get_db_pool()
    all_rows = []

    query = """
    SELECT
        metric_time, device_id, platform_customer_id,
        application_customer_id, avg_watts, peak_watts, ...
    FROM telemetry_live
    WHERE metric_time >= $1 AND metric_time < $2
    ORDER BY metric_time, device_id
    """

    async with pool.acquire() as conn:
        cursor = await conn.cursor(query, start_time, end_time)
        while True:
            batch = await cursor.fetch(batch_size)
            if not batch:
                break
            all_rows.extend(batch)

            # Convert to Parquet-friendly format
            yield batch

    return all_rows


async def safe_upload_batch(df: pd.DataFrame, hour: datetime, idx: int):
    """
    Safely upload batch to MinIO using temp-file pattern.
    """
    partition_path = f"2026/05/01/{hour.hour:02d}"
    filename = f"data_{idx:04d}.parquet"

    # Step 1: Write to temp location
    temp_path = f"temp/pending_writes/{uuid.uuid4().hex}.parquet"
    df.to_parquet(
        f"s3://telemetry-cache/{temp_path}",
        engine="pyarrow",
        **parquet_config
    )

    # Step 2: Verify checksum
    expected_rows = len(df)
    actual_rows = await verify_parquet(f"s3://telemetry-cache/{temp_path}")
    assert expected_rows == actual_rows, f"Row mismatch: {expected_rows} vs {actual_rows}"

    # Step 3: Atomic move to final location
    final_path = f"7day-rolling/{partition_path}/{filename}"
    await minio_client.move_object(
        source=temp_path,
        destination=final_path
    )

    # Step 4: Update partition metadata
    await append_to_metadata(partition_path, filename, actual_rows)

    log.info(f"✅ Uploaded: {final_path} ({len(df)} rows)")
```

### TSDB Query Optimization for Delta

```python
# Create index if missing
CREATE INDEX CONCURRENTLY idx_telemetry_live_metric_time
ON telemetry_live(metric_time DESC)
WHERE metric_time > NOW() - INTERVAL '8 days';

# For faster delta queries, use this pattern:
SELECT * FROM telemetry_live
WHERE metric_time >= NOW() - INTERVAL '10 minutes'  -- Only 10 min window!
ORDER BY metric_time, device_id;
```

---

## 5. API READ PATH: High-Speed Data Retrieval

### Pseudocode: API Endpoint

```python
@app.post("/pcid/{pcid}/acid/{acid}/telemetry/latest/export")
async def get_telemetry_from_cache(
    pcid: str,
    acid: str,
    days: int = 7,
    fetch_fresh_delta: bool = True
) -> None:
    """
    Read 7-day telemetry from MinIO cache (not TSDB).
    Optional: Merge fresh delta (< 5 min old) from TSDB for accuracy.

    Latency: 2-5 seconds (vs 110 seconds from TSDB)
    """

    device_ids = get_device_ids_for_hierarchy(pcid, acid)
    current_time = datetime.utcnow()
    start_time = current_time - timedelta(days=days)

    # Step 1: Identify required partitions
    required_partitions = calculate_partitions(start_time, current_time)
    # Returns: ["2026/05/01/00", "2026/05/01/01", "2026/05/01/02", ...]

    # Step 2: Stream Parquet files from MinIO
    kafka_producer = await get_kafka()

    for partition in required_partitions:
        # List files in partition
        files = await minio_client.list_objects(
            bucket="telemetry-cache",
            prefix=f"7day-rolling/{partition}/"
        )

        for file in files:
            if not file.endswith('.parquet'):
                continue

            # Stream read (chunked)
            await stream_parquet_to_kafka(
                s3_path=f"s3://telemetry-cache/7day-rolling/{partition}/{file}",
                pcid_filter=pcid,
                acid_filter=acid,
                device_ids=device_ids,
                producer=kafka_producer
            )

    # Step 3 (Optional): Merge fresh delta from TSDB
    if fetch_fresh_delta:
        last_cache_sync = await get_last_sync_time()
        fresh_delta = await fetch_delta_from_tsdb(
            start_time=last_cache_sync,
            end_time=current_time
        )

        for row in fresh_delta:
            if row['device_id'] in device_ids:
                await kafka_producer.send_and_wait(
                    "raw-server-metrics",
                    value=row
                )

    await kafka_producer.flush()
    return {"status": "ok", "devices": len(device_ids)}


async def stream_parquet_to_kafka(
    s3_path: str,
    pcid_filter: str,
    acid_filter: str,
    device_ids: List[str],
    producer
):
    """
    Stream Parquet from MinIO directly to Kafka.
    Use Parquet column projections to avoid loading unnecessary data.
    """

    # Parquet column projection (filter at read time)
    columns = [
        'metric_time', 'device_id', 'platform_customer_id',
        'application_customer_id', 'avg_watts', 'peak_watts', ...
    ]

    # Read with PyArrow (supports columnar reads)
    parquet_file = pq.ParquetFile(s3_path)

    for batch in parquet_file.iter_batches(
        batch_size=50000,
        columns=columns
    ):
        df = batch.to_pandas()

        # Filter to target hierarchy
        df = df[
            (df['platform_customer_id'] == pcid_filter) &
            (df['application_customer_id'] == acid_filter) &
            (df['device_id'].isin(device_ids))
        ]

        if df.empty:
            continue

        # Send to Kafka (chunked)
        for _, row in df.iterrows():
            await producer.send_and_wait(
                "raw-server-metrics",
                value=row.to_dict()
            )

    log.info(f"✅ Streamed {s3_path} to Kafka")
```

---

## 6. SLIDING WINDOW MAINTENANCE

### Pseudocode: Cleanup Old Data

```python
async def cleanup_old_partitions(days: int = 7):
    """
    Delete partitions older than 7 days.
    Runs as part of maintenance job.
    """
    current_time = datetime.utcnow()
    cutoff_time = current_time - timedelta(days=days)

    # List all partitions
    partitions = await minio_client.list_prefixes(
        bucket="telemetry-cache",
        prefix="7day-rolling/"
    )

    for partition in partitions:
        # Extract date from partition path (2026/05/01/00)
        partition_datetime = parse_partition_datetime(partition)

        if partition_datetime < cutoff_time:
            log.info(f"Deleting partition: {partition}")
            await minio_client.delete_objects(
                bucket="telemetry-cache",
                prefix=f"7day-rolling/{partition}/"
            )

        # Update cleanup log
        await log_cleanup_event(partition, deleted=True)


def parse_partition_datetime(partition: str) -> datetime:
    """
    Parse: 7day-rolling/2026/05/01/00
    Returns: datetime(2026, 5, 1, 0, 0, 0)
    """
    parts = partition.strip('/').split('/')
    year, month, day, hour = parts[1:5]
    return datetime(int(year), int(month), int(day), int(hour))
```

### Handling Late-Arriving Data

```python
# Overlap window: 1 hour
# When syncing at 2026-05-01 10:00:
# - Include data from 2026-05-01 08:55 to 10:00
# - This catches any late-arriving data from previous sync

overlap_minutes = 60  # 1 hour overlap

last_sync = await load_metadata("metadata/last_sync.json")
fetch_start = last_sync - timedelta(minutes=overlap_minutes)
fetch_end = datetime.utcnow()

# When deduplicating:
# - Check if (device_id, metric_time) already exists in cache
# - If yes, replace (take latest version)
```

---

## 7. FILE COMPACTION STRATEGY

### Pseudocode: Compact Small Files

```python
async def compact_hour_partitions():
    """
    Merge small files into 100-200 MB target size.
    Run hourly (after all writes for previous hour complete).
    """

    current_time = datetime.utcnow()
    hour_to_compact = current_time - timedelta(hours=1)
    partition = format_partition(hour_to_compact)

    # Step 1: List files in partition
    files = await list_parquet_files(partition)

    if len(files) <= 1:
        return  # No compaction needed

    # Step 2: Calculate total size
    total_size = sum(await minio_client.stat_object(f) for f in files)

    if total_size < 100_000_000:  # < 100 MB, compact it
        log.info(f"Compacting {len(files)} files in {partition} ({total_size} bytes)")

        # Step 3: Read all files
        dfs = []
        for file in files:
            df = pd.read_parquet(f"s3://telemetry-cache/{file}")
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)

        # Step 4: Sort for deterministic output
        combined_df.sort_values(['metric_time', 'device_id'], inplace=True)

        # Step 5: Write compacted file
        compacted_file = f"7day-rolling/{partition}/data_0001_compacted.parquet"
        await safe_upload_batch(combined_df, hour_to_compact, 0)

        # Step 6: Delete old files
        for file in files:
            await minio_client.remove_object("telemetry-cache", file)

        log.info(f"✅ Compaction complete: {len(files)} files → 1 file")
```

---

## 8. FAULT TOLERANCE & SAFETY

### Safe Write Pattern

```python
# All writes use this pattern:
# 1. Write to temp location
# 2. Verify (checksum, row count)
# 3. Atomic rename/move to final location
# 4. Update metadata last

async def safe_write_to_minio(data: pd.DataFrame, final_path: str):
    """
    Three-phase write for atomic operations.
    """

    # Phase 1: Write to staging
    staging_id = uuid.uuid4().hex
    staging_path = f"temp/staging/{staging_id}.parquet"

    try:
        data.to_parquet(f"s3://telemetry-cache/{staging_path}", ...)
    except Exception as e:
        log.error(f"Write failed at staging: {e}")
        # Cleanup staging file
        await minio_client.remove_object("telemetry-cache", staging_path)
        raise

    # Phase 2: Verify
    try:
        verify_result = await verify_parquet(staging_path)
        assert verify_result['row_count'] == len(data)
        assert verify_result['checksum'] == calculate_checksum(data)
    except AssertionError as e:
        log.error(f"Verification failed: {e}")
        await minio_client.remove_object("telemetry-cache", staging_path)
        raise

    # Phase 3: Atomic move (rename)
    try:
        await minio_client.copy_object(
            src_bucket="telemetry-cache",
            src_key=staging_path,
            dst_bucket="telemetry-cache",
            dst_key=final_path
        )
        await minio_client.remove_object("telemetry-cache", staging_path)
    except Exception as e:
        log.error(f"Atomic move failed: {e}")
        # Leave staging file for manual recovery
        raise


# Retry logic
async def safe_write_with_retry(
    data: pd.DataFrame,
    final_path: str,
    max_retries: int = 3,
    backoff_base: float = 2.0
):
    """
    Exponential backoff retry for write operations.
    """
    for attempt in range(max_retries):
        try:
            await safe_write_to_minio(data, final_path)
            return
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait_time = backoff_base ** attempt
            log.warning(f"Write failed (attempt {attempt + 1}), retrying in {wait_time}s")
            await asyncio.sleep(wait_time)
```

---

## 9. RECOMMENDED CONFIGURATIONS

```python
# === SYNC JOB SETTINGS ===
SYNC_INTERVAL_MINUTES = 5               # Run every 5 minutes
SYNC_LOOKBACK_MINUTES = 10              # Fetch last 10 minutes (5 min overlap)
TSDB_BATCH_SIZE = 100000                # Rows per DB fetch
LATE_ARRIVAL_WINDOW_HOURS = 1           # 1 hour overlap

# === PARQUET SETTINGS ===
TARGET_FILE_SIZE_MB = 150               # 150 MB per file (range: 100-200)
TARGET_ROWS_PER_FILE = 750000           # ~750K rows per 150 MB file
ROWS_PER_BATCH = 50000                  # Batch for streaming reads
ROW_GROUP_SIZE = 50000                  # Parquet row group size

# === CLEANUP SETTINGS ===
RETENTION_DAYS = 7                      # Keep 7 days rolling window
CLEANUP_INTERVAL_HOURS = 6              # Run cleanup every 6 hours
CLEANUP_BUFFER_DAYS = 0.5               # Safety margin (12 hours extra)

# === COMPACTION SETTINGS ===
COMPACTION_INTERVAL_HOURS = 1           # Compact hourly
MIN_FILES_TO_COMPACT = 3                # Only if 3+ files
MIN_PARTITION_SIZE_MB = 50              # Only if < 50 MB total
MAX_COMPACTION_SIZE_MB = 300            # Don't compact if > 300 MB

# === MINIO CLIENT SETTINGS ===
MINIO_CONNECT_TIMEOUT_SEC = 30
MINIO_READ_TIMEOUT_SEC = 60
MINIO_WRITE_TIMEOUT_SEC = 120
MINIO_MAX_RETRIES = 3
MINIO_RETRY_BACKOFF_BASE = 2.0

# === CHECKSUM SETTINGS ===
USE_SHA256 = True                       # For verification
VERIFY_ON_UPLOAD = True
VERIFY_ON_READ = False                  # Too slow for streaming
```

---

## 10. COMMON PITFALLS & SOLUTIONS

| Pitfall                | Cause                            | Solution                                                              |
| ---------------------- | -------------------------------- | --------------------------------------------------------------------- |
| **Small File Problem** | Writing 1 file per API request   | Aggregate 100-200 MB batches, then write once per 5 min               |
| **Memory Overflow**    | Loading entire 20M rows into RAM | Use streaming reads (50K row batches), chunk Parquet                  |
| **Duplicate Data**     | Late arrivals not deduplicated   | Maintain 1-hour overlap, check (device_id, metric_time) before insert |
| **Missing Data**       | Clock skew between services      | Use UTC timestamps, add 5-min overlap window                          |
| **Corrupted Parquet**  | Partial write on crash           | Use temp → rename pattern, verify checksums                           |
| **Slow Reads**         | No column projection             | Use Parquet column selection, push down filters to reader             |
| **Slow Cleanup**       | Deleting 1 file at a time        | Batch delete, use prefix matching                                     |
| **Failed Compaction**  | No rollback on failure           | Leave temp files, manual cleanup script                               |
| **Storage Bloat**      | Old partitions not deleted       | Automated cleanup with 0.5-day safety margin                          |
| **API Latency Spike**  | Cache miss + delta merge         | Prioritize cache hits (99%+ should hit cache)                         |

---

## 11. IMPLEMENTATION ROADMAP

### Phase 1: Setup (Week 1)

- [ ] Create MinIO bucket and folder structure
- [ ] Define Parquet schema (48 fields)
- [ ] Create TSDB delta query (10-minute window)
- [ ] Build safe write pipeline

### Phase 2: Background Job (Week 2)

- [ ] Implement 5-minute sync job
- [ ] Fetch delta from TSDB
- [ ] Aggregate into 150 MB files
- [ ] Upload with verification
- [ ] Handle deduplication

### Phase 3: API Integration (Week 2)

- [ ] Update API to read from MinIO
- [ ] Implement partition pruning (7-day window)
- [ ] Stream Parquet to Kafka
- [ ] Add optional fresh delta merge

### Phase 4: Maintenance (Week 3)

- [ ] Implement 7-day cleanup job
- [ ] Add hourly compaction
- [ ] Create monitoring/alerts
- [ ] Write recovery procedures

### Phase 5: Testing & Tuning (Week 3-4)

- [ ] Load test with 500+ concurrent requests
- [ ] Verify fault tolerance
- [ ] Tune file size / batch size / intervals
- [ ] Performance benchmark: Target < 5 seconds

---

## 12. EXPECTED PERFORMANCE IMPROVEMENTS

| Metric                  | Before (TSDB only) | After (MinIO Cache) | Improvement         |
| ----------------------- | ------------------ | ------------------- | ------------------- |
| Single request latency  | 110s               | 2-5s                | **20-50x faster**   |
| 100 concurrent requests | ~50s per request   | ~5s per request     | **10x faster**      |
| 500 concurrent requests | ~75s per request   | ~5s per request     | **15x faster**      |
| TSDB load per request   | Full 7-day scan    | Only 5-min delta    | **99.7% reduction** |
| Memory per request      | ~5 GB (20M rows)   | ~50 MB (batched)    | **100x less**       |

---

## Next Steps

1. **Clarify**: Any questions on architecture or implementation?
2. **Implement**: Start with Phase 1 (MinIO setup + schema)
3. **Test**: Benchmark delta sync vs full scan
4. **Deploy**: Gradually roll out, keep TSDB as fallback initially
