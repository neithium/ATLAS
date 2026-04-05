# Delta Lake - ATLAS Deduplication Module

This module handles **ACID-compliant deduplication** for the ATLAS telemetry pipeline using Delta Lake MERGE operations for **batch** processing.

## Overview

The Delta Lake module is responsible for:
- **Delta Lake MERGE** (upsert) operations with Triple-Hash composite primary keys
- **Batch deduplication** of overlapping 7-day rolling window telemetry data
- **ACID transactions** ensuring data consistency during concurrent writes
- **Storage optimization** with Zstd compression and automatic VACUUM

## Key Features

| Feature | Description |
|---------|-------------|
| **Triple-Hash Key** | `(device_id, metric_time, application_customer_id)` for multi-tenant uniqueness |
| **5-Level Partitioning** | `report_type/partition_date/platform_customer_id/application_customer_id/device_id` |
| **Zstd Compression** | ~30% better compression ratio than Snappy |
| **14-Day Vacuum** | Automatic cleanup of old Delta log files |
| **Horizontal Scaling** | Dynamic allocation support for auto-scaling executors |


## Integration with ATLAS Pipeline

This module sits between the **Processing Layer** and the downstream **ClickHouse/PostgreSQL** storage:

```
                    ┌──────────────────────────────┐
                    │   Kafka / API Ingestion      │
                    └──────────────────────────────┘
                              │
                              ▼
              ┌─────────────────────────────┐
              │    BATCH PROCESSING         │
              │  (delta_merge_pipeline)     │
              ├─────────────────────────────┤
              │ - File-based input          │
              │ - DataFrame API             │
              │ - Periodic OPTIMIZE         │
              └─────────────────────────────┘
                              │
                              ▼
              ┌─────────────────────────────────┐
              │      UNIFIED DELTA TABLE        │
              │   (deduplication via MERGE)     │
              └─────────────────────────────────┘
                              │
                              ▼
              ┌─────────────────────────────────┐
              │  ClickHouse / PostgreSQL        │
              │    (analytics dashboards)       │
              └─────────────────────────────────┘
```

## Module Structure

```
delta_lake/
├── delta_merge_pipeline.py      # Batch deduplication pipeline
├── generate_data.py             # Batch data generator (file & DataFrame modes)
├── docker-compose.yml           # Container setup for batch service
├── Dockerfile                   # Spark + Delta Lake environment
├── data/
│   ├── raw/                     # Input parquet files (flattened)
│   └── refined/                 # Delta Lake output with _delta_log/
├── docs/
│   └── IMPLEMENTATION_NOTES.md  # Technical decisions and lessons learned
└── jobs/                        # Scheduled job definitions
```

## The Deduplication Logic

Using a **Triple-Hash composite primary key** `(device_id, metric_time, application_customer_id)`, the MERGE operation:
1. Matches incoming records against existing Delta table
2. Inserts only non-matching (new) records
3. Drops duplicates from overlapping time windows

**Example scenario (7-day rolling window):**
| Input | Device | Readings | Time Coverage |
|-------|--------|----------|---------------|
| Day 8 File | SRV-101 | 2016 | Days 2-8 |
| Day 9 File | SRV-101 | 2016 | Days 3-9 |

- **Overlap**: 1728 readings share timestamps (6 days) → deduplicated
- **New data**: 288 readings (1 day) → inserted
- **Expected dedup ratio**: ~85.7%

## Quick Start

### Batch Processing

```bash
# Generate benchmark data and run deduplication
docker compose run -e RUN_GENERATOR=y -e RUN_PIPELINE=y -e RUN_VACUUM=y spark

# With custom settings
docker compose run \
  -e RUN_GENERATOR=y \
  -e RUN_PIPELINE=y \
  -e DEVICE_COUNT=10000 \
  -e NUM_DAYS=7 \
  -e RUN_VACUUM=y \
  -e COMPRESSION_CODEC=zstd \
  spark
```

### DataFrame API (Programmatic)

```python
from generate_data import generate_batch_dataframe
from delta_merge_pipeline import process_dataframe
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Generate batch DataFrame (instead of files)
batch_df = generate_batch_dataframe(
    spark=spark,
    total_devices=1000,
    file_date=datetime.now()
)

# Process through merge pipeline
result = process_dataframe(
    batch_df,
    output_path="/refined",
    run_optimize=True,
    run_vacuum=True
)

print(f"Processed {result['row_count']} rows")
```

## Pipeline Mode

The pipeline supports **benchmark mode** for production-scale testing with N daily files using 7-day rolling window patterns.

```bash
# Small benchmark (10 devices, 7 days)
docker compose run -e RUN_GENERATOR=y -e RUN_PIPELINE=y \
  -e DEVICE_COUNT=10 -e NUM_DAYS=7 spark

# Production scale (100K devices)  
docker compose run -e RUN_GENERATOR=y -e RUN_PIPELINE=y \
  -e DEVICE_COUNT=100000 -e NUM_DAYS=7 atlas-lakehouse
```

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPRESSION_CODEC` | `zstd` | Compression: zstd, snappy, gzip |
| `VACUUM_RETENTION_DAYS` | `14` | Days to retain Delta log files |
| `SPARK_DYNAMIC_ALLOCATION` | `false` | Enable auto-scaling executors |
| `SPARK_EXECUTOR_INSTANCES` | `2` | Number of Spark executors |
| `SPARK_MIN_EXECUTORS` | `1` | Min executors (dynamic mode) |
| `SPARK_MAX_EXECUTORS` | `8` | Max executors (dynamic mode) |


## Compression: Why Zstd?

| Codec | Compression Ratio | Speed | Use Case |
|-------|-------------------|-------|----------|
| **Zstd** | ~30% better than Snappy | Medium | Large-scale telemetry (storage-optimized) |
| Snappy | Baseline | Fast | Real-time with speed priority |
| Gzip | Best | Slow | Cold storage archives |

For 400K+ device telemetry data, **Zstd** provides the best balance:
- Significantly smaller storage footprint
- Acceptable read/write performance
- Native Delta Lake support

## Data Files

After execution, Delta Lake files are stored in `data/refined/`:
- `_delta_log/` - Transaction log (JSON)
- `*.parquet` - Data files (Zstd compressed)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              ATLAS Processing Layer                              │
├─────────────────────────────────────────────────────────────────┤
│  Flattened telemetry data from Spark (one row per reading)     │
│  Schema: ../schema/output_schema.py                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DELTA LAKE MERGE                            │
├─────────────────────────────────────────────────────────────────┤
│  Triple-Hash Key: (device_id, metric_time, application_customer_id) │
│                                                                 │
│  MERGE INTO refined AS target                                   │
│  USING incoming_data AS source                                  │
│  ON target.device_id = source.device_id                         │
│     AND target.metric_time = source.metric_time                 │
│     AND target.application_customer_id = source.application_customer_id │
│  WHEN NOT MATCHED THEN INSERT *                                 │
│                                                                 │
│  + VACUUM (14-day retention)                                    │
│  + OPTIMIZE with Z-ORDER by metric_time                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│               ClickHouse / PostgreSQL Storage                   │
├─────────────────────────────────────────────────────────────────┤
│  Deduplicated telemetry for analytics dashboards               │
└─────────────────────────────────────────────────────────────────┘
```

## Schema Reference

This module uses the project-level schemas defined in `../schema/`:
- `input_schema.py` - Nested telemetry format from API
- `output_schema.py` - Flattened format after processing

## Cleanup

```bash
# Stop containers
docker compose down

# Remove generated test data
rm -rf ./data/raw/* ./data/refined/*
```

## Related Documentation

- [Implementation Notes](docs/IMPLEMENTATION_NOTES.md) - Technical decisions and lessons learned
- [ATLAS Main README](../README.md) - Overall project documentation
