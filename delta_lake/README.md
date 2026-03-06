# Delta Lake - ATLAS Deduplication Module

This module handles **ACID-compliant deduplication** for the ATLAS telemetry pipeline using Delta Lake MERGE operations.

## Overview

The Delta Lake module is responsible for:
- **Delta Lake MERGE** (upsert) operations with composite primary keys
- **Deduplication** of overlapping time-series telemetry data from the processing layer
- **ACID transactions** ensuring data consistency during concurrent writes

## Integration with ATLAS Pipeline

This module sits between the **Processing Layer (Sanjula)** and the downstream **ClickHouse/PostgreSQL** storage:

```
Spark Processing (Sanjula)  →  Delta Lake MERGE  →  ClickHouse/PostgreSQL
     (explode & window)         (deduplication)       (analytics)
```

## Module Structure

```
delta_lake/
├── delta_merge_pipeline.py   # Main deduplication pipeline
├── generate_data.py          # Test data generator (for local testing)
├── docker-compose.yml        # Container setup for isolated testing
├── Dockerfile                # Spark + Delta Lake environment
├── data/
│   ├── raw/                  # Input parquet files (flattened)
│   └── refined/              # Delta Lake output with _delta_log/
└── jobs/                     # Scheduled job definitions
```

## The Deduplication Logic

Using a composite primary key `(device_id, metric_time)`, the MERGE operation:
1. Matches incoming records against existing Delta table
2. Inserts only non-matching (new) records
3. Drops duplicates from overlapping time windows

**Example scenario:**
| Input | Device | Readings | Time Coverage |
|-------|--------|----------|---------------|
| Batch 1 (Baseline) | SRV-101 | 2016 | Days 1-7 |
| Batch 2 (Overlap) | SRV-101 | 2016 | Days 2-8 |

- **Overlap**: 1728 readings share timestamps → deduplicated
- **New data**: 288 readings (Day 8) → inserted
- **Result**: 2016 + 288 = **2304 unique rows**

## Local Testing

```bash
# Run with Docker (isolated Spark environment)
docker-compose up --build

# Or run directly with local PySpark
python generate_data.py
python delta_merge_pipeline.py
```

## Data Files

After execution, Delta Lake files are stored in `data/refined/`:
- `_delta_log/` - Transaction log (JSON)
- `*.parquet` - Data files (Snappy compressed)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              ATLAS Processing Layer (Sanjula)                   │
├─────────────────────────────────────────────────────────────────┤
│  Flattened telemetry data from Spark (one row per reading)     │
│  Schema: ../schema/output_schema.py                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DELTA LAKE MERGE                            │
├─────────────────────────────────────────────────────────────────┤
│  Composite Key: (device_id, metric_time)                        │
│                                                                 │
│  MERGE INTO refined AS target                                   │
│  USING incoming_data AS source                                  │
│  ON target.device_id = source.device_id                         │
│     AND target.metric_time = source.metric_time                 │
│  WHEN NOT MATCHED THEN INSERT *                                 │
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
docker-compose down

# Remove generated test data
rm -rf ./data/raw/* ./data/refined/*
```
