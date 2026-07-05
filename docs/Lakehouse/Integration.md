# ATLAS Data Integration Guide

This document outlines the data flow and integration between the `atlas-processor`, `atlas-lakehouse`, and `atlas-analytics` services in the ATLAS architecture.

## High-Level Overview

The data flows in a unidirectional pipeline:

1.  **`atlas-processor`**: Consumes raw data, processes it into Parquet files, and writes them to a shared volume.
2.  **`atlas-lakehouse`**: Ingests the Parquet files from the shared volume, performs deduplication and ACID MERGE operations, and stores the refined data in a Delta Lake.
3.  **`atlas-analytics`**: Reads the refined data from the Delta Lake for analytics, reporting, and dashboarding.

## Shared Volumes and Data Paths

The integration between these services is achieved through Docker-named volumes and host-mounted directories.

### 1. Processing to Delta Lake (`delta-refined` volume)

-   **`atlas-processor` (The "Producer")**:
    -   This service is responsible for processing raw telemetry data from Kafka.
    -   It runs two main jobs: `kafka_streaming.py` for real-time data and `batch_job.py` for historical data.
    -   Both jobs write their output as Parquet files to the `delta-refined` named volume.
        -   **Streaming data path**: `/app/data/processed/stream/` inside the container, which maps to the `delta-refined` volume.
        -   **Batch data path**: `/app/data/processed/batch/` inside the container, which also maps to the `delta-refined` volume.

-   **`atlas-lakehouse` (The "Consumer")**:
    -   This service's primary role is to manage the Delta Lake.
    -   It mounts the `delta-refined` volume to the `/stream_raw` directory inside its container.
    -   The `run_livewire.py` script is configured to monitor this `/stream_raw` directory for new Parquet files from both the stream and batch processing jobs.
    -   This allows the `atlas-lakehouse` to continuously ingest and merge new data into the main Delta table.

### 2. Delta Lake to Analytics (`delta-refined` volume)

-   **`atlas-lakehouse` (The "Writer")**:
    -   After ingesting and processing the data from the `delta-refined` volume, the `atlas-lakehouse` service writes the final, clean, and deduplicated data to a Delta Lake table.
    -   This Delta Lake is stored on the `delta-refined` volume, which is mapped to the `/refined` directory inside the container.

-   **`atlas-analytics` (The "Reader")**:
    -   The `atlas-analytics` service, which includes ClickHouse and PostgreSQL, needs access to this refined data for analytical queries.
    -   It mounts the same `delta-refined` volume to its `/data/refined` directory in **read-only mode**.
    -   This ensures that the analytics service can query the data without the risk of accidentally modifying the source of truth in the Delta Lake.

## Summary of Volume Mappings

| Service           | Internal Path        | Volume/Host Path                     | Purpose                                             |
| ----------------- | -------------------- | ------------------------------------ | --------------------------------------------------- |
| `atlas-processor` | `/app/data/processed`| `delta-refined` (Named Volume)       | Writes processed Parquet files.                     |
| `atlas-lakehouse` | `/stream_raw`        | `delta-refined` (Named Volume)       | Reads raw Parquet files for ingestion.              |
| `atlas-lakehouse` | `/refined`           | `delta-refined` (Named Volume)       | Writes the final, refined Delta Lake table.         |
| `atlas-analytics` | `/data/refined`      | `delta-refined` (Named Volume)       | Reads the refined Delta Lake table for analytics.   |

This architecture effectively decouples the processing, storage, and analytics layers, allowing for a scalable and maintainable system.
