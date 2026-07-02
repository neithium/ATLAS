# ATLAS Spark Processor - Integration Guide
**Author:** Sanjula S

## Overview

The ATLAS Spark Processing Engine acts as the central processing layer of the platform, connecting the ingestion service with the downstream Lakehouse. It consumes raw telemetry from Apache Kafka, performs validation and aggregation, and generates analytics-ready Parquet datasets for further refinement.

---

# Integration Flow

```
┌──────────────────┐
│ Ingestion Service│
└─────────┬────────┘
          │
          ▼
     Apache Kafka
          │
          ▼
┌──────────────────┐
│ Spark Processor  │
└─────────┬────────┘
          │
          ▼
 Shared Parquet Volume
          │
          ▼
┌──────────────────┐
│ Delta Lakehouse  │
└─────────┬────────┘
          │
          ▼
┌──────────────────┐
│ Analytics Layer  │
└──────────────────┘
```

---

# Integration Points

### Ingestion → Spark

- Kafka acts as the communication layer between services.
- Spark continuously consumes telemetry from Kafka topics using Structured Streaming.
- A common telemetry schema is maintained across both services.

### Spark → Lakehouse

- Processed stream and batch outputs are written as Parquet files to a shared Docker volume.
- The Lakehouse continuously ingests these files for Delta MERGE and refinement.

---

# Integration Challenges

### Schema Consistency

During development, changes to the ingestion payload required corresponding updates to the Spark schema and validation logic to ensure compatibility across the pipeline.

---

### Service Startup Dependencies

Spark occasionally initialized before Kafka was ready, resulting in connection failures during container startup.
A startup synchronization mechanism was introduced to wait for Kafka availability before launching Spark workers.

---

### Shared Data Format

To maintain compatibility with the Lakehouse, the processor standardized its output as Snappy-compressed Parquet with a consistent schema for both streaming and batch pipelines.

---

### Cross-Service Coordination

Since the ingestion, processing, and Lakehouse modules were developed independently, frequent interface validation and testing were required to ensure seamless communication between services.

---

# Summary

The Spark Processing Engine serves as the integration layer between data ingestion and long-term storage. By standardizing schemas, coordinating service startup, and using shared storage for downstream processing, the processor enables reliable communication between all components of the ATLAS platform while remaining loosely coupled and independently deployable.