# ATLAS Spark Processor - Integration Notes
**Author:** Sanjula S

# End-to-End Integration

```text
                 Ingestion Service
                        │
                        ▼
                 Apache Kafka
                        │
                        ▼
              Spark Processing Engine
         ┌──────────────┼──────────────┐
         │              │              │
         ▼              ▼              ▼
 Streaming        Batch Processing    DLQ
         │              │              │
         └──────────────┼──────────────┘
                        ▼
             Shared Parquet Volume
                        │
                        ▼
                Delta Lakehouse
                        │
                        ▼
                 Analytics Layer
```

---

# Integration Points

### Ingestion → Spark

The ingestion service publishes production-like telemetry to Kafka using a common schema. Spark continuously consumes these messages using Structured Streaming, ensuring both services remain loosely coupled.

### Spark → DLQ

Invalid records are redirected to a dedicated Dead Letter Queue instead of interrupting processing. Recoverable records are repaired and replayed through a retry topic, while permanent failures are isolated for later inspection.

### Spark → Lakehouse

Streaming and batch outputs are written as Snappy-compressed Parquet files to a shared Docker volume. The Lakehouse continuously ingests these files for refinement and Delta MERGE operations.

---

# Integration Challenges

### Schema Synchronization

As the ingestion and processing services evolved together, maintaining a consistent telemetry schema required continuous validation across both services.

### Service Startup Dependencies

Spark depends on Kafka availability during startup. A startup synchronization mechanism was introduced to ensure Kafka became available before Spark workers and the DLQ service were initialized.

### Shared Storage Compatibility

A standardized Parquet schema and Snappy compression were adopted to ensure compatibility between the Spark processor and the downstream Lakehouse.

### Cross-Service Validation

End-to-end integration testing was performed throughout development to verify communication between ingestion, Kafka, Spark, Lakehouse, and analytics services under both streaming and batch workloads.

---

# Conclusion

The Spark Processing Engine successfully integrates with the complete ATLAS ecosystem by combining event-driven communication through Kafka with shared-volume data exchange for downstream processing. This modular integration approach improves scalability, maintainability, and fault tolerance while enabling reliable telemetry processing across the platform.