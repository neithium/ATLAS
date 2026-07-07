# ATLAS Kafka Event Streaming Layer

> **Ownership**: Knsrikanta (Streaming & Orchestration sub-team)

This module manages the distributed message queue and event streaming infrastructure for the ATLAS telemetry platform. It provides a multi-broker ZooKeeper-less Apache Kafka (KRaft) cluster designed for high availability, fault tolerance, and scalable event routing.

---

## Architecture Overview

The Kafka streaming layer buffers high-velocity telemetry events, isolates validation anomalies, and enables reliable, parallel message consumption downstream.

```text
[Ingestion API] ──► [raw-server-metrics (12 partitions)] ──► [Spark Streaming]
                             │
                             ▼
                 [raw-server-metrics-dlq] ──► [DLQ Reviewer]
```

---

## Technical Specifications

| Parameter | Configuration | Purpose / Detail |
| :--- | :--- | :--- |
| **Broker Engine** | Apache Kafka 4.0 (KRaft) | ZooKeeper-less metadata management and quorum setup |
| **Cluster Identifier** | `atlas-telemetry-cluster-3node` | Unique identifier for cluster member discovery |
| **Ingestion Partitioning** | 12 partitions | Parallel write distribution and concurrent Spark scaling |
| **Replication Factor** | `RF=3` | Ensures 3 copies of every message across the cluster |
| **Minimum ISR** | `MIN_ISR=2` | Minimum in-sync replicas needed to accept writes safely |
| **Producer Compression** | `LZ4` | High-speed compression for minimal network payload overhead |

---

## Deployment Modes

### 1. Single Broker Developer Mode (Dev)
Used for local prototyping and fast verification loops.
* **Command**: `.\single.bat` (wraps `kafka/scripts/single.bat`)
* **Topologies**: Runs a single container `broker1` with `RF=1` and `MIN_ISR=1`.

### 2. Multi-Broker High Availability Mode (HA)
Used for reliability demonstrations, failover testing, and load testing.
* **Command**: `.\cluster.bat` (wraps `kafka/scripts/cluster.bat`)
* **Topologies**: Deploys `broker1`, `broker2`, and `broker3` across distinct ports, enforcing `RF=3` and `MIN_ISR=2`.

---

## Topic Topology

The cluster establishes four distinct topics to manage raw streams, retries, and schema failures:

| Topic Name | Partitions | Description |
| :--- | :---: | :--- |
| `raw-server-metrics` | 12 | Primary telemetry stream containing golden schema payloads. |
| `raw-server-metrics-dlq` | 3 | Dead Letter Queue capturing schema violations and corruptions. |
| `raw-server-metrics-retry` | 3 | Target topic for recovered messages dispatched from the DLQ Reviewer. |
| `raw-server-metrics-failure` | 3 | Quarantine topic for permanently unrecoverable records. |

---

## Operational Scripts Reference

All management and validation utilities are located under `kafka/scripts/`:

* **`single.bat`**: Wipes local storage volumes and starts dev mode.
* **`cluster.bat`**: Wipes local storage volumes and spins up the 3-node KRaft cluster.
* **`stream_data.bat`**: Generates and exports a continuous stream of telemetry records to the cluster.
* **`failover_test.bat`**: Terminates the primary leader broker to test cluster self-healing and replication failover.
* **`watchdog.bat`**: Actively monitors broker health, restarting failed nodes to keep the ISR quorum complete.
* **`check_kafka.py`**: Python smoke test that verifies basic write/read capability.

---

## Troubleshooting

### KRaft Metadata Quorum Mismatch
* **Symptom**: Brokers refuse to form a cluster or exit with meta-mismatch logs.
* **Cause**: Switching between Single and HA modes leaves legacy metadata in Docker volumes.
* **Resolution**: Force-recreate the volumes:
  ```powershell
  docker compose --profile full-cluster down
  docker volume rm atlas_kafka-data-1 atlas_kafka-data-2 atlas_kafka-data-3
  .\cluster.bat
  ```
