# ATLAS Spark Standalone Cluster Architecture

This document outlines the architecture, deployment process, and remote worker integration for the ATLAS Spark Standalone Cluster.

## 1. Architecture Overview

The ATLAS Spark pipeline leverages a **Standalone Cluster** model to distribute high-intensity PySpark processing and Delta Lake deduplication workloads across multiple nodes.

### Core Components
- **Spark Master (`atlas-spark-master`)**: The orchestrator node. It manages cluster resources, schedules workloads, and tracks the health of all registered workers.
- **Spark Worker (`atlas-spark-worker-local`)**: The executor node. It connects to the master and provides CPU/RAM for executing Spark tasks. By default, one local worker runs alongside the master.
- **Shared Storage Volumes**: 
  - `/raw` (`raw-volume`): Stores the generated and incoming streaming/batch telemetry data.
  - `/refined` (`refined-volume`): Stores the heavily partitioned and deduplicated Delta Lake tables.
  - *Note:* Both Master and Worker nodes must have identical access to these volumes.

### Ports and Communication
- **`7077`**: The primary RPC internal communication port. Workers connect to the Master through this port.
- **`8080`**: The Spark Master Web UI. (View active workers and running applications).
- **`8081`**: The Spark Worker Web UI. (View executor logs and resource usage).

---

## 2. Booting the Local Cluster (Automated)

The most reliable way to spin up the cluster and execute workloads is using the unified PowerShell automation script. This script handles container provisioning, volume permission fixes (UID `1001`), and Java dependency caching.

1. Open PowerShell and navigate to the project root.
2. Execute the cluster orchestrator:
   ```powershell
   .\Run-ClusterBenchmark.ps1
   ```
   
**What this script does behind the scenes:**
1. Brings up the Master and Local Worker containers (`docker-compose --profile cluster-poc up -d`).
2. Waits 15 seconds for network stabilization and node registration.
3. Modifies the Linux permissions of `/raw`, `/refined`, and `/tmp/.ivy2` so the `spark` execution user (UID 1001) has full filesystem write capabilities.
4. Pre-installs `delta-spark==3.1.0`.
5. Executes the primary benchmark payload (`run_benchmark.py`).

To shut down the cluster and clean up resources:
```powershell
docker-compose --profile cluster-poc down
```

---

## 3. Adding Remote Physical Workers (Horizontal Scaling)

Because the `atlas-spark-master` exposes the RPC port `7077` to your host machine, you can expand the cluster horizontally by attaching remote physical servers natively over your Local Area Network (LAN).

### Prerequisites for Remote Workers
1. **Network Visibility**: The remote worker must be able to ping your Master machine's IP address (e.g., `192.168.1.15`).
2. **Shared Storage (Crucial)**: Spark requires all workers to have symmetrical access to identical files. The remote machine **must** have the `/raw` and `/refined` paths mapped to the *exact same underlying data* (e.g., via an SMB/NFS network share, or using a cloud-native store like MinIO/S3). If a remote worker has empty local `/raw` and `/refined` directories, tasks assigned to it will fail.
3. **Docker**: Installed on the remote machine.

### Integration Steps

To add an external Linux or Windows machine to your ATLAS Spark Cluster, run the following Docker command on the **remote machine**:

```bash
docker run -d --name atlas-remote-worker \
  --net host \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://<YOUR_MASTER_MACHINE_IP>:7077 \
  -e SPARK_WORKER_MEMORY=8g \
  -e SPARK_WORKER_CORES=4 \
  -v /path/to/network/share/raw:/raw \
  -v /path/to/network/share/refined:/refined \
  bitnamilegacy/spark:3.5
```

*Make sure to replace `<YOUR_MASTER_MACHINE_IP>` with the IP address of the machine running the `atlas-spark-master`.*

### Verification
Once the remote worker container starts:
1. Open your browser and navigate to the Spark Master UI: `http://<YOUR_MASTER_MACHINE_IP>:8080`.
2. Look under the **"Workers"** grid. You will see both your Local Worker and the newly attached Remote Worker actively supplying their CPU Cores and RAM to the pool. Next time a PySpark task is submitted, the Master will seamlessly shard the data processing across both nodes.