# ATLAS Lakehouse — Kubernetes Manifests

Production-oriented manifests converted from `docker-compose.yml` for local Minikube testing.

## Directory layout

```
k8s/
├── README.md
├── namespace.yaml
├── kustomization.yaml
├── 01-storage/
│   ├── storage-class.yaml          # Shared RWX storage class (Minikube hostPath)
│   ├── pv-delta-refined.yaml       # HostPath PV for cross-pod Delta Lake access
│   ├── pvc-delta-refined.yaml
│   ├── pvc-spark-checkpoint.yaml
│   ├── configmap-lakehouse.yaml
│   ├── statefulset-lakehouse.yaml
│   └── service-lakehouse.yaml
├── 02-queue/
│   ├── configmap-kafka.yaml
│   ├── service-kafka-headless.yaml # Headless — KRaft peer / pod DNS discovery
│   ├── service-kafka.yaml          # ClusterIP — client bootstrap
│   ├── statefulset-kafka.yaml
│   └── job-kafka-init.yaml
├── 03-processing/
│   ├── configmap-spark.yaml
│   ├── pvc-spark-checkpoint.yaml   # Processor-owned checkpoint (RWO)
│   ├── deployment-spark-processor.yaml
│   └── service-spark-ui.yaml       # Optional Spark UI (port 4040)
└── 04-frontend/
    ├── configmap-ingestion.yaml
    ├── secret-atlas.yaml
    ├── deployment-fastapi.yaml
    ├── service-fastapi.yaml
    ├── deployment-streamlit.yaml
    └── service-streamlit.yaml
```

> **Note on Zookeeper:** The Docker Compose stack runs Kafka in **KRaft mode** (no Zookeeper).
> KRaft replaces Zookeeper for controller quorum. Only the Kafka headless Service is required
> for broker peer discovery. If you migrate to classic Zookeeper-based Kafka, add a
> `statefulset-zookeeper.yaml` + headless Service under `02-queue/`.

## Quick start (Minikube)

```bash
# 1. Start cluster with enough resources
minikube start --cpus=6 --memory=16384 --disk-size=50g

# 2. Build images inside Minikube's Docker daemon
eval $(minikube docker-env)   # Linux/macOS
# minikube -p minikube docker-env | Invoke-Expression   # PowerShell

docker build -t atlas-ingestion:latest -f ingestion/Dockerfile.v2.allinone ingestion/
docker build -t atlas-processor:latest -f processing/docker/Dockerfile processing/
docker build -t atlas-lakehouse:latest -f delta_lake/Dockerfile delta_lake/
docker build -t atlas-streamlit:latest -f storage/Dockerfile storage/

# 3. Prepare hostPath directory for shared Delta Lake volume
minikube ssh -- sudo mkdir -p /data/atlas/delta-refined && sudo chmod 777 /data/atlas/delta-refined

# 4. Apply manifests (order matters on first install)
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/01-storage/
kubectl apply -f k8s/02-queue/
kubectl wait --for=condition=complete job/atlas-kafka-init -n atlas --timeout=300s
kubectl apply -f k8s/03-processing/
kubectl apply -f k8s/04-frontend/

# Or apply everything via Kustomize
kubectl apply -k k8s/

# 5. Port-forward for local access
kubectl port-forward -n atlas svc/atlas-fastapi 8080:80
kubectl port-forward -n atlas svc/atlas-streamlit 8501:8501
```

## Internal DNS cheat sheet

| Client            | Target                         | Bootstrap / URL                                      |
|-------------------|--------------------------------|------------------------------------------------------|
| FastAPI           | Kafka                          | `atlas-kafka.atlas.svc.cluster.local:9092`           |
| Spark Processor   | Kafka                          | `atlas-kafka.atlas.svc.cluster.local:9092`           |
| Spark Processor   | Delta Lake (write)             | `/refined` (shared PVC)                              |
| Lakehouse         | Delta Lake (read/write)        | `/refined` (shared PVC)                              |
| Streamlit         | ClickHouse / Postgres          | Configure via env (see `04-frontend/`)               |
