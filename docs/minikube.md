# ATLAS on Minikube (Windows PowerShell)

Run every command from the **repository root**:

```powershell
cd C:\Users\manth\Documents\GitHub\ATLAS
```

---

## 0. Reset Docker (do this first if anything failed)

`minikube docker-env` sets `DOCKER_HOST`, which **overrides** `docker context` and breaks `docker tag` / `minikube image load`.

Symptom: `DOCKER_HOST environment variable overrides the active context` or connection refused on `127.0.0.1:55531`.

**Open a fresh PowerShell window**, or run:

```powershell
Remove-Item Env:DOCKER_HOST -ErrorAction SilentlyContinue
Remove-Item Env:DOCKER_CERT_PATH -ErrorAction SilentlyContinue
Remove-Item Env:DOCKER_TLS_VERIFY -ErrorAction SilentlyContinue
Remove-Item Env:MINIKUBE_ACTIVE_DOCKERD -ErrorAction SilentlyContinue

docker context use desktop-linux
docker images | Select-String atlas-atlas    # must show your images
```

> **Never run** `minikube docker-env | Invoke-Expression` when using `minikube image load`.

---

## 1. Start Minikube (memory)

Minikube runs **inside** Docker Desktop. `--memory` must be **less than** Docker Desktop's RAM (you had ~9945 MB).

```powershell
minikube delete   # only if changing memory/CPU
minikube start --cpus=4 --memory=7900 --disk-size=50g
```

Do **not** run plain `minikube start` (defaults to ~3900MB / 2 CPUs). Kafka and other pods will stay `Pending` with `Insufficient memory` because base manifests request ~10.5 Gi total. Deploy uses `k8s/overlays/minikube` to lower requests.

---

## 2. Load images (no rebuild — re-run after every `minikube delete`)

> **Important:** `minikube delete` wipes all loaded images. You must reload after recreating the cluster.

> **Don't trust** `minikube ssh -- docker images` alone — use **`minikube image ls`** to verify what Kubernetes can see.

Your compose images:

| Compose (source) | K8s (target) |
|------------------|--------------|
| `atlas-atlas-ingestion:latest` | `atlas-ingestion:latest` |
| `atlas-atlas-processor:latest` | `atlas-processor:latest` |
| `atlas-atlas-lakehouse:latest` | `atlas-lakehouse:latest` |
| `atlas-atlas-analytics:latest` | `atlas-analytics:latest` |

**One-shot scripts** (recommended):

```powershell
cd C:\Users\manth\Documents\GitHub\ATLAS
.\scripts\minikube-load-images.ps1   # loads 6 images via docker save (reliable)
minikube image ls | Select-String "atlas|kafka|busybox"
.\scripts\minikube-deploy.ps1    # uses k8s/overlays/minikube (lower CPU/RAM requests)
```

Confirm scheduling:

```powershell
kubectl describe pod -n atlas atlas-kafka-0 | Select-String -Pattern "Events:" -Context 0,8
kubectl get pods -n atlas
```

**Or manually** (after section 0 reset):

```powershell
docker tag atlas-atlas-ingestion:latest  atlas-ingestion:latest
docker tag atlas-atlas-processor:latest  atlas-processor:latest
docker tag atlas-atlas-lakehouse:latest  atlas-lakehouse:latest
docker tag atlas-atlas-analytics:latest  atlas-analytics:latest

minikube image load atlas-ingestion:latest
minikube image load atlas-processor:latest
minikube image load atlas-lakehouse:latest
minikube image load atlas-analytics:latest
minikube image load soldevelo/kafka:4.0
```

---

## Compose vs Kubernetes — what you need

Docker Compose runs **more** services than the k8s manifests. You only need **5 images** for k8s:

| Docker Compose service | K8s resource | Image needed |
|------------------------|--------------|--------------|
| `atlas-ingestion` | Deployment `atlas-fastapi` | `atlas-ingestion:latest` |
| `broker1` | StatefulSet `atlas-kafka` | `soldevelo/kafka:4.0` |
| `kafka-init` | Job `atlas-kafka-init` | (same kafka image) |
| `atlas-processor` | Deployment `atlas-processor` | `atlas-processor:latest` |
| `atlas-lakehouse` | StatefulSet `atlas-lakehouse` | `atlas-lakehouse:latest` |
| `atlas-analytics` | Deployment `atlas-streamlit` | `atlas-analytics:latest` |

**Not required for k8s** (optional compose profiles / extras):

- `broker2`, `broker3` — HA cluster profile only
- `atlas-streaming`, `atlas-data-producer` — `streaming` / `producer` profiles
- `grafana`, `kafka-ui` — monitoring extras

Required images (6 total — includes init-container `busybox`):

- `atlas-ingestion`, `atlas-processor`, `atlas-lakehouse`, `atlas-analytics`
- `soldevelo/kafka:4.0`
- `busybox:1.36`

---

## 3. Deploy

```powershell
minikube ssh -- sudo mkdir -p /data/atlas/delta-refined
minikube ssh -- sudo chmod 777 /data/atlas/delta-refined

kubectl apply -k k8s/
kubectl wait --for=condition=complete job/atlas-kafka-init -n atlas --timeout=300s
kubectl get pods -n atlas
```

---

## 4. Access services (two terminals)

```powershell
kubectl port-forward -n atlas svc/atlas-fastapi 8080:80
```

```powershell
kubectl port-forward -n atlas svc/atlas-streamlit 8501:8501
```

- FastAPI: http://localhost:8080
- Streamlit: http://localhost:8501

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `DOCKER_HOST overrides the active context` | Run section **0** — unset env vars before any `docker` command |
| `127.0.0.1:55531 connection refused` | Same — stale `DOCKER_HOST` from old minikube docker-env |
| `No such image: atlas-atlas-ingestion` | `DOCKER_HOST` still set, or image not built — run `docker compose build` |
| `Docker Desktop has only XMB memory` | `minikube start --memory=7900` |
| `You cannot change the memory size` | `minikube delete` then start again |
| `ImagePullBackOff` | Re-run `.\scripts\minikube-load-images.ps1` |
| `namespaces "atlas" not found` | `kubectl apply -k k8s/` from repo root |
