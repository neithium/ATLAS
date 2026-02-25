# TVMJNS — Environment Setup Guide

This document covers the dependencies, Docker infrastructure, and troubleshooting notes for the **Distributed Real-Time Data Streaming Platform**.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Windows Host (Python 3.13)                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │ Telemetry       │  │ Spark Submit    │  │ Analytics/                  │  │
│  │ Producer        │  │ (PySpark)       │  │ Forecasting                 │  │
│  └────────┬────────┘  └────────┬────────┘  └──────────────┬──────────────┘  │
│           │                    │                          │                 │
└───────────┼────────────────────┼──────────────────────────┼─────────────────┘
            │ :9092              │ :7077                    │ :5432
┌───────────┼────────────────────┼──────────────────────────┼─────────────────┐
│           ▼                    ▼                          ▼                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │     Kafka       │  │  Spark Master   │  │        PostgreSQL           │  │
│  │   (cp-kafka)    │  │ (apache/spark)  │  │       (postgres:16)         │  │
│  └────────┬────────┘  └────────┬────────┘  └─────────────────────────────┘  │
│           │                    │                                            │
│           ▼                    ▼                                            │
│  ┌─────────────────┐  ┌─────────────────┐                                   │
│  │   Zookeeper     │  │  Spark Worker   │       Docker Network:             │
│  │ (cp-zookeeper)  │  │ (apache/spark)  │         streaming-net             │
│  └─────────────────┘  └─────────────────┘                                   │
│                                                                             │
│                         Docker Compose (Linux containers)                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Docker Infrastructure

### Services & Ports

| Service | Image | Localhost Port | Purpose |
|---------|-------|----------------|---------|
| **Zookeeper** | `confluentinc/cp-zookeeper:7.6.1` | `2181` | Kafka coordination & leader election |
| **Kafka** | `confluentinc/cp-kafka:7.6.1` | `9092` (external), `29092` (internal) | Message broker for data ingestion |
| **Spark Master** | `apache/spark:3.5.1` | `7077` (RPC), `8080` (UI), `4040` (App UI) | Cluster manager |
| **Spark Worker** | `apache/spark:3.5.1` | `8081` (UI) | Executes PySpark jobs |
| **PostgreSQL** | `postgres:16` | `5432` | Stores processed results & alerts |

### Quick Start

```powershell
# Start all services
docker compose up -d

# Verify health
docker compose ps

# View logs
docker logs kafka --tail 50
docker logs spark-master --tail 50

# Stop everything
docker compose down
```

### Web UIs

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **Spark Application UI**: http://localhost:4040 (when a job is running)

---

## 2. Python Dependencies

### requirements.txt

```
kafka-python-ng==2.2.2      # Kafka producer/consumer (pure Python)
pyspark==3.5.1              # Spark structured streaming
psycopg[binary]==3.2.4      # PostgreSQL adapter (psycopg3)
python-dotenv==1.0.1        # Environment variable loader
```

### Installation

```powershell
# Create virtual environment
python -m venv .venv

# Activate
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

---

## 3. Troubleshooting — Issues Encountered

### Issue 1: `bitnami/postgresql:16.3.0` — manifest unknown

**Error:**
```
Error response from daemon: manifest for bitnami/postgresql:16.3.0 not found: manifest unknown
```

**Cause:** Bitnami uses non-standard versioning and frequently delists old tags.

**Solution:** Switched to the official `postgres:16` image which is always available and stable.

---

### Issue 2: `apache/spark-py:v3.5.3` — manifest unknown

**Error:**
```
no such manifest: docker.io/apache/spark-py:v3.5.3
```

**Cause:** The `apache/spark-py` image doesn't exist with that tag. Apache publishes Spark images as `apache/spark:<version>` (not `spark-py`).

**Solution:** Changed to `apache/spark:3.5.1` which includes Python support.

---

### Issue 3: Zookeeper healthcheck failing (unhealthy status)

**Error:**
```
zookeeper: Up 4 minutes (unhealthy)
```

**Cause:** The healthcheck used `echo ruok | nc localhost 2181`, but modern ZooKeeper versions disable four-letter commands by default for security.

**Logs showed:**
```
The list of enabled four letter word commands is: [[srvr]]
```

**Solution:** Changed healthcheck to use the always-enabled AdminServer HTTP endpoint:

```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -sf http://localhost:8080/commands/ruok || exit 1"]
```

---

### Issue 4: `confluent-kafka` build failure on Python 3.13

**Error:**
```
fatal error C1083: Cannot open include file: 'librdkafka/rdkafka.h': No such file or directory
```

**Cause:** `confluent-kafka` requires native C extension compilation. No pre-built wheels exist for Python 3.13 on Windows, and building from source requires `librdkafka` headers + C compiler.

**Solution:** Switched to `kafka-python-ng==2.2.2`, a pure-Python Kafka client that works on Python 3.13 without compilation.

---

### Issue 5: `psycopg2-binary` build failure on Python 3.13

**Error:**
```
Building wheel for psycopg2-binary failed
```

**Cause:** `psycopg2-binary` doesn't have pre-built wheels for Python 3.13 on Windows.

**Solution:** Switched to `psycopg[binary]==3.2.4` (psycopg3), which has Python 3.13 wheels.

---

### Issue 6: Docker Compose `version` attribute warning

**Warning:**
```
the attribute `version` is obsolete, it will be ignored, please remove it
```

**Cause:** Docker Compose V2 no longer requires the `version` field.

**Solution:** Removed `version: "3.9"` from `docker-compose.yml`.

---

### Issue 7: Docker pull timeout / network errors

**Error:**
```
unable to decode token response: context deadline exceeded
```

**Cause:** Slow network connection to Docker Hub causing timeouts during parallel image pulls.

**Solution:** Pull images individually with retries:

```powershell
docker pull confluentinc/cp-zookeeper:7.6.1
docker pull confluentinc/cp-kafka:7.6.1
docker pull apache/spark:3.5.1
docker pull postgres:16
```

Docker caches partially downloaded layers, so retries resume from where they left off.

---

## 4. Key Configuration Explained

### KAFKA_ADVERTISED_LISTENERS (Critical)

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092
```

This tells Kafka **what address to return to clients** after initial connection:

| Listener | Returned To | Address |
|----------|-------------|---------|
| `PLAINTEXT` | Other containers (Spark, etc.) | `kafka:29092` (Docker DNS) |
| `EXTERNAL` | Your Windows Python scripts | `localhost:9092` (port-mapped) |

**Why two listeners?**
- Containers resolve `kafka` via Docker's internal DNS
- Your local scripts can't resolve `kafka` — they need `localhost`

Without this dual-listener setup, you get "broker not available" errors.

---

### KAFKA_LISTENER_SECURITY_PROTOCOL_MAP

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
```

Maps each named listener to a wire protocol. Both use `PLAINTEXT` (no TLS) for local development.

---

### KAFKA_INTER_BROKER_LISTENER_NAME

```yaml
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

Tells Kafka which listener brokers use to talk to **each other**. Must be the internal one (`PLAINTEXT`), not `EXTERNAL`.

---

## 5. Sample Code

### Kafka Producer (Python)

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send telemetry
producer.send('telemetry', {'sensor_id': 'S001', 'value': 42.5, 'ts': 1234567890})
producer.flush()
```

### Kafka Consumer (Python)

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'telemetry',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

for message in consumer:
    print(f"Received: {message.value}")
```

### PostgreSQL Connection (Python)

```python
import psycopg

with psycopg.connect(
    host='localhost',
    port=5432,
    user='streaming_user',
    password='streaming_pass',
    dbname='streaming_db'
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(cur.fetchone())
```

---

## 6. File Structure

```
TVMJNS/
├── docker-compose.yml      # Infrastructure definition
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables
├── docs/
│   └── SETUP_GUIDE.md      # This document
└── .venv/                  # Python virtual environment
```

---

## 7. Credentials (Development Only)

| Service | Username | Password | Database |
|---------|----------|----------|----------|
| PostgreSQL | `streaming_user` | `streaming_pass` | `streaming_db` |

⚠️ **Change these in production!**
