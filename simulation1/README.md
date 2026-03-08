# Power Monitor API

IPMI → Redis → FastAPI pipeline that returns 7-day rolling power data per server.

## Architecture

```
Physical Server (BMC/iLO/iDRAC)
        │  IPMI over LAN (port 623)
        ▼
  core/ipmi_reader.py         ← reads AmbTemp, CpuWatts, GpuWatts, CpuUtil etc.
        │  every 5 mins (APScheduler)
        ▼
  core/redis_store.py         ← ring buffer per device (max 2016 readings, TTL 7d)
        │
        ▼
  Redis List: readings:{device_id}
  [oldest reading ... ... ... newest reading]   ← max 2016 entries
        │
        ▼
  GET /devices/{device_id}
        │
        ▼  core/response_builder.py
  JSON response (input_schema)
  ├── metadata (server_name, location, vendor ...)
  ├── data.PowerDetail[2016]
  │     ├── [0..2003]  historical  (23 hrs + 6 days)  is_fresh=false
  │     └── [2004..2015] fresh     (current 1 hour)   is_fresh=true
  └── summary (avg W, peak, energy kWh, cpu%, temp)
```

## Data composition per API response

```
Total = 2016 readings (7 days × 24 hrs × 12 readings/hr)
      = 12  fresh readings    (last hour, from IPMI via Redis)
      + 2004 historical readings (23 hrs + 6 days, from Redis)
```

## Project structure

```
power_api/
├── main.py                  ← FastAPI app + all endpoints
├── requirements.txt
├── config/
│   └── devices.py           ← server registry + Redis + poller config
└── core/
    ├── ipmi_reader.py       ← IPMI data fetcher (real + mock mode)
    ├── redis_store.py       ← Redis ring buffer operations
    ├── poller.py            ← APScheduler background poller
    └── response_builder.py  ← assembles final JSON (matches input_schema)
```

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Redis
docker run -d -p 6379:6379 redis:7

# 3. Configure devices
nano config/devices.py     # set IPMI IPs + credentials

# 4. Run in mock mode (no real IPMI needed)
MOCK_IPMI=true uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 5. Run against real BMCs
MOCK_IPMI=false uvicorn main:app --host 0.0.0.0 --port 8000
```

Swagger UI → http://localhost:8000/docs

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `MOCK_IPMI` | `true` | Use simulated data instead of real IPMI |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | _(none)_ | Redis auth password |
| `IPMI_HOST_01` | `192.168.1.11` | BMC IP for DEV-SERVER-01 |
| `IPMI_USER_01` | `admin` | IPMI username |
| `IPMI_PASS_01` | `admin` | IPMI password |

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Redis status + buffer coverage per device |
| `GET` | `/devices` | List all registered devices |
| `GET` | `/devices/{device_id}` | **Full 7-day JSON** (2016 readings) |
| `GET` | `/devices/{device_id}?from_time=&to_time=&limit=` | Filtered slice |
| `GET` | `/devices/{device_id}/fresh` | Last 12 readings (1 hour) only |
| `GET` | `/devices/{device_id}/latest` | Single most-recent reading |
| `GET` | `/devices/{device_id}/summary` | Aggregated stats, no PowerDetail |
| `POST` | `/devices/{device_id}/poll` | Force immediate IPMI poll |
| `DELETE` | `/devices/{device_id}/flush` | Clear Redis buffer for device |

## Example requests

```bash
# Full 7-day history for server 01
curl http://localhost:8000/devices/DEV-SERVER-01

# Only current hour (fresh 12 readings)
curl http://localhost:8000/devices/DEV-SERVER-01/fresh

# Filtered by date range
curl "http://localhost:8000/devices/DEV-SERVER-01?from_time=2026-03-01T00:00:00Z&to_time=2026-03-04T23:59:59Z"

# Last 24 readings (2 hours)
curl "http://localhost:8000/devices/DEV-SERVER-01?limit=24"

# Summary only (lightweight)
curl http://localhost:8000/devices/DEV-SERVER-01/summary

# Force poll right now
curl -X POST http://localhost:8000/devices/DEV-SERVER-01/poll
```

## Connecting to Kafka (existing pipeline)

After getting data from this API, push to Kafka:

```python
from kafka import KafkaProducer
import requests, json

producer = KafkaProducer(
    bootstrap_servers=["kafka-broker:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

response = requests.get("http://localhost:8000/devices/DEV-SERVER-01")
producer.send("power_metrics", key=b"DEV-SERVER-01", value=response.json())
```
