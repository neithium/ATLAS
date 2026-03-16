# Power Monitor API (Updated 2026-03-15)

IPMI → Redis → MinIO → FastAPI pipeline for 7-day power telemetry.

## Quick Start (All-in-One Docker)

```
cd ATLAS
docker compose down --remove-orphans
docker compose up --build atlas-ingestion
docker compose logs -f atlas-ingestion
```

**Ports:**

- **API:** http://localhost:8000/docs (Swagger/OpenAPI)
- **Nginx:** http://localhost:80 → API 8000
- **MinIO:** http://localhost:9000 (minioadmin/minioadmin)
- **MinIO Console:** http://localhost:9001

**Restart (instant):**

```
docker compose restart atlas-ingestion
```

**Expected logs:**

```
=== Starting All-in-One PowerPulse ===
Starting Redis... (PING)
Starting MinIO... (health OK)
Starting API... Uvicorn INFO on 0.0.0.0:8000
Starting Nginx...
All services running - tailing logs
```

**Stop:**

```
docker compose down -v
```

## Generate Test Data (7-day buffer)

```
cd ATLAS/ingestion
python fill_7day_data.py   # Fills Redis/MinIO with mock data
```

## Endpoints

| Method    | Path                           | Description                                |
| --------- | ------------------------------ | ------------------------------------------ |
| `GET`     | `/health`                      | System/Redis/MinIO status + buffer %       |
| `GET`     | `/devices`                     | List all registered devices                |
| `GET`     | `/devices/{device_id}`         | **Full 7-day JSON** (2016 readings/device) |
| `GET`     | `/devices/{device_id}/fresh`   | Last 12 readings (1hr)                     |
| `GET`     | `/devices/{device_id}/latest`  | Single latest reading                      |
| `GET`     | `/devices/{device_id}/summary` | Aggregated stats (no raw data)             |
| `**GET**` | **`/acids/{acid}`**            | **Devices + datapoints under ACID**        |
| `POST`    | `/devices/{device_id}/poll`    | Force IPMI poll now                        |
| `DEL`     | `/devices/{device_id}/flush`   | Clear Redis buffer                         |

## Test Commands

```bash
# Health
curl http://localhost:8000/health

# List devices
curl http://localhost:8000/devices | jq '.[0:3]'

# Full data for 1 device
curl http://localhost:8000/devices/DEV-SERVER-01 | jq '.data.PowerDetail | length'

# **NEW ACID endpoint**
curl http://localhost:8000/acids/APP-CUST-001 | jq '.device_count, .devices | keys'

# Fresh readings
curl http://localhost:8000/devices/DEV-SERVER-01/fresh | jq '.PowerDetail | length'

# Force poll (generates new data)
curl -X POST http://localhost:8000/devices/DEV-SERVER-01/poll
```

## ACID Endpoint Details

**GET /acids/{acid}**

Filters devices by `application_customer_id`, returns full datapoints parallel.

**Response:**

```json
{
  "acid": "APP-CUST-001",
  "device_count": 25,
  "devices": {
    "DEV-SERVER-01": { "data.PowerDetail": [...2016 readings...] },
    "DEV-SERVER-02": { ... },
    ...
  }
}
```

## Data Flow

```
IPMI (BMC) ──5min→ Redis (288 recent) ──┐
Mock (fill_7day_data.py) ────────────────┤
                                         │ FastAPI (main.py)
**POST /acids/{acid}** ──────────────────┘
         │
Redis/MinIO ← core/redis_store.push_reading()
```

## Troubleshooting

| Issue               | Fix                                           |
| ------------------- | --------------------------------------------- | ------- | ------------------------ | -------- |
| Container exits     | `docker compose logs atlas-ingestion`         |
| No /health response | Wait 60s warmup or `python fill_7day_data.py` |
| No data for ACID    | Check `curl http://localhost:8000/devices     | jq '.[] | .application_customer_id | unique'` |
| IPMI errors         | `MOCK_IPMI=true docker compose up`            |

## Architecture

```
Physical Server BMC ─IPMI→ poller.py (5min) → redis_store.py → MinIO (historical)
                                              │
                                              ↓ uvicorn main:app → response_builder.py → JSON
```

**Buffer:** Redis (24hr) + MinIO (6days) = 7-day rolling window per device.

Updated: Docker all-in-one, ACID endpoint, fill data script.
