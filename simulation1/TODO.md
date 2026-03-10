# Implementation TODO: Redis + MinIO 7-Day Warmup Solution

## Status: COMPLETED

### Step 1: Create MinIO Store Module

- [x] Create `core/minio_store.py` - MinIO client for historical data storage
- [x] Methods: save_readings(), get_history(), get_history_range()

### Step 2: Update Device Config

- [x] Add MinIO configuration to `config/devices.py`
- [x] Add MINIO_HOST, MINIO_PORT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

### Step 3: Modify Redis Store

- [x] Update `core/redis_store.py` to archive old data to MinIO
- [x] Reduce Redis retention to 24 hours (288 readings)

### Step 4: Update Response Builder

- [x] Modify `core/response_builder.py` to merge Redis + MinIO data
- [x] Add coverage_pct and complete fields to response

### Step 5: Update Docker Compose

- [x] Add MinIO service to `ATLAS/simulation1/docker-compose.yml`
- [x] Add minio to requirements.txt

### Step 6: Test

- [ ] Verify warmup scenario works correctly
- [ ] Verify coverage_pct shows correct percentage
- [ ] Verify complete field works correctly

## Architecture Summary

### Data Flow:

1. **Redis** → Last 24 hours (288 readings at 5-min intervals)
2. **MinIO** → Previous 6 days (1728 readings) stored as hourly JSON files
3. **Merged** → Full 7 days (2016 readings) returned to clients

### Warmup Behavior:

- **Never crashes** - Returns available data instead of failing
- **coverage_pct** - Tells clients exact completion % (Day 1: ~8.9%, Day 7+: 100%)
- **complete** - Boolean explicitly indicates full 7-day data availability

### Key Changes:

1. Added `core/minio_store.py` for S3-compatible storage
2. Modified Redis to only keep 24 hours, archive older data to MinIO
3. Response builder now merges Redis + MinIO data
4. All endpoints handle warmup gracefully
5. Added MinIO to docker-compose and requirements
