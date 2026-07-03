# ATLAS End Patch

## Codebase Summary

This workspace contains a multi-stage telemetry pipeline:

1. Ingestion generates or polls telemetry, writes hot-path rows to TimescaleDB, and now publishes Kafka batches for downstream streaming.
2. Processing consumes Kafka from `raw-server-metrics`, transforms the records, and writes Parquet files to the shared `stream-raw-data` volume.
3. Lakehouse livewire watches `/stream_raw`, bootstraps Delta tables safely, and merges incoming Parquet micro-batches into the refined Delta layer.

The key integration boundary is the shared file volume between the processor and lakehouse. The processor writes to `/app/data/processed`, which is mounted from `stream-raw-data`, and the lakehouse reads the same volume at `/stream_raw`.

## What Was Fixed

### 1. Lakehouse livewire startup on empty input

The livewire reader originally tried to infer schema from an empty source folder. That made the stream fragile at cold start.

Fix:
- Switched the stream to use an explicit schema derived from `schema.output_schema`.
- Allowed `run_livewire.py` to initialize the target Delta table even when `/stream_raw` had no files yet.

Relevant files:
- [delta_lake/run_livewire.py](../delta_lake/run_livewire.py)
- [schema/output_schema.py](../schema/output_schema.py)

### 2. Missing schema imports

`schema/input_schema.py` and `schema/output_schema.py` were missing Spark type imports, which would break imports at runtime.

Fix:
- Added the missing Spark schema imports so both modules load cleanly.

Relevant files:
- [schema/input_schema.py](../schema/input_schema.py)
- [schema/output_schema.py](../schema/output_schema.py)

### 3. Processor/lakehouse volume alignment

The intended shared path was already close, but the actual data flow needed to be verified carefully so the processor and lakehouse were reading and writing the same named volume.

Confirmed wiring:
- Processor writes Parquet to `stream-raw-data:/app/data/processed`.
- Lakehouse reads the same volume at `stream-raw-data:/stream_raw`.

Relevant file:
- [docker-compose.yml](../docker-compose.yml)

### 4. Ingestion did not publish Kafka by default

The biggest upstream issue was that the ingestion poller was successfully ingesting telemetry into TimescaleDB, but it was not publishing the same batch to Kafka unless explicitly enabled.

Fix:
- Added Kafka publishing to the poller after successful hot-path ingestion.
- Enabled that path in Compose with `ENABLE_KAFKA_PUSH=1`.

Relevant files:
- [ingestion/core/poller.py](../ingestion/core/poller.py)
- [docker-compose.yml](../docker-compose.yml)

### 5. Processor skipped already-published data

After the ingestion fix, the processor still started with an empty first batch because the Kafka source used `startingOffsets=latest`. That is correct for some production setups, but it is a bad default for local recovery and restart testing because it can skip data that was already published before the stream attached.

Fix:
- Made Kafka starting offsets configurable.
- Defaulted the processor to `earliest` so it can replay backlog safely in the dev stack.

Relevant file:
- [processing/jobs/kafka_streaming.py](../processing/jobs/kafka_streaming.py)

## Troubleshooting Notes

### Symptom: Lakehouse stream started, but no data arrived

Observed logs showed the lakehouse starting successfully and staying idle.

Root cause:
- The upstream processor was healthy, but there were no Kafka messages arriving because ingestion was not publishing to Kafka by default.

How it was verified:
- Ingestion logs showed successful TimescaleDB ingest and then `[kafka] Publishing 80,000 records to raw-server-metrics...` after the fix.
- Processor logs showed Spark started cleanly, but only the initial batch was empty until offset handling was corrected.

### Symptom: Processor started with `throughput=0.00 rec/sec`

Observed logs showed `WORKER 2 | BATCH 0 START` followed by a zero-row batch.

Root cause:
- The stream was attaching after data had already been produced, and `startingOffsets=latest` caused the consumer to skip the backlog.

How it was fixed:
- Changed the Kafka source to use `KAFKA_STARTING_OFFSETS` and default it to `earliest`.

### Symptom: Livewire schema/bootstrap failures

Observed behavior:
- The livewire mode was fragile when the source directory was empty.

Root cause:
- Schema inference from empty input is not safe for a streaming bootstrap path.

How it was fixed:
- Used an explicit schema and bootstrap table initialization before starting the stream.

## Validation Performed

The following checks passed during the fix:

- `python -m py_compile delta_lake/run_livewire.py schema/output_schema.py schema/input_schema.py`
- `python -m py_compile processing/jobs/kafka_streaming.py`
- `docker compose config -q`
- `docker compose up -d --build atlas-ingestion atlas-processor atlas-lakehouse`

Runtime confirmation from logs:

- Ingestion logged a successful Kafka publish of `80,000` records to `raw-server-metrics`.
- Processor Spark jobs started successfully and attached to Kafka.
- Lakehouse livewire started successfully and monitored `/stream_raw`.

## Current State

The pipeline is now wired end to end:

- Ingestion produces Kafka data.
- Processor consumes Kafka and writes Parquet to the shared stream volume.
- Lakehouse livewire reads the shared stream volume and merges the data into Delta.

If this regresses again, the first things to check are:

1. Ingestion logs for Kafka publish messages.
2. Processor logs for `startingOffsets` behavior and non-zero batch sizes.
3. Lakehouse logs for stream startup and batch counts.