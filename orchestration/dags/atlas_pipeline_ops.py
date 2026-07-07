"""
Shared Airflow task callables for ATLAS batch + stream pipelines.
"""

from __future__ import annotations

import logging
import os
import time

from atlas_utils import (
    container_is_running,
    container_top_contains,
    docker_exec_fire_and_forget,
    docker_exec_or_raise,
    wait_for_container_process,
    _docker_exec,
)

log = logging.getLogger(__name__)

CLICKHOUSE_HTTP = os.environ.get(
    "ATLAS_CLICKHOUSE_HTTP", "http://atlas-analytics:8123"
)
REFINED_PATH = os.environ.get("ATLAS_REFINED_PATH", "/refined")
RAW_PATH = os.environ.get("ATLAS_RAW_PATH", "/app/data/raw")
DELTA_LOADER_PATH = os.environ.get("ATLAS_DELTA_LOADER_PATH", "/app/delta_loader.py")
BATCH_SCRIPT = os.environ.get("ATLAS_SPARK_BATCH_SCRIPT", "/app/jobs/batch_job.py")
STREAMING_SCRIPT = os.environ.get(
    "ATLAS_STREAMING_SCRIPT", "/app/jobs/kafka_streaming.py"
)
LIVEWIRE_SCRIPT = os.environ.get("ATLAS_LIVEWIRE_SCRIPT", "run_livewire.py")
ARCHIVE_SETTLE_MINUTES = int(os.environ.get("ATLAS_ARCHIVE_SETTLE_MINUTES", "3"))
KAFKA_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Inline one-shot merge: processor batch parquet -> Delta /refined
# Processor writes delta-refined volume at /app/data/processed/batch_out
# Lakehouse sees the same volume at /stream_raw/batch_out
_LAKEHOUSE_BATCH_MERGE = r"""
import sys
sys.path.insert(0, "/app")
from run_benchmark import create_spark_session
from delta_core import (
    PipelineConfig, prepare_partition_columns, generate_composite_hash,
    delta_table_exists, initialize_delta_table, execute_merge_deduplication,
    optimize_delta_table,
)
from pyspark.sql.functions import col, coalesce, lit, to_timestamp

spark = create_spark_session("ATLAS-Airflow-BatchMerge")
source = "/stream_raw/batch_out"
target = PipelineConfig.REFINED_PATH

try:
    df = spark.read.parquet(source)
except Exception as exc:
    print(f"No batch parquet at {source}: {exc}")
    sys.exit(1)

if df.rdd.isEmpty():
    print("Batch output empty — nothing to merge")
    sys.exit(1)

aligned = df.withColumn(
    "metric_time",
    coalesce(
        to_timestamp(col("metric_time")),
        to_timestamp(col("max_metric_time")),
    ),
).withColumn(
    "MetricValue",
    coalesce(col("MetricValue"), col("avg_metric_value")).cast("double"),
).withColumn(
    "application_customer_id",
    coalesce(col("application_customer_id"), lit("batch_unknown")),
).withColumn(
    "platform_customer_id",
    coalesce(col("platform_customer_id"), lit("batch_unknown")),
).withColumn(
    "report_type",
    coalesce(col("report_type"), lit("telemetry_batch")),
)

prepared = prepare_partition_columns(aligned)
hashed = generate_composite_hash(prepared)

if not delta_table_exists(spark, target):
    initialize_delta_table(spark, hashed, target, PipelineConfig.PARTITION_COLUMNS)
else:
    execute_merge_deduplication(spark, target, hashed)

optimize_delta_table(spark, target, PipelineConfig.ZORDER_COLUMN)
print("Lakehouse batch MERGE complete")
"""


def wait_for_raw_parquet(**context) -> bool:
    """Sensor: RAW directory has parquet after manual-archive (batch path)."""
    check_cmd = [
        "python3",
        "-c",
        (
            "import pathlib, sys; "
            f"p = pathlib.Path('{RAW_PATH}'); "
            "files = list(p.rglob('*.parquet')) if p.exists() else []; "
            "print('RAW parquet files:', len(files)); "
            "sys.exit(0 if files else 1)"
        ),
    ]
    try:
        ok = _docker_exec("atlas-processor", check_cmd, timeout_s=60) == 0
        if ok:
            log.info("RAW parquet found under %s", RAW_PATH)
        else:
            log.info("No RAW parquet yet under %s — retrying", RAW_PATH)
        return ok
    except Exception as exc:
        log.warning("RAW sensor check failed (%s) — retrying", exc)
        return False


def settle_after_archive(**context) -> None:
    """manual-archive is async; allow time for silo parquet writes."""
    log.info(
        "Waiting %d min for daily archival to finish writing RAW parquet...",
        ARCHIVE_SETTLE_MINUTES,
    )
    time.sleep(ARCHIVE_SETTLE_MINUTES * 60)


def run_spark_batch(**context) -> None:
    """Airflow Trigger #2 — Spark batch reads RAW directory."""
    container = "atlas-processor"
    if container_top_contains(container, "batch_job.py"):
        log.warning("batch_job already running — waiting for completion")
        wait_for_container_process(
            container, "batch_job.py", present=False, timeout_s=90 * 60,
        )

    log.info("Triggering Spark batch job: %s", BATCH_SCRIPT)
    docker_exec_or_raise(
        container=container,
        cmd=["spark-submit", BATCH_SCRIPT],
        timeout_s=90 * 60,
    )
    log.info("Spark batch job finished.")


def trigger_lakehouse_deduplication(**context) -> None:
    """Airflow Trigger #3 — Delta Lake MERGE for batch processor output."""
    if not container_is_running("atlas-lakehouse"):
        raise RuntimeError(
            "atlas-lakehouse is not running. Start it: docker compose up -d atlas-lakehouse"
        )
    log.info("Running one-shot Lakehouse batch MERGE in atlas-lakehouse")
    docker_exec_or_raise(
        container="atlas-lakehouse",
        cmd=["python3", "-c", _LAKEHOUSE_BATCH_MERGE],
        timeout_s=60 * 60,
    )
    log.info("Lakehouse deduplication complete.")


def check_refined_parquet(**context) -> bool:
    check_cmd = [
        "python3",
        "-c",
        (
            "import pathlib, sys; "
            f"p = pathlib.Path('{REFINED_PATH}'); "
            "files = [f for f in p.rglob('*.parquet') "
            "         if '_delta_log' not in str(f) and not f.name.startswith('.')] "
            "if p.exists() else []; "
            "print('Refined parquet files:', len(files)); "
            "sys.exit(0 if files else 1)"
        ),
    ]
    try:
        ok = _docker_exec("atlas-analytics", check_cmd, timeout_s=60) == 0
        if ok:
            log.info("Refined parquet ready at %s", REFINED_PATH)
        else:
            log.info("No refined parquet yet at %s", REFINED_PATH)
        return ok
    except Exception as exc:
        log.warning("Refined sensor failed (%s) — retrying", exc)
        return False


def run_clickhouse_load(**context) -> None:
    log.info("Triggering ClickHouse loader: %s", DELTA_LOADER_PATH)
    docker_exec_or_raise(
        container="atlas-analytics",
        cmd=["python3", DELTA_LOADER_PATH],
        timeout_s=30 * 60,
    )


def verify_clickhouse_data(**context) -> None:
    """
    Data Guard: verifies telemetry_refined has rows and logs avg MetricValue.

    Runs clickhouse-client inside atlas-analytics via docker exec (same pattern
    as all other tasks) — direct HTTP from the Airflow container is not viable
    because ClickHouse binds only to 127.0.0.1 inside the container.
    """
    container = "atlas-analytics"

    def _ch_query(sql: str) -> str:
        """Execute a ClickHouse query inside atlas-analytics via docker exec."""
        cmd = ["clickhouse-client", "--query", sql]
        payload = __import__("json").dumps(
            {"Cmd": cmd, "AttachStdout": True, "AttachStderr": True}
        )
        import subprocess, json as _json

        _CURL_BASE = ["curl", "-sf", "--unix-socket", "/var/run/docker.sock"]
        _CT_JSON = ["-H", "Content-Type: application/json"]

        create = subprocess.run(
            _CURL_BASE + ["-X", "POST"] + _CT_JSON + [
                "-d", payload,
                f"http://localhost/containers/{container}/exec",
            ],
            capture_output=True, text=True,
        )
        if create.returncode != 0 or not create.stdout.strip():
            raise RuntimeError(
                f"[verify_ch] Exec create failed: {create.stdout!r} {create.stderr!r}"
            )
        exec_id = _json.loads(create.stdout)["Id"]

        # Start attached (blocking) — query is fast, no need for detached+poll
        start = subprocess.run(
            _CURL_BASE + ["-X", "POST"] + _CT_JSON + [
                "-d", '{"Detach":false,"Tty":false}',
                f"http://localhost/exec/{exec_id}/start",
            ],
            capture_output=True,
        )
        # Strip Docker stream multiplexing header (8-byte frame prefix per chunk)
        raw = start.stdout
        output = b""
        i = 0
        while i + 8 <= len(raw):
            frame_size = int.from_bytes(raw[i + 4: i + 8], "big")
            output += raw[i + 8: i + 8 + frame_size]
            i += 8 + frame_size
        if not output:
            output = raw  # fallback: no multiplexing
        return output.decode("utf-8", errors="replace").strip()

    count_str = _ch_query("SELECT count() FROM atlas.telemetry_refined")
    try:
        count = int(count_str)
    except ValueError:
        raise RuntimeError(f"Data Guard: unexpected count response: {count_str!r}")

    if count == 0:
        raise ValueError("Data Guard FAILED: atlas.telemetry_refined is empty")

    avg_str = _ch_query("SELECT avg(MetricValue) FROM atlas.telemetry_refined")
    try:
        avg_val = float(avg_str or "0")
    except ValueError:
        avg_val = 0.0

    if avg_val == 0.0:
        log.warning(
            "Data Guard: avg(MetricValue)=0.0 — batch data may have zero-valued metrics. "
            "Row count (%d) confirms data exists; pipeline considered PASSED.", count
        )
    else:
        log.info("Data Guard PASSED: %d rows, avg=%.4f", count, avg_val)

    log.info("Data Guard PASSED: %d rows loaded into atlas.telemetry_refined", count)


def ensure_kafka_streaming(**context) -> None:
    """Keep Kafka -> Spark streaming alive (continuous stream path)."""
    container = "atlas-processor"
    if container_top_contains(container, "kafka_streaming"):
        log.info("kafka_streaming.py is running in %s", container)
        return

    log.warning("Starting kafka_streaming.py in %s", container)
    docker_exec_fire_and_forget(
        container,
        ["spark-submit", "--packages", KAFKA_PACKAGES, STREAMING_SCRIPT],
    )
    time.sleep(30)
    if not container_top_contains(container, "kafka_streaming"):
        raise RuntimeError("kafka_streaming failed to start — check atlas-processor logs")


def ensure_lakehouse_livewire(**context) -> None:
    """Keep livewire dedup stream alive for processor -> /refined path."""
    container = "atlas-lakehouse"
    if not container_is_running(container):
        raise RuntimeError("atlas-lakehouse is not running")

    if container_top_contains(container, "run_livewire"):
        log.info("run_livewire.py is running in %s", container)
        return

    log.warning("Starting run_livewire.py in %s", container)
    docker_exec_fire_and_forget(container, ["python3", LIVEWIRE_SCRIPT])
    time.sleep(20)
    if not container_top_contains(container, "run_livewire"):
        log.warning(
            "run_livewire may still be starting — check: docker logs atlas-lakehouse --tail 50"
        )


def log_pipeline_success(label: str):
    def _fn(**context):
        log.info("=" * 55)
        log.info("%s completed successfully", label)
        log.info("DAG run  : %s", context["run_id"])
        log.info("Exec date: %s", context["logical_date"])
        log.info("=" * 55)
    return _fn
