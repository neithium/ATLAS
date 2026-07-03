"""
ATLAS Master Batch Pipeline — Production-Ready
===============================================
DAG ID   : atlas_batch_pipeline
Schedule : @hourly
Owner    : Nandini (Kafka/Airflow)

Flow:
  1. trigger_ingestion_export   — POST /fleet/telemetry/export on atlas-ingestion
                                  Flushes TimescaleDB → Kafka
  2. trigger_spark_batch        — spark-submit batch_job.py in atlas-processor
                                  (detached exec + polling — no more stuck tasks)
  3. check_refined_data         — PythonSensor: waits until at least one .parquet
                                  exists in /data/refined inside atlas-analytics.
                                  ClickHouse is NOT triggered on empty data.
  4. trigger_clickhouse_load    — python3 /app/delta_loader.py in atlas-analytics
                                  Path fixed: /app/delta_loader.py (not /app/clickhouse/...)
  5. verify_data_load           — Direct ClickHouse HTTP query (port 8123).
                                  Math check uses python3 -c, NOT bc (bc not installed).
  6. log_pipeline_success       — Final audit log.

Key fixes vs previous version:
  - Schedule @daily → @hourly
  - grep -oP exec ID parsing → robust json.loads() via atlas_utils._docker_exec()
  - Spark runs detached (no more 'up for retry' due to stream hang)
  - bc -l → python3 -c float comparison
  - /app/clickhouse/delta_loader.py → /app/delta_loader.py (teammate's path fix)
  - Added PythonSensor before ClickHouse step
  - execution_timeout set explicitly on long-running tasks
"""

import json
import logging
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator, PythonSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

# Shared Docker exec helper (detached + polling)
from atlas_utils import docker_exec_or_raise, _docker_exec

log = logging.getLogger(__name__)

CLICKHOUSE_HTTP = "http://atlas-analytics:8123"
REFINED_PATH_IN_ANALYTICS = "/data/refined" # refined-volume mount in atlas-analytics

# ─── Default Args ───────────────────────────────────────────────────────────
default_args = {
    "owner": "atlas",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# ============================================================================
# Task callables
# ============================================================================

def run_spark_batch(**context):
    """
    Trigger spark-submit batch_job.py inside atlas-processor.
    Uses detached Docker exec + polling so Airflow never blocks on the stream.
    Spark jobs can take 10-30 min — previous approach caused 'up for retry'.
    Timeout: 90 minutes (adjust if your cluster is slower).
    """
    log.info("🚀 Triggering Spark batch job in atlas-processor...")
    docker_exec_or_raise(
        container="atlas-processor",
        cmd=["spark-submit", "/app/jobs/batch_job.py"],
        timeout_s=90 * 60,  # 90 minutes
    )
    log.info("✅ Spark batch job completed successfully.")


def check_refined_parquet(**context):
    """
    PythonSensor callable.
    Returns True  → parquet files found in /data/refined → proceed to ClickHouse.
    Returns False → no files yet → sensor reschedules and tries again.

    Runs `python3 -c` inside atlas-analytics (which has refined-volume at /data/refined).
    Does NOT raise — returns bool so the sensor retries gracefully.
    """
    check_cmd = [
        "python3", "-c",
        (
            "import pathlib, sys; "
            "p = pathlib.Path('/data/refined'); "
            "files = [f for f in p.rglob('*.parquet') "
            "         if '_delta_log' not in str(f) and not f.name.startswith('.')] "
            "if p.exists() else []; "
            "print(f'Parquet files found: {len(files)}'); "
            "sys.exit(0 if files else 1)"
        ),
    ]
    try:
        exit_code = _docker_exec(
            container="atlas-analytics",
            cmd=check_cmd,
            timeout_s=60,
        )
        if exit_code == 0:
            log.info("✅ Refined parquet files detected — proceeding to ClickHouse load.")
            return True
        else:
            log.info("⏳ No refined parquet files yet — sensor will retry.")
            return False
    except Exception as exc:
        log.warning("Sensor check failed (%s) — will retry.", exc)
        return False


def run_clickhouse_load(**context):
    """
    Trigger delta_loader.py inside atlas-analytics.
    Path: /app/delta_loader.py  (mounted from ./storage/clickhouse/delta_loader.py)
    Note: NOT /app/clickhouse/delta_loader.py — that path doesn't exist.
    Timeout: 30 minutes.
    """
    log.info("📦 Triggering ClickHouse delta loader in atlas-analytics...")
    docker_exec_or_raise(
        container="atlas-analytics",
        cmd=["python3", "/app/delta_loader.py"],
        timeout_s=30 * 60,  # 30 minutes
    )
    log.info("✅ ClickHouse delta loader completed successfully.")


def verify_clickhouse_data(**context):
    """
    Verify data quality in ClickHouse via direct HTTP API (port 8123).
    Avoids Docker exec entirely — simpler, faster, no stream parsing.
    Math check uses float() — NOT bc (bc is not installed in Airflow container).
    """
    log.info("🔍 ATLAS Data Guard: Verifying ClickHouse data quality...")

    def _ch_query(sql: str) -> str:
        """Run a ClickHouse HTTP query and return the raw response text."""
        url = f"{CLICKHOUSE_HTTP}/?query={urllib.request.quote(sql)}"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return resp.read().decode("utf-8").strip()
        except urllib.error.HTTPError as exc:
            # ClickHouse returns error details in response body
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP error {exc.code}: {body}") from exc

    # ── Row count ────────────────────────────────────────────────────────────
    count_raw = _ch_query("SELECT count() FROM atlas.telemetry_refined")
    try:
        count = int(count_raw)
    except ValueError:
        raise RuntimeError(
            f"Could not parse row count from ClickHouse response: {count_raw!r}"
        )

    log.info("Total records in atlas.telemetry_refined: %d", count)

    if count == 0:
        raise ValueError(
            "❌ Data Guard FAILED: No records found in atlas.telemetry_refined. "
            "The pipeline may have produced no output — check Spark logs."
        )

    # ── Average MetricValue sanity check ─────────────────────────────────────
    avg_raw = _ch_query("SELECT avg(MetricValue) FROM atlas.telemetry_refined")
    try:
        avg_val = float(avg_raw) if avg_raw else 0.0
    except ValueError:
        raise RuntimeError(
            f"Could not parse avg MetricValue from ClickHouse: {avg_raw!r}"
        )

    log.info("Average MetricValue: %.4f", avg_val)

    # Use Python float comparison — NOT bc -l (bc is NOT installed)
    if avg_val == 0.0:
        raise ValueError(
            "❌ Data Guard FAILED: Average MetricValue is 0.0. "
            "This indicates corrupt or zeroed-out power data."
        )

    log.info(
        "✅ Data Guard PASSED: %d records, avg MetricValue = %.4f",
        count, avg_val,
    )


def log_pipeline_success(**context):
    log.info("━" * 55)
    log.info("  🏆 ATLAS Batch Pipeline Completed Successfully!")
    log.info("  DAG run  : %s", context["run_id"])
    log.info("  Exec date: %s", context["logical_date"])
    log.info("━" * 55)


# ============================================================================
# DAG definition
# ============================================================================
with DAG(
    dag_id="atlas_batch_pipeline",
    default_args=default_args,
    description="Master Batch Pipeline: Ingestion → Spark → Sensor → ClickHouse → Verify",
    schedule_interval="@hourly",   # Was @daily — changed to @hourly per requirements
    catchup=False,
    max_active_runs=1,             # Prevent overlapping runs during long Spark jobs
    tags=["atlas", "batch", "master", "kafka", "nandini"],
) as dag:

    # ── Step 1: Trigger fleet-wide ingestion export ──────────────────────────
    # Calls the ingestion API to flush TimescaleDB → Kafka
    trigger_export = SimpleHttpOperator(
        task_id="trigger_ingestion_export",
        http_conn_id="atlas_ingestion_api",
        endpoint="/fleet/telemetry/export",
        method="POST",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Step 2: Spark batch processing (detached exec + polling) ─────────────
    trigger_spark_batch = PythonOperator(
        task_id="trigger_spark_batch_processing",
        python_callable=run_spark_batch,
        execution_timeout=timedelta(minutes=100),  # Hard ceiling for Airflow task
    )

    # ── Step 3: Wait for refined parquet files ───────────────────────────────
    # Sensor retries every 2 min for up to 3 hours before failing
    check_refined_data = PythonSensor(
        task_id="check_refined_parquet_exists",
        python_callable=check_refined_parquet,
        poke_interval=120,           # Check every 2 minutes
        timeout=60 * 180,            # Give up after 3 hours
        mode="reschedule",           # Free up Airflow worker slot between pokes
        soft_fail=False,
    )

    # ── Step 4: ClickHouse delta load ────────────────────────────────────────
    trigger_analytics_load = PythonOperator(
        task_id="trigger_clickhouse_load",
        python_callable=run_clickhouse_load,
        execution_timeout=timedelta(minutes=40),
    )

    # ── Step 5: Verify data quality ──────────────────────────────────────────
    verify_data_load = PythonOperator(
        task_id="verify_data_load",
        python_callable=verify_clickhouse_data,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Step 6: Final audit log ──────────────────────────────────────────────
    log_success = PythonOperator(
        task_id="log_pipeline_status",
        python_callable=log_pipeline_success,
    )

    # ── Pipeline chain ───────────────────────────────────────────────────────
    (
        trigger_export
        >> trigger_spark_batch
        >> check_refined_data
        >> trigger_analytics_load
        >> verify_data_load
        >> log_success
    )
