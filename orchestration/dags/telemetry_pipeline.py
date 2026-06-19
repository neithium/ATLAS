"""
ATLAS Telemetry Master Pipeline
=================================
DAG ID   : telemetry_master_pipeline
Schedule : @hourly  (was timedelta(days=1))
Owner    : Nandini (Kafka/Airflow)

Flow:
  1. trigger_ingestion_export  — POST targeted export endpoint (PCID/ACID specific)
  2. trigger_spark_batch       — spark-submit batch_job.py in atlas-processor (detached)
  3. check_refined_data        — PythonSensor: wait for parquet in /data/refined
  4. trigger_clickhouse_load   — python3 /app/delta_loader.py in atlas-analytics
                                 Path fixed: /app/delta_loader.py (not /app/clickhouse/...)
  5. log_pipeline_success      — Final audit log

This DAG is a customer-scoped sibling of atlas_batch_pipeline.
Use atlas_batch_pipeline for fleet-wide runs.
Use this for per-customer (PCID/ACID) scoped telemetry runs.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

from atlas_utils import docker_exec_or_raise, _docker_exec

log = logging.getLogger(__name__)

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
    Detached exec + polling — no stream blocking, no 'up for retry' issue.
    """
    log.info("🚀 Triggering Spark batch job in atlas-processor...")
    docker_exec_or_raise(
        container="atlas-processor",
        cmd=["spark-submit", "/app/jobs/batch_job.py"],
        timeout_s=90 * 60,
    )
    log.info("✅ Spark batch job completed.")


def check_refined_parquet(**context):
    """
    PythonSensor callable — returns True when parquet files are present
    in /data/refined inside atlas-analytics (refined-volume mount point).
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
        exit_code = _docker_exec("atlas-analytics", check_cmd, timeout_s=60)
        if exit_code == 0:
            log.info("✅ Refined parquet files detected — proceeding to ClickHouse load.")
            return True
        log.info("⏳ No refined parquet files yet — sensor will retry.")
        return False
    except Exception as exc:
        log.warning("Sensor check exception (%s) — will retry.", exc)
        return False


def run_clickhouse_load(**context):
    """
    Trigger delta_loader.py inside atlas-analytics.
    Correct path: /app/delta_loader.py
    (mounted from ./storage/clickhouse/delta_loader.py via docker-compose)
    """
    log.info("📦 Triggering ClickHouse delta loader in atlas-analytics...")
    docker_exec_or_raise(
        container="atlas-analytics",
        cmd=["python3", "/app/delta_loader.py"],
        timeout_s=30 * 60,
    )
    log.info("✅ ClickHouse delta loader completed.")


def log_pipeline_success(**context):
    log.info("━" * 55)
    log.info("  🏆 ATLAS Telemetry Pipeline Completed Successfully!")
    log.info("  DAG run  : %s", context["run_id"])
    log.info("  Exec date: %s", context["logical_date"])
    log.info("━" * 55)


# ============================================================================
# DAG definition
# ============================================================================
with DAG(
    dag_id="telemetry_master_pipeline",
    default_args=default_args,
    description="Customer-scoped Telemetry Pipeline: Ingestion → Spark → Sensor → ClickHouse",
    schedule_interval="@hourly",    # Was timedelta(days=1) — changed to @hourly
    catchup=False,
    max_active_runs=1,
    tags=["atlas", "ingestion", "analytics"],
) as dag:

    # ── Step 1: Trigger customer-scoped ingestion export ────────────────────
    trigger_export = SimpleHttpOperator(
        task_id="trigger_ingestion_export",
        http_conn_id="atlas_ingestion_api",
        endpoint="/pcid/PLATCUST001/acid/APPCUST0001/telemetry/export",
        method="POST",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Step 2: Spark batch (detached exec + polling) ────────────────────────
    trigger_spark_batch = PythonOperator(
        task_id="trigger_spark_batch_processing",
        python_callable=run_spark_batch,
        execution_timeout=timedelta(minutes=100),
    )

    # ── Step 3: Sensor — wait for refined parquet ────────────────────────────
    check_refined_data = PythonSensor(
        task_id="check_refined_parquet_exists",
        python_callable=check_refined_parquet,
        poke_interval=120,
        timeout=60 * 180,
        mode="reschedule",
        soft_fail=False,
    )

    # ── Step 4: ClickHouse load ──────────────────────────────────────────────
    trigger_analytics_load = PythonOperator(
        task_id="trigger_clickhouse_load",
        python_callable=run_clickhouse_load,
        execution_timeout=timedelta(minutes=40),
    )

    # ── Step 5: Final log ────────────────────────────────────────────────────
    log_success = PythonOperator(
        task_id="log_pipeline_status",
        python_callable=log_pipeline_success,
    )

    # ── Chain ────────────────────────────────────────────────────────────────
    (
        trigger_export
        >> trigger_spark_batch
        >> check_refined_data
        >> trigger_analytics_load
        >> log_success
    )
