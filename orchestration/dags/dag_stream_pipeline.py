"""
ATLAS Stream Pipeline
=====================
Matches architecture stream path (Ingestion -> Kafka -> Spark Streaming -> Lakehouse Livewire):

  Airflow Trigger #1 : POST /fleet/telemetry/export   (Stream Mode -> Kafka)
  Supervisor         : ensure kafka_streaming.py running in atlas-processor
  Supervisor         : ensure run_livewire.py running in atlas-lakehouse

This DAG keeps the real-time path alive. Analytics load runs in atlas_batch_pipeline.

Schedule: */15 * * * *  (every 15 minutes)
DAG ID  : atlas_stream_pipeline
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from atlas_pipeline_ops import (
    ensure_kafka_streaming,
    ensure_lakehouse_livewire,
    log_pipeline_success,
)

default_args = {
    "owner": "atlas",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="atlas_stream_pipeline",
    default_args=default_args,
    description="Stream path: Kafka export + streaming + livewire supervisors",
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["atlas", "stream", "kafka"],
) as dag:

    # Trigger #1 — Ingestion stream mode export to Kafka
    trigger_stream_export = SimpleHttpOperator(
        task_id="trigger_stream_kafka_export",
        http_conn_id="atlas_ingestion_api",
        endpoint="/fleet/telemetry/export",
        method="POST",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        execution_timeout=timedelta(minutes=15),
    )

    ensure_streaming = PythonOperator(
        task_id="ensure_kafka_streaming_job",
        python_callable=ensure_kafka_streaming,
        execution_timeout=timedelta(minutes=10),
    )

    ensure_livewire = PythonOperator(
        task_id="ensure_lakehouse_livewire",
        python_callable=ensure_lakehouse_livewire,
        execution_timeout=timedelta(minutes=10),
    )

    log_success = PythonOperator(
        task_id="log_stream_health",
        python_callable=log_pipeline_success("ATLAS Stream Pipeline"),
    )

    (
        trigger_stream_export
        >> ensure_streaming
        >> ensure_livewire
        >> log_success
    )
