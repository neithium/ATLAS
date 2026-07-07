"""
ATLAS Batch Pipeline
====================
Matches the architecture batch path (Ingestion Archive -> Spark Batch -> Delta Merge -> ClickHouse Load -> Data Guard Verify):

  1. Wait for raw Parquet archive (PythonSensor)
  2. Settle period (3 min sleep)
  3. Spark Batch Processing: spark-submit /app/jobs/batch_job.py
  4. Delta Lakehouse MERGE: runs inline Delta merge + Z-Order in atlas-lakehouse
  5. Wait for refined Parquet in Delta (PythonSensor)
  6. ClickHouse Load: delta_loader.py
  7. Data Guard: verifies row counts and non-zero averages via ClickHouse HTTP API
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from atlas_pipeline_ops import (
    wait_for_raw_parquet,
    settle_after_archive,
    run_spark_batch,
    trigger_lakehouse_deduplication,
    check_refined_parquet,
    run_clickhouse_load,
    verify_clickhouse_data,
    log_pipeline_success,
)

default_args = {
    "owner": "atlas",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="atlas_batch_pipeline",
    default_args=default_args,
    description="Batch path: raw sensor -> Spark batch -> Delta merge -> ClickHouse load -> Verify",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["atlas", "batch"],
) as dag:

    # 1. Wait for raw Parquet files in the raw storage volume (from ingestion archive)
    sensor_raw = PythonSensor(
        task_id="wait_for_raw_parquet",
        python_callable=wait_for_raw_parquet,
        poke_interval=30,
        timeout=1800,
        mode="poke",
    )

    # 2. Allow archive writes to fully settle
    settle_archive = PythonOperator(
        task_id="settle_after_archive",
        python_callable=settle_after_archive,
    )

    # 3. Run Spark batch processing (reads raw Parquet, writes Parquet output)
    spark_batch = PythonOperator(
        task_id="run_spark_batch",
        python_callable=run_spark_batch,
        execution_timeout=timedelta(minutes=90),
    )

    # 4. Trigger Lakehouse Deduplication (Delta Lake MERGE)
    lakehouse_merge = PythonOperator(
        task_id="trigger_lakehouse_deduplication",
        python_callable=trigger_lakehouse_deduplication,
        execution_timeout=timedelta(minutes=60),
    )

    # 5. Check if refined Parquet has been generated in the Lakehouse
    sensor_refined = PythonSensor(
        task_id="check_refined_parquet",
        python_callable=check_refined_parquet,
        poke_interval=30,
        timeout=1800,
        mode="poke",
    )

    # 6. Run ClickHouse Load (Delta Loader parses Delta Lake Parquet files into ClickHouse)
    clickhouse_load = PythonOperator(
        task_id="run_clickhouse_load",
        python_callable=run_clickhouse_load,
        execution_timeout=timedelta(minutes=30),
    )

    # 7. Verify ClickHouse data count and averages
    clickhouse_verify = PythonOperator(
        task_id="verify_clickhouse_data",
        python_callable=verify_clickhouse_data,
        execution_timeout=timedelta(minutes=5),
    )

    log_success = PythonOperator(
        task_id="log_pipeline_status",
        python_callable=log_pipeline_success("ATLAS Batch Pipeline"),
    )

    (
        sensor_raw
        >> settle_archive
        >> spark_batch
        >> lakehouse_merge
        >> sensor_refined
        >> clickhouse_load
        >> clickhouse_verify
        >> log_success
    )
