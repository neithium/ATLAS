from airflow import DAG
try:
    from airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator
except ImportError:
    from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json

# Default arguments for all tasks
default_args = {
    'owner': 'atlas',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# The Directed Acyclic Graph (DAG) definition
with DAG(
    'telemetry_master_pipeline',
    default_args=default_args,
    description='Master orchestration for ATLAS Telemetry Ingestion & Processing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['atlas', 'ingestion', 'analytics'],
) as dag:

    # TASK 1: Trigger Ingestion API Export
    # Moves data from TimescaleDB -> Kafka
    trigger_export = SimpleHttpOperator(
        task_id='trigger_ingestion_export',
        http_conn_id='atlas_ingestion_api',
        endpoint='/pcid/PLATCUST001/acid/APPCUST0001/telemetry/export',
        method='POST',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True
    )

    # TASK 2: Trigger Spark Batch Processing
    # Cleans data and saves it to Delta Lake
    # Uses curl to talk to the Docker Socket directly (bypassing pip issues)
    trigger_spark_batch = BashOperator(
        task_id='trigger_spark_batch_processing',
        bash_command="""
        curl --unix-socket /var/run/docker.sock -X POST http://localhost/containers/atlas-processor/exec \
        -H "Content-Type: application/json" \
        -d '{"AttachStdout": true, "AttachStderr": true, "Cmd": ["spark-submit", "/app/jobs/batch_job.py"]}' > /tmp/exec_id.json && \
        EXEC_ID=$(cat /tmp/exec_id.json | grep -oP '(?<="Id":")[^"]+') && \
        curl --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
        -H "Content-Type: application/json" \
        -d "{}"
        """,
    )

    # TASK 3: Trigger ClickHouse Analytics Load
    # Moves data from Lakehouse (Delta) -> ClickHouse
    trigger_analytics_load = BashOperator(
        task_id='trigger_clickhouse_load',
        bash_command="""
        curl --unix-socket /var/run/docker.sock -X POST http://localhost/containers/atlas-analytics/exec \
        -H "Content-Type: application/json" \
        -d '{"AttachStdout": true, "AttachStderr": true, "Cmd": ["python3", "/app/clickhouse/delta_loader.py"]}' > /tmp/exec_id_ch.json && \
        EXEC_ID=$(cat /tmp/exec_id_ch.json | grep -oP '(?<="Id":")[^"]+') && \
        curl --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
        -H "Content-Type: application/json" \
        -d "{}"
        """,
    )

    def print_success():
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("  🏆 ATLAS Zero-Lag Pipeline Completed Successfully!")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # TASK 4: Final Logging
    log_success = PythonOperator(
        task_id='log_pipeline_status',
        python_callable=print_success
    )

    # Set the task sequence: Export -> Spark -> ClickHouse -> Log
    trigger_export >> trigger_spark_batch >> trigger_analytics_load >> log_success
