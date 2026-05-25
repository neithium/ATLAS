from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'atlas',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def print_success():
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  🏆 ATLAS Batch Pipeline Completed Successfully!")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

with DAG(
    'atlas_batch_pipeline',
    default_args=default_args,
    description='Master Batch Pipeline: Ingestion -> Spark -> ClickHouse -> Status',
    schedule_interval='@daily',
    catchup=False,
    tags=['atlas', 'batch', 'master'],
) as dag:

    # Step 1: Trigger Fleet-wide Ingestion Export
    trigger_export = SimpleHttpOperator(
        task_id='trigger_ingestion_export',
        http_conn_id='atlas_ingestion_api',
        endpoint='/fleet/telemetry/export',
        method='POST',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True
    )

    # Step 2: Trigger Spark Batch Processing
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

    # Step 3: Trigger ClickHouse Analytics Load
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

    # Step 4: Verify Data in ClickHouse (Advanced Data Guard)
    verify_data_load = BashOperator(
        task_id='verify_data_load',
        bash_command="""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  🔍 ATLAS Data Guard: Verifying Data Quality"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        # 1. Check Row Count
        COUNT=$(curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/containers/atlas-analytics/exec \
        -H "Content-Type: application/json" \
        -d '{"AttachStdout": true, "AttachStderr": true, "Cmd": ["clickhouse-client", "--query", "SELECT count() FROM atlas.telemetry_refined"]}' > /tmp/verify_count.json && \
        EXEC_ID=$(cat /tmp/verify_count.json | grep -oP '(?<="Id":")[^"]+') && \
        curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
        -H "Content-Type: application/json" \
        -d '{}')

        # 2. Check Data Validity (Avg Power > 0)
        AVG_POWER=$(curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/containers/atlas-analytics/exec \
        -H "Content-Type: application/json" \
        -d '{"AttachStdout": true, "AttachStderr": true, "Cmd": ["clickhouse-client", "--query", "SELECT avg(MetricValue) FROM atlas.telemetry_refined"]}' > /tmp/verify_avg.json && \
        EXEC_ID=$(cat /tmp/verify_avg.json | grep -oP '(?<="Id":")[^"]+') && \
        curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
        -H "Content-Type: application/json" \
        -d '{}')

        echo "Total Records: $COUNT"
        echo "Average Power Value: $AVG_POWER"
        
        if [ "$COUNT" -eq "0" ]; then
            echo "❌ ERROR: No data found in ClickHouse!"
            exit 1
        elif [ "$(echo "$AVG_POWER == 0" | bc -l)" -eq 1 ]; then
            echo "❌ ERROR: Quality Check Failed (Zero Power Detected)!"
            exit 1
        else
            echo "✅ SUCCESS: $COUNT records found with healthy power levels."
        fi
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        """,
    )

    # Step 5: Final Status Log
    log_success = PythonOperator(
        task_id='log_pipeline_status',
        python_callable=print_success
    )

    # Chain: Export -> Spark -> ClickHouse -> Verify -> Log
    trigger_export >> trigger_spark_batch >> trigger_analytics_load >> verify_data_load >> log_success
