from airflow import DAG
from airflow.operators.bash import BashOperator
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

with DAG(
    '2_spark_processing',
    default_args=default_args,
    description='Step 2: Trigger Spark Batch Processing',
    schedule_interval=None,
    catchup=False,
    tags=['atlas', 'step2'],
) as dag:

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
