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
    '3_clickhouse_load',
    default_args=default_args,
    description='Step 3: Trigger ClickHouse Analytics Load',
    schedule_interval=None,
    catchup=False,
    tags=['atlas', 'step3'],
) as dag:

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
