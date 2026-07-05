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
    'atlas_streaming_supervisor',
    default_args=default_args,
    description='Monitors and restarts the Kafka Streaming job if it stops',
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['atlas', 'streaming', 'supervisor'],
) as dag:

    # Check if kafka_streaming.py is already running inside the processor container
    check_streaming = BashOperator(
        task_id='check_streaming_status',
        bash_command="""
        RESULT=$(curl -s --unix-socket /var/run/docker.sock http://localhost/containers/atlas-processor/top | grep -c "kafka_streaming" || true)
        if [ "$RESULT" -gt "0" ]; then
            echo "Streaming job is ALIVE. No action needed."
        else
            echo "Streaming job is DOWN. Restarting..."
            curl --unix-socket /var/run/docker.sock -X POST http://localhost/containers/atlas-processor/exec \
            -H "Content-Type: application/json" \
            -d '{"AttachStdout": true, "AttachStderr": true, "Detach": true, "Cmd": ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "/app/jobs/kafka_streaming.py"]}' > /tmp/stream_exec.json && \
            EXEC_ID=$(cat /tmp/stream_exec.json | grep -oP '(?<="Id":")[^"]+') && \
            curl --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
            -H "Content-Type: application/json" \
            -d '{"Detach": true}'
            echo "Streaming job RESTARTED successfully."
        fi
        """,
    )
