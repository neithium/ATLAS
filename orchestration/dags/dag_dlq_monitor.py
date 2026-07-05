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
    'atlas_dlq_monitor',
    default_args=default_args,
    description='Monitors the Dead Letter Queue for failed/corrupt messages',
    schedule_interval='@hourly',
    catchup=False,
    tags=['atlas', 'kafka', 'dlq', 'monitoring'],
) as dag:

    # Check if any messages exist in the DLQ topic
    check_dlq = BashOperator(
        task_id='check_dead_letter_queue',
        bash_command="""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  ATLAS Dead Letter Queue Monitor"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Get the message count from the DLQ topic
        DLQ_COUNT=$(curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/containers/broker1/exec \
        -H "Content-Type: application/json" \
        -d '{"AttachStdout": true, "AttachStderr": true, "Cmd": ["kafka-run-class.sh", "kafka.tools.GetOffsetShell", "--broker-list", "localhost:9092", "--topic", "raw-server-metrics-dlq"]}' > /tmp/dlq_exec.json && \
        EXEC_ID=$(cat /tmp/dlq_exec.json | grep -oP '(?<="Id":")[^"]+') && \
        curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
        -H "Content-Type: application/json" \
        -d '{}')

        echo "DLQ Response: $DLQ_COUNT"

        # Also check the topic exists
        TOPIC_CHECK=$(curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/containers/broker1/exec \
        -H "Content-Type: application/json" \
        -d '{"AttachStdout": true, "AttachStderr": true, "Cmd": ["kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--describe", "--topic", "raw-server-metrics-dlq"]}' > /tmp/dlq_topic.json && \
        EXEC_ID=$(cat /tmp/dlq_topic.json | grep -oP '(?<="Id":")[^"]+') && \
        curl -s --unix-socket /var/run/docker.sock -X POST http://localhost/exec/$EXEC_ID/start \
        -H "Content-Type: application/json" \
        -d '{}')

        echo "DLQ Topic Info: $TOPIC_CHECK"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  DLQ check completed."
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        """,
    )
