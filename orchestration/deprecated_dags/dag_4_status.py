from airflow import DAG
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
    print("  🏆 ATLAS Zero-Lag Pipeline Completed Successfully!")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

with DAG(
    '4_pipeline_status',
    default_args=default_args,
    description='Step 4: Final Pipeline Status',
    schedule_interval=None,
    catchup=False,
    tags=['atlas', 'step4'],
) as dag:

    log_success = PythonOperator(
        task_id='log_pipeline_status',
        python_callable=print_success
    )
