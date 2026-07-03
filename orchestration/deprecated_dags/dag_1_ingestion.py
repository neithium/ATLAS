from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
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
    '1_ingestion_export',
    default_args=default_args,
    description='Step 1: Trigger Ingestion API Export',
    schedule_interval=None,
    catchup=False,
    tags=['atlas', 'step1'],
) as dag:

    trigger_export = SimpleHttpOperator(
        task_id='trigger_ingestion_export',
        http_conn_id='atlas_ingestion_api',
        endpoint='/pcid/PLATCUST001/acid/APPCUST0001/telemetry/export',
        method='POST',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True
    )
