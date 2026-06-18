import os
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -- SLA miss callback - logs to Airflow and can trigger alerts
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISSED | DAG : {dag.dag_id} | Tasks: {task_list}")

def slack_alert(context):
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    log_url = context['task_instance'].log_url
    message = (
        f":red_circle: *FAILED* "
        f"| DAG: `{dag_id}`"
        f"| Task: `{task_id}`"
        f"| <{log_url} | View Logs>"
    )
    requests.post(webhook_url, json={'text': message})

# -- Define default arguments for the DAG which will apply to all tasks
default_args = {
    'owner': 'marketrisk',
    'retries': 1,
    'retry_delay' : timedelta(minutes=5),
    'on_failure_callback': slack_alert,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['learning_pc2026@outlook.com']
}


with DAG(
    dag_id = 'marketrisk_pipeline',
    default_args = default_args,
    description='MarketRisk Lakehouse end-to-end pipeline: ingest → bronze → silver → gold',
    schedule_interval='0 18 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=['marketrisk', 'production'],
) as dag:
    
    DBT_DIR = '/opt/airflow/dbt/marketrisk'
    DBT_PROFILES = '/home/airflow/.dbt'

    # -- STEP 1: ingest raw data from Yahoo Finance (run in parallel)
    ingest_prices = BashOperator(
        task_id = 'ingest_prices',
        bash_command = 'python /opt/airflow/ingestion/fetch_market_data.py',
        sla = timedelta(minutes=10)
    )

    ingest_reference_data = BashOperator(
        task_id = 'ingest_reference_data',
        bash_command = 'python /opt/airflow/ingestion/fetch_reference_data.py',
        sla = timedelta(minutes=10)
    )    

    upload_to_s3 = BashOperator(
        task_id = 'upload_to_s3',
        bash_command = 'python /opt/airflow/ingestion/upload_to_s3.py',
        sla=timedelta(minutes=15),
    )

    ingest_positions = BashOperator(
        task_id = 'ingest_positions',
        bash_command = 'python opt/airflow/ingestion/generate_positions.py',
        sla = timedelta(minutes=30)
    )