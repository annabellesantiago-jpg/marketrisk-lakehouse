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
        env = {'RUN_DATE': {{'ds'}}},
        sla = timedelta(minutes=10)
    )

    ingest_positions = BashOperator(
        task_id = 'ingest_positions',
        bash_command = 'python /opt/airflow/ingestion/generate_positions.py',
        env = {'RUN_DATE': {{'ds'}}},
        sla = timedelta(minutes=10)
    )

    ingest_reference_data = BashOperator(
        task_id = 'ingest_reference_data',
        bash_command = 'python /opt/airflow/ingestion/fetch_reference_data.py',
        env = {'RUN_DATE': {{'ds'}}},
        sla = timedelta(minutes=10)
    )    

    # -- STEP 2: Load dbt seeds
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_DIR} && dbt seed --profiles-dir {DBT_PROFILES}',
        sla=timedelta(minutes=10)
    )

    # -- STEP 3: Test seeds before progressing
    dbt_test_seeds = BashOperator(
        task_id='dbt_test_seeds',
        bash_command=f'cd {DBT_DIR} && dbt test --select seeds --profiles-dir {DBT_PROFILES}',
        sla=timedelta(minutes=10)
    )

    # -- STEP 4: Build Silver layer
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command=f'cd {DBT_DIR} && dbt run --select silver --profiles-dir {DBT_PROFILES}',
        sla=timedelta(minutes=20)
    )

    # -- STEP 5: Test Silver layer
    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver',
        bash_command=f'cd {DBT_DIR} && dbt test --select silver --profiles-dir {DBT_PROFILES}',
        sla=timedelta(minutes=10)
    )

    # -- STEP 6: Build Gold layer
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command=f'cd {DBT_DIR} && dbt run --select gold --profiles-dir {DBT_PROFILES}',
        sla=timedelta(minutes=30)
    )

    # -- STEP 7: Test Gold layer
    dbt_test_gold = BashOperator(
        task_id='dbt_test_gold',
        bash_command=f'cd {DBT_DIR} && dbt test --select gold --profiles-dir {DBT_PROFILES}',
        sla=timedelta(minutes=10)
    )

    # -- Option B: DatabricksRunNowOperator (uncomment when Databricks Job is set up)
    # from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
    #
    # dbt_databricks = DatabricksRunNowOperator(
    #     task_id='dbt_databricks',
    #     databricks_conn_id='databricks_default',
    #     job_id=REPLACE_WITH_YOUR_JOB_ID,
    # )

    # -- Pipeline dependency chain
    ingest_prices >> ingest_reference_data
    [ingest_reference_data, ingest_positions] >> dbt_seed >> dbt_test_seeds
    dbt_test_seeds >> dbt_silver >> dbt_test_silver
    dbt_test_silver >> dbt_gold >> dbt_test_gold
