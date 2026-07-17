import os
import requests

from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# -- SLA miss callback - logs to Airflow and can trigger alerts
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISSED | DAG : {dag.dag_id} | Tasks: {task_list}")

def slack_alert(context):
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        return
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
        task_id='ingest_prices',
        bash_command = 'python /opt/airflow/ingestion/fetch_market_data.py',
        env = {'RUN_DATE': '{{ ds }}' },
        append_env=True,
        sla = timedelta(minutes=15)
    )

    ingest_positions = BashOperator(
        task_id = 'ingest_positions',
        bash_command = 'python /opt/airflow/ingestion/generate_positions.py',
        env = {'RUN_DATE': '{{ ds }}' },
        append_env=True,
        sla = timedelta(minutes=10)
    )

    ingest_reference_data = BashOperator(
        task_id = 'ingest_reference_data',
        bash_command = 'python /opt/airflow/ingestion/fetch_reference_data.py',
        env={'RUN_DATE': '{{ ds }}'},
        append_env=True,        
        sla = timedelta(minutes=10),
    )    

    # -- STEP 2: Load Bronze via COPY INTO on Databricks SQL Warehouse
    bronze_prices = DatabricksSqlOperator(
        task_id='bronze_ingest_prices',
        databricks_conn_id='databricks_default',
        http_path=os.environ.get('DATABRICKS_HTTP_PATH'),
        sql="""
            COPY INTO market_risk_dev.bronze.market_prices
            FROM (
            SELECT
                CAST(Date       AS DATE)      AS Date,
                CAST(Open       AS DOUBLE)    AS Open,
                CAST(High       AS DOUBLE)    AS High,
                CAST(Low        AS DOUBLE)    AS Low,
                CAST(Close      AS DOUBLE)    AS Close,
                CAST(Volume     AS DOUBLE)    AS Volume,
                CAST(ticker     AS STRING)    AS ticker,
                CAST(fetched_at AS STRING)    AS fetched_at,
                _metadata.file_path           AS _source_file,
                current_timestamp()           AS _ingested_at
            FROM 's3://{{ var.value.s3_bucket }}/raw/prices/'
            )
            FILEFORMAT = CSV
            PATTERN = 'year=*/month=*/day=*/*.csv'
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """,
        sla=timedelta(minutes=15)
    )

    bronze_positions = DatabricksSqlOperator(
        task_id='bronze_ingest_positions',
        databricks_conn_id='databricks_default',
        http_path=os.environ.get('DATABRICKS_HTTP_PATH'),
        sql="""
            COPY INTO market_risk_dev.bronze.positions
            FROM (
            SELECT
                trade_id,
                desk,
                book_id,
                trader_id,
                counterparty,
                asset_class,
                instrument_type,
                ticker,
                isin,
                cusip,
                direction,
                CAST(notional      AS DOUBLE) AS notional,
                currency,
                CAST(trade_date    AS DATE)   AS trade_date,
                CAST(maturity_date AS DATE)   AS maturity_date,
                CAST(generated_at  AS STRING) AS generated_at,
                _metadata.file_path           AS _source_file,
                current_timestamp()           AS _ingested_at
            FROM 's3://{{ var.value.s3_bucket }}/raw/positions/'
            )
            FILEFORMAT = CSV
            PATTERN = 'year=*/month=*/day=*/*.csv'
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """,
        sla=timedelta(minutes=15)
    )

    bronze_fx_rates = DatabricksSqlOperator(
        task_id='bronze_ingest_fx_rates',
        databricks_conn_id='databricks_default',
        http_path=os.environ.get('DATABRICKS_HTTP_PATH'),
        sql="""
            COPY INTO market_risk_dev.bronze.fx_rates
            FROM (
            SELECT
                currency,
                CAST(rate_vs_usd AS DOUBLE) AS rate_vs_usd,
                CAST(as_of_date  AS DATE)   AS as_of_date,
                CAST(generated_at AS STRING) AS generated_at,
                _metadata.file_path         AS _source_file,
                current_timestamp()         AS _ingested_at
            FROM 's3://{{ var.value.s3_bucket }}/raw/reference/'
            )
            FILEFORMAT = CSV
            PATTERN = 'fx_rates.csv'
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
            COPY_OPTIONS ('force' = 'true')
        """,
        sla=timedelta(minutes=10)
    )

    # -- STEP 3: Load dbt seeds
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_DIR} && dbt seed --profiles-dir {DBT_PROFILES}',
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

    # -- Pipeline dependency chain
    # Ingestion (prices first, then reference data which reads price files)
    ingest_prices >> ingest_reference_data

    # Bronze COPY INTO — each table after its upstream ingestion
    ingest_prices        >> bronze_prices
    ingest_positions     >> bronze_positions
    ingest_reference_data >> bronze_fx_rates

    # Silver needs Bronze + seeds both complete
    [bronze_prices, bronze_positions, bronze_fx_rates, dbt_seed] >> dbt_silver
    dbt_silver >> dbt_test_silver >> dbt_gold >> dbt_test_gold