from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'feyisayo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='supplychain360_pipeline',
    default_args=default_args,
    description='SupplyChain360 daily data pipeline',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 3, 13),
    catchup=False,
    tags=['supplychain360'],
) as dag:

    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command='cd /opt/airflow/ingestion && python main.py',
    )

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command='cd /opt/airflow/dbt/supplychain360_dbt && dbt run --select staging --profiles-dir /opt/airflow/dbt/supplychain360_dbt',
    )

    dbt_analytics = BashOperator(
        task_id='dbt_analytics',
        bash_command='cd /opt/airflow/dbt/supplychain360_dbt && dbt run --select analytics --profiles-dir /opt/airflow/dbt/supplychain360_dbt',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt/supplychain360_dbt && dbt test --profiles-dir /opt/airflow/dbt/supplychain360_dbt',
    )

    ingest_data >> dbt_staging >> dbt_analytics >> dbt_test