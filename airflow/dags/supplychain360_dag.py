from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


from ingestion.s3_ingester import (
    ingest_products,
    ingest_warehouses,
    ingest_suppliers,
    ingest_inventory,
    ingest_shipments
)
from ingestion.postgres_ingester import ingest_all_sales
from ingestion.gsheets_ingester import ingest_store_locations

default_args = {
    'owner': 'feyisayo',
    'depends_on_past': False,
    'email': ['solapeajiboye@gmail.com'],
    'email_on_failure': True,
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

    ingest_products_task = PythonOperator(
        task_id='ingest_products',
        python_callable=ingest_products,
    )

    ingest_warehouses_task = PythonOperator(
        task_id='ingest_warehouses',
        python_callable=ingest_warehouses,
    )

    ingest_suppliers_task = PythonOperator(
        task_id='ingest_suppliers',
        python_callable=ingest_suppliers,
    )

    ingest_inventory_task = PythonOperator(
        task_id='ingest_inventory',
        python_callable=ingest_inventory,
    )

    ingest_shipments_task = PythonOperator(
        task_id='ingest_shipments',
        python_callable=ingest_shipments,
    )

    ingest_sales_task = PythonOperator(
        task_id='ingest_sales',
        python_callable=ingest_all_sales,
    )

    ingest_store_locations_task = PythonOperator(
        task_id='ingest_store_locations',
        python_callable=ingest_store_locations,
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

    # Pipeline flow
    [
        ingest_products_task,
        ingest_warehouses_task,
        ingest_suppliers_task,
        ingest_inventory_task,
        ingest_shipments_task,
        ingest_sales_task,
        ingest_store_locations_task,
    ] >> dbt_staging >> dbt_analytics >> dbt_test