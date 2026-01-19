from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Chris Yoon',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bluejays_data_warehouse_v1',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['de', 'bluejays'],
) as dag:

    # Step 1: Initialize Postgres Tables
    init_db = BashOperator(
        task_id='initialize_database',
        bash_command='python -m src.init_db',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Step 2: Extract & Load Roster Data
    load_data = BashOperator(
        task_id='etl_bluejays_roster',
        bash_command='python -m src.etl_bluejays',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Step 3: Data Integrity (Pytest)
    run_tests = BashOperator(
        task_id='run_data_integrity_tests',
        bash_command='python -m pytest /opt/airflow/tests',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Pipeline Flow
    init_db >> load_data >> run_tests
