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
    # Creates dim_players and fact_contracts if they don't exist
    init_db = BashOperator(
        task_id='initialize_database',
        bash_command='python -m src.init_db',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Step 2: Extract & Load Roster Data (Dimensions)
    # Syncs players from MLB API to dim_players
    load_roster = BashOperator(
        task_id='etl_bluejays_roster',
        bash_command='python -m src.etl_bluejays',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Step 3: Load Player Finances (Facts)
    # Syncs salary and performance data to fact_contracts
    load_finances = BashOperator(
        task_id='load_player_finances',
        bash_command='python -m src.load_salaries',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Step 4: Data Integrity (Pytest)
    # Validates schema and data quality across both tables
    run_tests = BashOperator(
        task_id='run_data_integrity_tests',
        bash_command='python -m pytest /opt/airflow/tests',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Updated Pipeline Flow
    init_db >> load_roster >> load_finances >> run_tests
