from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings (Owner, retries on failure, etc.)
default_args = {
    'owner': 'Chris Yoon',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
with DAG(
    dag_id='bluejays_etl_pipeline_v1',
    default_args=default_args,
    description='Fetch MLB data, validate schema, and generate plots',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Runs daily
    catchup=False,               # Prevent backfilling past data
    tags=['mlops', 'bluejays'],
) as dag:

    # Task 1: Data Integrity Check (SDET Core)
    # The libraries are now pre-installed in the Docker image.
    # We immediately run Pytest to ensure data adheres to the Schema Contract.
    run_tests = BashOperator(
        task_id='run_data_integrity_tests',
        bash_command='python -m pytest /opt/airflow/tests'
    )

    # Task 2: Execute ETL & Visualization
    # Load Data -> Validate -> Generate Plot
    run_etl = BashOperator(
        task_id='run_etl_visualization',
        bash_command='python -m src.visualize',
        env={'PYTHONPATH': '/opt/airflow'}
    )

    # Define execution order (Arrows indicate flow)
    # 1. Run Tests (Quality Gate) -> 2. Run ETL (Only if tests pass)
    run_tests >> run_etl
