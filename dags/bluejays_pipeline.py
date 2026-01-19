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

    # Task 1: Environment Setup (Install Dependencies)
    # Note: In a real production environment, these should be pre-installed
    # in the Docker Image. We install them at runtime here for local testing.
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install --user -r /opt/airflow/requirements.txt'
    )

    # Task 2: Data Integrity Check (SDET Core)
    # Runs the Pytest suite to ensure data adheres to the Schema Contract.
    # If this fails, the pipeline stops immediately (Quality Gate).
    run_tests = BashOperator(
        task_id='run_data_integrity_tests',
        bash_command='pytest /opt/airflow/tests'
    )

    # Task 3: Execute ETL & Visualization
    # Load Data -> Validate -> Generate Plot
    run_etl = BashOperator(
        task_id='run_etl_visualization',
        bash_command='python -m src.visualize'
    )

    # Define execution order (Arrows indicate flow)
    # 1. Install Libs -> 2. Run Tests -> 3. Run ETL
    install_deps >> run_tests >> run_etl
