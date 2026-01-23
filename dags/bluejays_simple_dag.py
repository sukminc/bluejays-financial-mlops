from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chris Yoon',
    'retries': 0,  # Fail fast for debugging purposes
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='bluejays_v2_simple_pipeline',
    default_args=default_args,
    description='Simple: Manual CSV -> Raw Table -> DQ Check',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['v2', 'simple', 'dq'],
) as dag:

    # Task 1: Load Raw CSV
    # Using parenthesis to split long strings for Flake8 E501 compliance
    task_ingest_raw = BashOperator(
        task_id='ingest_raw_csv',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python /opt/airflow/src/load/ingest_raw.py"
        )
    )

    # Define dependency flow
    task_ingest_raw
