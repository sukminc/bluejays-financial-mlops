from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chris Yoon',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='bluejays_v2_simple_pipeline',
    default_args=default_args,
    description='End-to-End: CSV -> Staging -> DQ Gate -> Fact Table',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['v2', 'production'],
) as dag:

    # 1. Ingest Raw (Uses default Good CSV)
    task_ingest_raw = BashOperator(
        task_id='ingest_raw_csv',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python /opt/airflow/src/load/ingest_raw.py"
        )
    )

    # 2. DQ Gate (Fail Fast)
    task_dq_gate = BashOperator(
        task_id='dq_gate_check',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python -m src.dq.checks"
        )
    )

    # 3. Transform & Load Fact
    task_transform = BashOperator(
        task_id='transform_to_fact',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python /opt/airflow/src/load/transform_fact_salary.py"
        )
    )

    # Flow
    task_ingest_raw >> task_dq_gate >> task_transform
