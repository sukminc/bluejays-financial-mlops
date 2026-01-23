

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="one_time_spotrac_csv_load",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["one_time", "spotrac", "raw"],
) as dag:

    load_spotrac_csv = BashOperator(
        task_id="load_spotrac_csv",
        bash_command=(
            "set -e; "
            "PYTHONPATH=/opt/airflow "
            "python -m src.load.load_salary_csv "
            "--input /opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv "
            "--truncate"
        ),
    )

    load_spotrac_csv
