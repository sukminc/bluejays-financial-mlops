from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_CSV = "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv"

with DAG(
    dag_id="one_time_spotrac_csv_load",
    description="stg_spotrac_bluejays_salary_raw",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Toronto"),
    schedule=None,  # one-time manual run only
    catchup=False,
    tags=["bluejays", "spotrac", "manual", "raw"],
) as dag:
    load_spotrac_csv = BashOperator(
        task_id="load_spotrac_csv",
        bash_command=(
            "set -euo pipefail; "
            "cd /opt/airflow; "
            "PYTHONPATH=/opt/airflow "
            "python -m src.load.load_salary_csv "
            "--input '{{ dag_run.conf.get(\"input_csv\", \"" + DEFAULT_CSV + "\") }}' "
            "--truncate"
        ),
    )

    load_spotrac_csv
