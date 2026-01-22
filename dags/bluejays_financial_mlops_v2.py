from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_ID = "bluejays_financial_mlops_v2"
TZ = "America/Toronto"

DEFAULT_ARGS = {
    "owner": "Chris Yoon",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def bash(module: str) -> str:
    """
    Run a Python module inside the Airflow container with a stable PYTHONPATH.

    We explicitly `cd /opt/airflow` so relative imports / files behave the same
    as interactive debugging inside the container.
    """
    return (
        "set -euo pipefail; "
        "cd /opt/airflow; "
        "PYTHONPATH=/opt/airflow python -m " + module
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=(
        "Baseline: init_db -> "
        "roster_sync -> "
        "salary_load -> "
        "stats_load -> "
        "dq_gate"),
    start_date=pendulum.datetime(2026, 1, 20, tz=TZ),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bluejays", "dq", "baseline", "v2"],
) as dag:
    init_db = BashOperator(
        task_id="init_db",
        bash_command=bash("src.db.init_db"),
    )

    roster_sync = BashOperator(
        task_id="roster_sync",
        bash_command=bash("src.extract.mlb_stats_api"),
    )

    salary_load = BashOperator(
        task_id="salary_load",
        bash_command=bash("src.load.load_salary"),
    )

    stats_load = BashOperator(
        task_id="stats_load",
        bash_command=bash("src.load.load_stats"),
    )

    # Fail-fast gate: src.dq.checks must exit(1) on threshold breach.
    dq_gate = BashOperator(
        task_id="dq_gate",
        bash_command=bash("src.dq.checks"),
    )

    init_db >> roster_sync >> salary_load >> stats_load >> dq_gate
