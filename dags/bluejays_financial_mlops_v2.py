from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "Chris Yoon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _py(module: str) -> str:
    """
    Run a python module inside the Airflow container.
    We rely on PYTHONPATH=/opt/airflow so `import src...` works.
    """
    return f"python -m {module}"


with DAG(
    dag_id="bluejays_financial_mlops_v2",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bluejays", "etl", "dq"],
) as dag:
    # 1) Init DB schema (dim/fact tables)
    init_db = BashOperator(
        task_id="init_db",
        bash_command=_py("src.db.init_db"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 2) Extract roster + stats raw from MLB API
    extract_mlb = BashOperator(
        task_id="extract_mlb_roster_stats",
        bash_command=_py("src.extract.mlb_stats_api"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 3) Extract payroll raw from Spotrac (Playwright)
    extract_spotrac = BashOperator(
        task_id="extract_spotrac_payroll",
        bash_command=_py("src.extract.spotrac"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 4) Upsert players dimension (canonical)
    upsert_players = BashOperator(
        task_id="upsert_dim_players",
        bash_command=_py("src.load.upsert_players"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 5) Map Spotrac names -> MLBAM player_id bridge
    map_spotrac = BashOperator(
        task_id="map_spotrac_to_mlbam",
        bash_command=_py("src.load.map_spotrac"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 6) Load salary fact (keyed by player_id)
    load_salary = BashOperator(
        task_id="load_fact_salary",
        bash_command=_py("src.load.load_salary"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 7) Load stats fact (keyed by player_id)
    load_stats = BashOperator(
        task_id="load_fact_player_stats",
        bash_command=_py("src.load.load_stats"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # 8) Run DQ gate (fail-fast)
    dq_gate = BashOperator(
        task_id="dq_gate",
        bash_command=_py("src.dq.checks"),
        env={"PYTHONPATH": "/opt/airflow"},
    )

    # Flow
    init_db >> [extract_mlb, extract_spotrac] >> upsert_players
    upsert_players >> map_spotrac >> load_salary
    upsert_players >> load_stats
    [load_salary, load_stats] >> dq_gate
