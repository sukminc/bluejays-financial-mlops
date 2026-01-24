from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chris Yoon',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='bluejays_regression_suite',
    default_args=default_args,
    description='CI/CD: Verify DQ Logic with Good & Bad Datasets',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['qa', 'regression', 'ci'],
) as dag:

    # ----------------------------------------------------
    # Case 1: Positive Test (Happy Path)
    # Feed clean data -> Expect 0 errors -> Exit 0
    # ----------------------------------------------------
    t1_load_clean = BashOperator(
        task_id='setup_clean_data',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python /opt/airflow/src/load/ingest_raw.py "
            "--input /opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv "
            "--table test_stg_clean"
        )
    )

    t2_verify_clean = BashOperator(
        task_id='verify_clean_passes',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python -m src.dq.checks "
            "--table test_stg_clean"
        )
    )

    # ----------------------------------------------------
    # Case 2: Negative Test (Sad Path)
    # Feed bad data -> Expect >0 errors -> Exit 0 (via --expect-fail)
    # ----------------------------------------------------
    t3_load_bad = BashOperator(
        task_id='setup_bad_data',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python /opt/airflow/src/load/ingest_raw.py "
            "--input /opt/airflow/data/raw/manual/spotrac_bad_data.csv "
            "--table test_stg_bad"
        )
    )

    t4_verify_bad = BashOperator(
        task_id='verify_bad_is_caught',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python -m src.dq.checks "
            "--table test_stg_bad "
            "--expect-fail"
        )
    )

    # Execution Flow: Run both tests in parallel
    t1_load_clean >> t2_verify_clean
    t3_load_bad >> t4_verify_bad
