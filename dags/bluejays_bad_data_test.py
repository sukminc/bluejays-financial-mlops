from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chris Yoon',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='bluejays_v2_bad_data_test',
    default_args=default_args,
    description='QA Test: Ingest Bad Data -> Fail at DQ Gate',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['v2', 'qa_demo', 'fail_test'],
) as dag:

    # Task 1: Ingest the BAD CSV
    # We override the csv path dynamically using arguments (if logic allows)
    # OR we just hardcode the path in the command for this test.
    # Note: We need to modify ingest_raw.py slightly to accept args
    # OR just point to the bad file. For simplicity, we assume
    # ingest_raw.py is flexible or we swap the file.
    #
    # To avoid changing ingest_raw.py right now, we will do a trick:
    # We will pass the CSV_PATH as an env var if we update ingest_raw,
    # or simpler: We will just tell ingest_raw.py to load the bad file
    # by using a small python script inline or modifying ingest_raw.py to
    # take an argument.
    #
    # LET'S UPDATE ingest_raw.py TO TAKE ARGUMENTS FIRST (See below).
    task_ingest_bad_data = BashOperator(
        task_id='ingest_bad_csv',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python /opt/airflow/src/load/ingest_raw.py "
            "--input /opt/airflow/data/raw/manual/spotrac_bad_data.csv"
        )
    )

    # Task 2: Run DQ Checks (MUST FAIL)
    task_dq_gate = BashOperator(
        task_id='dq_gate_check',
        bash_command=(
            "export PYTHONPATH=$PYTHONPATH:/opt/airflow && "
            "python -m src.dq.checks"
        )
    )

    task_ingest_bad_data >> task_dq_gate
