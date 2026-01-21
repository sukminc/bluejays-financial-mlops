from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def fetch_spotrac_payroll(**context):
    """Runtime import wrapper to avoid Broken DAG on missing local modules.

    Airflow parses DAG files in the scheduler/webserver process. If a local module
    is missing/mis-mounted, importing at module import-time breaks the entire DAG.

    This wrapper defers the import until task execution time and provides a clear
    error message if the project package layout is not set up correctly.
    """
    try:
        # Preferred target layout:
        #   /opt/airflow/src/extract/spotrac.py  (function: fetch_spotrac_payroll)
        from src.extract.spotrac import fetch_spotrac_payroll as impl  # type: ignore
        return impl(**context)
    except ModuleNotFoundError:
        # Backward-compatible fallback (if you still keep the module at src root)
        try:
            from src.scraper_playwright import fetch_spotrac_payroll as impl  # type: ignore
            return impl(**context)
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "Cannot import Spotrac extractor. Expected one of:\n"
                "  - src.extract.spotrac.fetch_spotrac_payroll\n"
                "  - src.scraper_playwright.fetch_spotrac_payroll\n\n"
                "Fix by ensuring the code is mounted into the Airflow container and PYTHONPATH includes /opt/airflow."
            ) from e

default_args = {
    'owner': 'Chris Yoon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bluejays_payroll_pipeline',
    default_args=default_args,
    description='Extract Spotrac payroll data and persist to data layer',
    schedule_interval='@daily',  # Runs once a day
    start_date=days_ago(1),
    catchup=False,
    tags=['bluejays', 'etl', 'playwright'],
) as dag:

    # Task 1: Extract Spotrac payroll data (Playwright)
    extract_task = PythonOperator(
        task_id='extract_spotrac_data',
        python_callable=fetch_spotrac_payroll,
    )

    # Pipeline
    extract_task
