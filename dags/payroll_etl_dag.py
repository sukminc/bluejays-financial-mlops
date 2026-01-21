import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# -------------------------------------------------------------------------
# SYSTEM PATH CONFIGURATION
# -------------------------------------------------------------------------
# Ensure Airflow can see the 'src' package.
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


# -------------------------------------------------------------------------
# RUNTIME IMPORT WRAPPERS
# -------------------------------------------------------------------------
def fetch_spotrac_payroll_task(**context):
    """
    Runtime wrapper for the Spotrac scraper.

    Why this is needed:
    1. Prevents Top-Level Code errors during DAG parsing.
    2. Allows the Scheduler to parse the DAG even if Playwright
       browsers aren't installed in the Scheduler container.
    """
    try:
        # Import strictly from the new architectural path
        from src.extract.spotrac import fetch_spotrac_payroll

        # Execute the function
        output_path = fetch_spotrac_payroll()

        # Log success for Airflow UI
        print(f"Extraction successful. Data saved to: {output_path}")
        return output_path

    except ImportError as e:
        raise ImportError(
            f"Failed to import 'src.extract.spotrac'. "
            f"Current sys.path: {sys.path}. Error: {e}"
        )


# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------
default_args = {
    'owner': 'Chris Yoon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    # Retry twice if the scraper fails (e.g., network blip)
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'bluejays_payroll_pipeline',
    default_args=default_args,
    description='ETL: Scrape Spotrac Payroll -> Raw Data Layer',
    schedule_interval='@daily',      # Run once daily
    start_date=days_ago(1),
    catchup=False,
    tags=['bluejays', 'etl', 'playwright', 'phase1'],
) as dag:

    # Task 1: Extract Data (Playwright Scraper)
    extract_task = PythonOperator(
        task_id='extract_spotrac_data',
        python_callable=fetch_spotrac_payroll_task,
    )

    # Future Phase 2 Tasks (Transformation & Loading)
    # transform_task = ...
    # load_task = ...

    # extract_task >> transform_task >> load_task
