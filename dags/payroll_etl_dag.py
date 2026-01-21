import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add scripts folder to path so we can import the module
# because we MUST modify sys.path before importing the custom module.
sys.path.append('/opt/airflow/scripts')
from scraper_playwright import fetch_spotrac_payroll  # noqa: E402

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
    description='Scrape Spotrac payroll data using Playwright',
    schedule_interval='@daily',  # Runs once a day
    start_date=days_ago(1),
    catchup=False,
    tags=['bluejays', 'etl', 'playwright'],
) as dag:

    # Task 1: Extract Data using Playwright
    extract_task = PythonOperator(
        task_id='extract_spotrac_data',
        python_callable=fetch_spotrac_payroll,
    )

    # Define Pipeline
    extract_task
