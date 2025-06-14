from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

# Add script directory to PYTHONPATH
sys.path.append("/src/airflow")

from selenium_yahoo_scraper import main as run_yahoo_scraper
from RawToBronze import main as run_raw_to_bronze

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_yahoo_scraper_and_load_to_bronze',
    default_args=default_args,
    description='Daily scrape from Yahoo and load to Bronze',
    schedule_interval='0 21 * * 1-5',  # Runs daily at 4:00 AM
    start_date=datetime(2025, 6, 4),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    def run_scraper_callable(**kwargs):
        start_date = datetime.today() - timedelta(days=1)
        start_date_str = start_date.strftime("%d-%m-%Y")
        output_dir = "/src/data/raw"
        run_yahoo_scraper(output_dir, start_date_str)

    def run_bronze_loader_callable(**kwargs):
        run_raw_to_bronze()

    scrape_task = PythonOperator(
        task_id='run_yahoo_scraper',
        python_callable=run_scraper_callable,
    )

    load_to_bronze = PythonOperator(
        task_id='load_to_bronze',
        python_callable=run_bronze_loader_callable,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_daily",
        trigger_dag_id="silver_to_gold",
        wait_for_completion=False,
        reset_dag_run=False,
        execution_date="{{ ts }}",
    )

    end = EmptyOperator(task_id='end')

    start >> scrape_task >> load_to_bronze >> trigger_silver >> end