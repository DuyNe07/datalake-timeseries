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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_yahoo_scraper_and_load_to_bronze',
    default_args=default_args,
    description='Daily scrape from Yahoo and load to Bronze',
    schedule_interval='0 21 * * 1-5',
    #schedule_interval = None,
    start_date=datetime(2025, 5, 26),
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')

    def run_scraper_callable(**kwargs):
        #start_date_str = datetime.today().strftime("%d-%m-%Y")
        start_date_str = '07-03-2025'
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
        reset_dag_run=True,
        execution_date="{{ ds }}",
    )

    end = EmptyOperator(task_id='end')

    start >> scrape_task >> load_to_bronze >> trigger_silver >> end