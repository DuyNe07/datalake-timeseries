from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Add your script directory to Python path
sys.path.append("/src/airflow")

# Import the main() functions from each file
from fred_inflation_scraper import main as run_inflation
from fred_interest_scraper import main as run_interest

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'monthly_inflation_interest_pipeline',
    default_args=default_args,
    description='Fetch latest inflation and interest rate data monthly on 15th',
    schedule_interval='0 8 15 * *',
    start_date=datetime(2025, 6, 15),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    def run_inflation_task():
        logging.info("Running inflation data scraper...")
        run_inflation()
        logging.info("Inflation data completed.")

    def run_interest_task():
        logging.info("Running interest rate data scraper...")
        run_interest()
        logging.info("Interest rate data completed.")

    def run_bronze_loader_callable(**kwargs):
        run_raw_to_bronze()

    inflation_task = PythonOperator(
        task_id='fetch_inflation_data',
        python_callable=run_inflation_task,
    )

    interest_task = PythonOperator(
        task_id='fetch_interest_data',
        python_callable=run_interest_task,
    )

    load_to_bronze = PythonOperator(
        task_id='load_to_bronze',
        python_callable=run_bronze_loader_callable,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_monthly",
        trigger_dag_id="silver_to_gold",
        wait_for_completion=False,
        reset_dag_run=False,
        execution_date="{{ ts }}",
    )

    end = EmptyOperator(task_id='end')

    start >> [inflation_task, interest_task] >> load_to_bronze >> trigger_silver >> end