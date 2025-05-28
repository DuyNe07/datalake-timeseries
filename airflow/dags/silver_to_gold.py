from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Add your script directory to Python path
sys.path.append("/src/airflow")

# Import the main() functions from each file
from LoadToIndices import main as run_load_to_indices
from LoadToMacro import main as run_load_to_macro
from LoadToUSBonds import main as run_load_to_us_bonds
from BronzeToSilver import main as run_bronze_to_silver

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'silver_to_gold',
    default_args=default_args,
    description='Load the data from bronze to silver in order to process the data, then to gold to build model and extract BI',
    schedule = None,
    start_date=datetime(2025, 5, 15),
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')

    def run_bronze_to_silver_task():
        logging.info("Running bronze to silver...")
        run_bronze_to_silver()
        logging.info("Loading completed.")

    def run_load_to_indices_task():
        logging.info("Loading data into indices...")
        run_load_to_indices()
        logging.info("Loading completed.")

    def run_load_to_macro_task():
        logging.info("Loading data into macro...")
        run_load_to_macro()
        logging.info("Loading completed.")

    def run_load_to_us_bonds_task():
        logging.info("Loading data into us bonds...")
        run_load_to_us_bonds()
        logging.info("Loading completed.")

    bronze_to_silver_task = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=run_bronze_to_silver_task,
    )

    load_to_indices_task = PythonOperator(
        task_id='load_to_indices',
        python_callable=run_load_to_indices_task,
    )

    load_to_macro_task = PythonOperator(
        task_id='load_to_macro',
        python_callable=run_load_to_macro_task,
    )

    load_to_us_bonds_task = PythonOperator(
        task_id='load_to_us_bonds',
        python_callable=run_load_to_us_bonds_task,
    )

    trigger_model_training = TriggerDagRunOperator(
        task_id="trigger_training_models",
        trigger_dag_id="training_models",
        wait_for_completion=False,
        reset_dag_run=True,
        execution_date="{{ ds }}",
    )

    end = EmptyOperator(task_id='end')

    start >> bronze_to_silver_task >> load_to_indices_task >> load_to_macro_task >> load_to_us_bonds_task >> trigger_model_training >> end