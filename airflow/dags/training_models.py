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
from TrainingSrVAR import main as run_train_srvar
from TrainingVARNN import main as run_train_varnn

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'training_models',
    default_args=default_args,
    description='Training models for each field',
    schedule = None,
    start_date=datetime(2025, 5, 15),
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')

    def run_train_srvar_task(table_name):
        logging.info("Running bronze to silver...")
        run_train_srvar(table_name)
        logging.info("Loading completed.")

    def run_train_varnn_task(table_name):
        logging.info("Loading data into indices...")
        run_train_varnn(table_name)
        logging.info("Loading completed.")

    table_names = ['indices', 'macro', 'usbonds']
    srvar_tasks = []
    varnn_tasks = []

    for table in table_names:
        task = PythonOperator(
            task_id=f'train_srvar_{table}',
            python_callable=run_train_srvar_task,
            op_args=[table],
        )
        srvar_tasks.append(task)

        task = PythonOperator(
            task_id=f'train_varnn_{table}',
            python_callable=run_train_varnn_task,
            op_args=[table],
        )
        varnn_tasks.append(task)

    trigger_load_to_visualize = TriggerDagRunOperator(
        task_id="trigger_loading_to_visualize",
        trigger_dag_id="loading_to_visualize",
        wait_for_completion=False,
        reset_dag_run=True,
        execution_date="{{ ds }}",
    )

    end = EmptyOperator(task_id='end')

    start >> srvar_tasks[0]
    for i in range(1, len(srvar_tasks)):
        srvar_tasks[i - 1] >> srvar_tasks[i]

    srvar_tasks[-1] >> varnn_tasks[0]
    for i in range(1, len(varnn_tasks)):
        varnn_tasks[i - 1] >> varnn_tasks[i]

    varnn_tasks[-1] >> trigger_load_to_visualize >> end