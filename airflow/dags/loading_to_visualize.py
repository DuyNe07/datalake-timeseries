from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Add your script directory to Python path
sys.path.append("/src/airflow")

# Import the main() functions from each file
from LoadToVisualize import main as run_load_to_visualize
from LoadEvaluation import main as run_load_evaluation

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'loading_to_visualize',
    default_args=default_args,
    description='Load the data from model\'s prediction and visualize it',
    schedule = None,
    start_date=datetime(2025, 5, 15),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    table_names = ['indices', 'macro', 'usbonds']
    loading_tasks = []

    for table in table_names:
        task_predict = PythonOperator(
            task_id=f'load_to_visualize_{table}_predict',
            python_callable=run_load_to_visualize,
            op_args=[table, 'predict'],
        )
        loading_tasks.append(task_predict)

        task_future = PythonOperator(
            task_id=f'load_to_visualize_{table}_future',
            python_callable=run_load_to_visualize,
            op_args=[table, 'future'],
        )
        loading_tasks.append(task_future)

        task_eval = PythonOperator(
            task_id=f'load_to_evaluation_{table}',
            python_callable=run_load_evaluation,
            op_args=[table],
        )
        loading_tasks.append(task_eval)

    end = EmptyOperator(task_id='end')

    start >> loading_tasks[0]
    for i in range(1, len(loading_tasks)):
        loading_tasks[i - 1] >> loading_tasks[i]

    loading_tasks[-1] >> end