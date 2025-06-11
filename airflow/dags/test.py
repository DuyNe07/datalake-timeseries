from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_task():
    print("✅ This DAG ran as scheduled!")

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="test_scheduler_dag",
    default_args=default_args,
    description="Test DAG for Airflow scheduler",
    schedule_interval="*/1 * * * *",  # Every 10 minutes
    start_date=datetime(2025, 6, 5),
    catchup=False,  # Only run future runs
    tags=["test"],
) as dag:
    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    start >> end