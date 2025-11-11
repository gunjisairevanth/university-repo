from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def run_for_two_minutes(**kwargs):
    print("Task started... running for 2 minutes")
    time.sleep(120)  # sleep for 120 seconds
    print("Task ended after 2 minutes")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="run_each_task_for_2_minutes",
    default_args=default_args,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="task_1",
        python_callable=run_for_two_minutes,
    )

    task2 = PythonOperator(
        task_id="task_2",
        python_callable=run_for_two_minutes,
    )

    task3 = PythonOperator(
        task_id="task_3",
        python_callable=run_for_two_minutes,
    )

    task1 >> task2 >> task3
