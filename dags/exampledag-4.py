"""
Minimal Continuous DAG - 30-second schedule, 30-second execution
"""

from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_for_30_seconds(**context):
    """Task that runs for exactly 30 seconds."""
    print("Starting 30-second task...")
    
    for i in range(30):
        print(f"Second {i+1}/30")
        time.sleep(1)
    
    print("Task completed!")


# DAG definition
dag = DAG(
    'minimal_continuous_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 0,
    },
    description='Minimal continuous DAG',
    schedule="@continuous",
    catchup=False,
    tags=['minimal', 'continuous'],
)

# Single task
task = PythonOperator(
    task_id='run_30_seconds',
    python_callable=run_for_30_seconds,
    dag=dag,
)
