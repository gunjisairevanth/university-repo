from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import numpy as np

def memory_intensive_task():
    """Task that consumes large amounts of memory"""
    # Create large numpy arrays to consume memory
    large_array = np.random.rand(10000, 10000)  # ~800MB
    large_list = [i for i in range(50000000)]   # ~400MB
    
    # Process the data to keep it in memory
    result = np.sum(large_array)
    list_sum = sum(large_list)
    
    print(f"Array sum: {result}")
    print(f"List sum: {list_sum}")
    
    return result

def extreme_memory_task():
    """Task that consumes even more memory"""
    # Create multiple large objects
    arrays = []
    for i in range(5):
        arrays.append(np.random.rand(5000, 5000))  # ~200MB each
    
    # Create large dictionary
    large_dict = {i: f"value_{i}" * 1000 for i in range(1000000)}
    
    print(f"Created {len(arrays)} arrays and dict with {len(large_dict)} items")
    
    return len(arrays)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'memory_intensive_dag',
    default_args=default_args,
    description='A DAG with memory-intensive tasks',
   
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='memory_intensive_task',
        python_callable=memory_intensive_task,
    )
    
    task2 = PythonOperator(
        task_id='extreme_memory_task',
        python_callable=extreme_memory_task,
    )
    
    task1 >> task2
