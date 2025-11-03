# dag = DAG(
#     'minimal_continuous_dag',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2024, 1, 1),
#         'retries': 0,
#     },
#     description='Minimal continuous DAG',
#     schedule="@continuous",
#     max_active_runs=1,
#     catchup=False,
#     tags=['minimal', 'continuous'],
# )



"""
Simple Deferrable Sensor - Succeeds on 2nd Try

This sensor uses deferrable mode and succeeds on the second attempt.
Uses the task instance's try number to track attempts.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator


def check_condition(**context) -> bool:
    """
    Check condition that succeeds on 2nd attempt.
    Uses task instance try number to track attempts.
    
    Returns:
        bool: True if condition is met
    """
    # Get try number from task instance
    try_number = context['ti'].try_number
    
    print(f"🔍 Sensor attempt #{try_number}")
    print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}")
    
    if try_number >= 2:
        print("✅ Condition met! Sensor succeeds on 2nd try.")
        return True
    else:
        print("❌ Condition not met. Sensor will defer...")
        return False


def success_task(**context):
    """Task that runs after sensor succeeds."""
    print("🎉 Success! Sensor succeeded on 2nd attempt.")
    print("📊 Used task instance try number for tracking.")


# DAG definition
dag = DAG(
    'simple_deferrable_sensor',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 0,
    },
    schedule="@continuous",
    max_active_runs=1,
    description='Simple deferrable sensor that succeeds on 2nd try',
    catchup=False,
    tags=['sensor', 'deferrable', 'simple'],
)

# Deferrable sensor
sensor = PythonSensor(
    task_id='wait_for_condition',
    python_callable=check_condition,
    mode='reschedule',  # Required for deferrable mode
    poke_interval=5,    # Check every 5 seconds
    timeout=60,         # Timeout after 1 minute
    deferrable=True,    # Enable deferrable mode
    dag=dag,
)

# Success task
success = PythonOperator(
    task_id='success_task',
    python_callable=success_task,
    dag=dag,
)

# Dependencies
sensor >> success
