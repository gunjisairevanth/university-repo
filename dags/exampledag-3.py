from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests  # third-party package

# Define default args
default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Define the callable function
def call_public_api(**context):
    from slack_sdk import WebClient
    """
    Example task that calls a third-party API using the 'requests' package
    and pushes the result into XCom.
    """
    url = "https://api.agify.io/?name=revanth"  # simple public API
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code} - {response.text}")

    data = response.json()
    print("Response:", data)

    # Push to XCom
    context['ti'].xcom_push(key='api_response', value=data)

# Define the DAG
with DAG(
    dag_id="third_party_api_with_python_operator",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "api", "requests"],
) as dag:

    api_task = PythonOperator(
        task_id="call_api_task",
        python_callable=call_public_api
    )

    api_task
