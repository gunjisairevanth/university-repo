from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='xcom_example_dag',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['example', 'xcom']
)
def xcom_example_dag():
  
    @task
    def push_data_to_xcom() -> str:
  
        data = "Hello from XCom!"
        return data

    @task
    def pull_data_from_xcom(received_data: str):

        print(f"Received data from XCom: {received_data}")


    data = push_data_to_xcom()
    pull_data_from_xcom(data)

xcom_example_dag()