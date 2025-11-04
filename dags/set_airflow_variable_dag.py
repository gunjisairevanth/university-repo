from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from typing import Dict

@dag(
    dag_id='get_airflow_variable_dag',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'variables', 'python_operator']
)

def get_airflow_variable_dag():



    def fetch_and_log_variables() -> None:
        """
        Fetch all variables stored in AWS Secrets Manager and log their values.
        """
        # Retrieve all variables from AWS Secrets Manager using Airflow's Variable.get method
        # Variables are prefixed with 'airflow/variables/' in AWS Secrets Manager
        variables_prefix = "airflow"
        all_variables: Dict[str, str] = Variable.get(variables_prefix, deserialize_json=True)
        
        # Log each variable and its value
        for key, value in all_variables.items():
            print(f"Variable Key: {key}, Variable Value: {value}")


    def get_variable():

        variable_value = Variable.get(key="VAR_A")
        print(f"The value of the variable is: {variable_value}")


    fetch_and_log_task = PythonOperator(
        task_id='fetch_and_log_variables',
        python_callable=fetch_and_log_variables, 
        retries=1, 
    )



    get_variable_task = PythonOperator(
        task_id='get_variable_task',
        python_callable=get_variable, 
        retries=1, 
    )


    fetch_and_log_task >> get_variable_task 


get_airflow_variable_dag()