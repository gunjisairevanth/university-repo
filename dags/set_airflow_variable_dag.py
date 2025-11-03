from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

@dag(
    dag_id='set_airflow_variable_dag',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'variables', 'python_operator']
)

def set_airflow_variable_dag():

    def set_variable():
  
        variable_key = "test"  
        variable_value = "test" 

        Variable.set(key=variable_key, value=variable_value)
        print(f"Variable '{variable_key}' set with value '{variable_value}'.")

    def list_airflow_variables():
        """
        Task to list all Airflow variables and print their keys and values.
        """
        # Fetch all variables stored in Airflow
        variables = Variable.get_all()
        
        # Print each variable's key and value
        for key, value in variables.items():
            print(f"Variable Key: {key}, Variable Value: {value}")

    set_variable_task = PythonOperator(
        task_id='set_variable_task',
        python_callable=set_variable, 
        retries=1, 
    )

    list_variable_task = PythonOperator(
        task_id='set_variable_task',
        python_callable=list_airflow_variables, 
        retries=1, 
    )

    set_variable_task >> list_variable_task


set_airflow_variable_dag()