from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

@dag(
    dag_id='get_airflow_variable_dag',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'variables', 'python_operator']
)

def get_airflow_variable_dag():

    def get_variable():

        variable_value = Variable.get(key="VAR_A")
        print(f"The value of the variable is: {variable_value}")


    get_variable_task = PythonOperator(
        task_id='get_variable_task',
        python_callable=get_variable, 
        retries=1, 
    )


    get_variable_task 


get_airflow_variable_dag()