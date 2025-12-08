from __future__ import annotations
from datetime import datetime
from airflow import DAG
from custom_components.math_operators import DeferrableMathOperator


with DAG(
    dag_id="deferrable_math_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    do_math = DeferrableMathOperator(
        task_id="do_math_deferrable",
        x=10,
        y=5,
        operation="add",
    )
    do_math