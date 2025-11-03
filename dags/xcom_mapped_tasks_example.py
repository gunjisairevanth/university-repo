from airflow.decorators import dag, task
from datetime import datetime
from typing import List
from airflow.secrets.local_filesystem import LocalFilesystemBackend

@dag(
    dag_id="xcom_mapped_tasks_example",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["xcom", "dynamic task mapping", "example"]
)
def xcom_mapped_tasks_example():

    @task
    def generate_numbers() -> List[int]:

        return [1, 2, 3, 4, 5]

    @task
    def mapped_task(number: int, index: int, **kwargs):

        ti = kwargs["ti"]
        xcom_value = f"Processed number {number} at index {index}"
        ti.xcom_push(key=f"index_{index}", value=xcom_value)
        return xcom_value

    @task
    def process_xcom_entries(numbers: List[int], **kwargs):

        ti = kwargs["ti"]
        results = []
        for index in range(len(numbers)):
            xcom_value = ti.xcom_pull(task_ids="mapped_task", key=f"index_{index}")
            results.append(xcom_value)
        print("Processed XCom entries:", results)

    numbers = generate_numbers()

    mapped_results = mapped_task.expand(
        number=numbers,
        index=list(range(len(numbers))),
    )

    process_xcom_entries(numbers)

xcom_mapped_tasks_example()