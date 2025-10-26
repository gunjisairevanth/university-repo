from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator


class PingApiSensor(BaseSensorOperator):
    """
    Custom Sensor that polls an API endpoint and stops when response is [{"res": false}]
    """

    def __init__(self, endpoint: str, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint

    def poke(self, context):
        try:
            response = requests.get(self.endpoint, timeout=5)
            response.raise_for_status()
            data = response.json()
            self.log.info(f"Response from API: {data}")

            # End sensor if the response is [{"res": false}]
            if isinstance(data, list) and len(data) > 0 and data[0].get("res") is False:
                self.log.info("Condition met: [{'res': false}] → stopping sensor.")
                return True

            self.log.info("Condition not met, will retry later...")
            return False

        except Exception as e:
            self.log.error(f"Error calling endpoint: {e}")
            return False


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="ping_api_sensor_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sensor", "api"],
) as dag:

    wait_for_ping = PingApiSensor(
        task_id="wait_for_ping",
        endpoint="https://68f0b8e20b966ad50033e931.mockapi.io/83198/ping",
        poke_interval=15,  # seconds between retries
        timeout=60 * 5,   # give up after 10 minutes
        mode="reschedule",  # <-- non-blocking mode
    )

    wait_for_ping
