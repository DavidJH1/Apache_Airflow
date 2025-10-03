from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id = "hello_airflow",
    start_date = datetime(2025, 1, 1),
    schedule = None,
    catchup=False,
    tags=["smoke-test"],
)

def _hello():
    @task
    def say_hi():
        print("Hello from airflow")
        return "It Works!"

    say_hi()

_hello() 