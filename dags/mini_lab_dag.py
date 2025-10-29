from airflow.decorators import dag, task
from datetime import datetime, timedelta

today = datetime.today()
days_since_saturday = (today.weekday() - 5) % 7  # 5 = Saturday
last_saturday = today - timedelta(days=days_since_saturday)
start_date = datetime(last_saturday.year, last_saturday.month, last_saturday.day)

@dag(
    dag_id="catchup_demo_dag",
    start_date=start_date,
    schedule="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["demo", "catchup", "itm327"]
)
def catchup_demo_dag():

    @task()
    def print_date_window(data_interval_start=None, data_interval_end=None):
        print(f"Run window: {data_interval_start.date()} → {data_interval_end.date()}")

    print_date_window()

catchup_demo_dag()