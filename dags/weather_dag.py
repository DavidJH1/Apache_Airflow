from airflow.decorators import dag, task
from datetime import datetime, timedelta

#%%
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # parent of dags/ = project root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

#%%
from piplines.weather_api import upload_weather_data

#%%
@dag(
    dag_id="Upload_weather_data_daily",
    start_date=datetime(2024, 9, 1),
    schedule="15 6 * * *",
    catchup=True,
    max_active_runs=1,
    tags = ["weather", "snowflake"],
)

def weather_loader():
    @task
    def load_for_interval():
        from airflow.decorators import get_current_context
        ctx = get_current_context()

        start_date = ctx["data_interval_start"]
        end_date = ctx["data_interval_end"]

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = (end_date - timedelta(seconds=1)).strftime("%Y-%m-%d")

        upload_weather_data(start_str, end_str, table="HANSEND_WEATHER")
    
    load_for_interval()

dag = weather_loader()

#%%