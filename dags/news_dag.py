#%%
from airflow.decorators import dag, task
#%%
import sys
from datetime import datetime, timedelta
from pathlib import Path

# %%
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from piplines.weather_api import get_snowflake_connection
from piplines.news_api import run_news_pipeline

# %%
@dag(
    dag_id="Upload_news_data_daily",
    start_date=datetime(2025, 11, 7),
    schedule="@daily",  
    catchup=True,
    max_active_runs=1,
    tags = ["stocks", "snowflake"],
)

def news_loader():
    @task()
    def run_one_day(ds: str):
        run_news_pipeline(ds)
    run_one_day(ds= "{{ ds }}")

dag = news_loader()