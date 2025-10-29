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
from piplines.stocks_api import upload_stock_data

# %%
@dag(
    dag_id="Upload_stock_data_daily",
    start_date=datetime(2025, 10, 25),
    schedule="@daily",  
    catchup=True,
    max_active_runs=1,
    tags = ["stocks", "snowflake"],
)

def stock_loader():
    @task()
    def run_for_day(ds: str):
        from piplines.weather_api import get_snowflake_connection
        from piplines.stocks_api import upload_stock_data
        upload_stock_data(ds)
    run_for_day(ds= "{{ ds }}")

dag = stock_loader()
