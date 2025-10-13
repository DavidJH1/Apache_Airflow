from airflow.decorators import dag, task
#%%
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Get the weather_api uploading function
ROOT = Path(__file__).resolve().parents[1]  # parent of dags/ = project root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from piplines.weather_api import upload_weather_data
#%%
# 2-day API lag + 1-day buffer
BUFFER_DAYS = 3

#%%

@dag(
    dag_id="Upload_weather_data_daily",
    start_date=datetime(2024, 9, 1),
    schedule="15 6 * * *",  
    catchup=False,
    max_active_runs=1,
    tags = ["weather", "snowflake"],
)

def weather_loader():
    @task
    def load_for_interval(ds: str):
        
        # ds is rendered by Airflow as 'YYYY-MM-DD' for data_interval_start
        ref_date = datetime.strptime(ds, "%Y-%m-%d").date()
        run_date = ref_date - timedelta(days=BUFFER_DAYS)
        day_str  = run_date.strftime("%Y-%m-%d")

        upload_weather_data(day_str, day_str, table="HANSEND_WEATHER")
    
    load_for_interval(ds="{{ ds }}")

dag = weather_loader()
