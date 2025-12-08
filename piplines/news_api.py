
#%%
import pandas as pd
import os   
import paramiko
import shutil
import sys
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
#%%
# get the snowflake connector function
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from piplines.weather_api import get_snowflake_connection



#----------------------------------------------------------
# Opens a connection to the sftp server
#----------------------------------------------------------
#%%
def open_sftp_connection():
    """
    opens a connection to the datacove.byui.edu sftp server
    returns paramiko.SFTPClient object
    """
    # load vairables for authentication
    load_dotenv("./.env")

    SFTP_HOST = os.getenv("SFTP_HOST")
    SFTP_PORT = int(os.getenv("SFTP_PORT"))
    SFTP_USER = os.getenv("SFTP_USER")
    SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")

    # start by opening an ssh connection
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # tunnel into server via ssh
    ssh_client.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD)

    # login into the sftp server
    sftp = ssh_client.open_sftp()

    return sftp

#%%
#----------------------------------------------------------
# with the sftp connection pull all files within one date folder
#----------------------------------------------------------
def pull_one_date(sftp_conn, date: str):
    """
    input:
        paramiko sftp client
        date in "YYYY-MM-DD" format
    output: 
        Pandas dataframe
    """

    # get the dir to search
    load_dotenv("./.env")
    SFTP_DIR = os.getenv("SFTP_DIR")

    # get a list of all files in the target dir
    day_dir = f"{SFTP_DIR}/{date}"
    file_list = sftp_conn.listdir(day_dir)

    # landing dir for file from sftp
    landing_dir = Path("last_day")
    os.makedirs(landing_dir, exist_ok=True)

    # initilize final dataframe
    full_day = []

    for file in file_list:
        remote_path = f"{day_dir}/{file}"
        local_path = landing_dir / file
        sftp_conn.get(remote_path, str(local_path))

        # read csv from local copy
        temp_df = pd.read_csv(f"{landing_dir}/{file}")

        # adjust column types and names
        temp_df["datetime"] = pd.to_datetime(temp_df['datetime'], unit="s", utc=True)
        temp_df['datetime'] = temp_df['datetime'].dt.date
        temp_df = temp_df.rename(columns={"related" : "ticker"})

        full_day.append(temp_df)

    # remove the temp day dir
    shutil.rmtree(landing_dir)

    return pd.concat(full_day, ignore_index=True)


#------------------------------------------------------------
# take a dataframe, open a snowflake connection, upload data into snowflake
#------------------------------------------------------------
#%%
def upload_news_data(data: pd.DataFrame, table: str ="HANSEND_NEWS"):

    # build strings for sql merge command
    stage_tbl = f"{table}_STAGE_TMP"
    cols_upper = [c.upper() for c in data.columns]
	
	# 3. build the pieces for the merge sql command
    stage_tbl = f"{table}_STAGE_TMP"
    cols_upper = [c.upper() for c in data.columns]
    non_key = [c for c in cols_upper if c not in ("DATETIME","TICKER","ID")]
    set_clause = ", ".join([f"tgt.{c}=src.{c}" for c in non_key])
    insert_cols = ", ".join(cols_upper)
    insert_vals = ", ".join([f"src.{c}" for c in cols_upper])

    
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_tbl} LIKE {table}")

            ok, _, nrows, _ = write_pandas(
				conn,
				data,
				table_name=stage_tbl,
				quote_identifiers=False,
				use_logical_type=True
			)
			
            merge_sql = f"""
				MERGE INTO {table} AS tgt
				USING {stage_tbl} AS src
				ON tgt.DATETIME = src.DATETIME AND tgt.TICKER = src.TICKER AND tgt.ID = src.ID
				WHEN MATCHED THEN UPDATE SET {set_clause}
				WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
			"""
        with conn.cursor() as cur:
            cur.execute(merge_sql)
	
    print(f"News data uploaded into {table}")




#%%
#---------------------------------------------------------------------
# Wrapping function that runs full news pipeline
#--------------------------------------------------------------------
def run_news_pipeline(pull_date: str):
    sftp_conn = open_sftp_connection()
    one_day = pull_one_date(sftp_conn, pull_date)
    upload_news_data(one_day)
    sftp_conn.close()
    print(f"News for {pull_date} has been uploaded into Snowflake")



#------------------------------------------------------------------
# Function testing
#-----------------------------------------------------------------
#%%
#