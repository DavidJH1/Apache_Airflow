#%% loading imports
import os
import sys
import pandas as pd
from pathlib import Path
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from dotenv import load_dotenv
from piplines.weather_api import get_snowflake_connection
from datetime import datetime, timedelta, timezone
from snowflake.connector.pandas_tools import write_pandas

#%%
#-----------------------------------------------
# ssh tunneling function
#-----------------------------------------------
def create_ssh_tunnel():
    
    load_dotenv()

    # get ssh credentials
    SSH_HOST = os.getenv("SSH_HOST")
    SSH_PORT = int(os.getenv("SSH_PORT"))
    SSH_USER = os.getenv("SSH_USER")
    SSH_PASSWORD = os.getenv("SSH_PASSWORD")
    
    # get mongo host and port
    MONGO_HOST=os.getenv("MONGO_HOST")
    MONGO_PORT=int(os.getenv("MONGO_PORT"))
    
    # local bind
    LOCAL_BIND_HOST = "127.0.0.1"
    LOCAL_BIND_PORT = 0
    
    # Defining the tunnel
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username = SSH_USER,
        ssh_password = SSH_PASSWORD,
        remote_bind_address=(MONGO_HOST, MONGO_PORT),
        local_bind_address=(LOCAL_BIND_HOST, LOCAL_BIND_PORT)
    )
    # start the tunnel
    tunnel.start()
    return tunnel

#%%
#-----------------------------------------
# get the mongodb client
#-----------------------------------------
def get_mongo_client(tunnel):
    
    # define the mongo info
    MONGO_DB=os.getenv("MONGO_DB")
    MONGO_USER=os.getenv("MONGO_USER")
    MONGO_PASSWORD=os.getenv("MONGO_PASSWORD")

    # write the uri
    uri = f"mongodb://127.0.0.1:{tunnel.local_bind_port}/{MONGO_DB}?directConnection=true"

    # use the user name and password for the mongo database
    if MONGO_USER and MONGO_PASSWORD:
        client = MongoClient(uri, username=MONGO_USER, password=MONGO_PASSWORD, serverSelectionTimeoutMS=5000)
    else:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client

#%%
#------------------------------------------------
# pull from mongo and format it into a dataframe
#------------------------------------------------
def pull_from_mongo(client, pull_date):
    db_name = os.getenv("MONGO_DB")
    col_name = os.getenv("MONGO_COLLECTION")

    # define the collect to query
    db = client[db_name]
    col = db[col_name]

    # what information we want to keep 1 = include, 0 = exclude
    PROJECTION = {
        "_id": 0,
        "symbol": 1, 
        "datetime": 1,
        "open": 1, 
        "high": 1, 
        "low": 1, 
        "close": 1,
        "volume": 1
    }

    # just regex match the date string, since all dates are strings
    cursor = col.find({"datetime": {"$regex": f"^{pull_date}"}}, PROJECTION)

    # make a list of the docs, if empty just return an empty df
    docs = list(cursor)

    if not docs:
        return pd.DataFrame(columns=["symbol","datetime","open","high","low","close","volume"])
    
    # transform into a pandas dataframe
    df = pd.DataFrame(docs)

    df["symbol"] = df["symbol"].astype('string')
    df['datetime'] = pd.to_datetime(df['datetime'])

    return df


#%%
#-------------------------------------------------
# Upload data into snowflake
#-------------------------------------------------
def stocks_to_snowflake(data: pd.DataFrame, pull_date: str, table: str ="HANSEND_STOCK"):
    
    # define staging table name
    temp_stage_tbl = f"{table}_STAGE_TEMP"
    cols_upper = [c.upper() for c in data.columns]

    # building pieces for the merge command
    non_key = [c for c in cols_upper if c not in ("DATETIME", "SYMBOL")]
    set_clause = ", ".join([f"tgt.{c}=src.{c}" for c in non_key])
    insert_cols = ", ".join(cols_upper)
    insert_vals = ", ".join([f"src.{c}" for c in cols_upper])

    # open the connection and upload the data
    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET TIMEZONE = 'UTC';")
            cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_stage_tbl} LIKE {table}")

            ok, _, nrows, _ = write_pandas(
				conn,
				data,
				table_name=temp_stage_tbl,
				quote_identifiers=False,
				use_logical_type=True
			)
            if not ok:
                print(f"Failed to upload for {pull_date}")
                return
			
            merge_sql = f"""
				MERGE INTO {table} AS tgt
				USING {temp_stage_tbl} AS src
				ON tgt.DATETIME = src.DATETIME AND tgt.SYMBOL = src.SYMBOL
				WHEN MATCHED THEN UPDATE SET {set_clause}
				WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
			"""
        with conn.cursor() as cur:
            cur.execute(merge_sql)
	
    print(f"Stock data uploaded into {table} for {pull_date}")
		        
#-------------------------------------------------
# Close the tunnel connection
#-------------------------------------------------
def close_tunnel(tunnel):
    tunnel.close()

#%%
#-------------------------------------------------
# Wrapper for all functions
#-------------------------------------------------
def upload_stock_data(pull_date):
    tunnel = create_ssh_tunnel()
    client = get_mongo_client(tunnel)
    data = pull_from_mongo(client, pull_date)
    stocks_to_snowflake(data, pull_date)
    close_tunnel(tunnel)
    return

# %%
