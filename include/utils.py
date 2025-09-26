from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
import os

f1_data_dict = {
    "results": "session_result",
    "sessions": "sessions",
    "drivers": "drivers",
    "starting_grid": "starting_grid",
    "weather": "weather"
}


minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pd.Timedelta(minutes=1),
}

# PostgreSQL connection details
sql_db_user = os.getenv('POSTGRES_USER')
sql_db_password = os.getenv('POSTGRES_PASSWORD')
sql_db_host = 'localhost'
sql_db_port = os.getenv('POSTGRES_PORT')
sql_db_name = os.getenv('POSTGRES_DB')
sql_f1_schema = 'f1_data'

sqlengine = create_engine(f'postgresql+psycopg2://{sql_db_user}:{sql_db_password}@{sql_db_host}:{sql_db_port}/{sql_db_name}')  
