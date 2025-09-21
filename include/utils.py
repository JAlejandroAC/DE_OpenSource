from minio import Minio
import pandas as pd
from sqlalchemy import create_engine


f1_data_dict = {
    "results": "session_result",
    "sessions": "sessions",
    "drivers": "drivers",
    "starting_grid": "starting_grid",
    "weather": "weather"
}


minio_client = Minio(
    "minio:9000",
    access_key="yvKv2KtQRBCbUuzK3l9H",
    secret_key="2asgmDmzj8uEbS0TVpQeVVKrvCcMrOfCpSV3i5Jb",
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
sql_db_user = 'airflow'
sql_db_password = 'airflow'
sql_db_host = 'localhost'
sql_db_port = '5432'
sql_db_name = 'airflow'
sql_f1_schema = 'f1_data'

sqlengine = create_engine(f'postgresql+psycopg2://{sql_db_user}:{sql_db_password}@{sql_db_host}:{sql_db_port}/{sql_db_name}')  
