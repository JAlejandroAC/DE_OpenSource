# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from minio import Minio
from io import BytesIO
import numpy as np
from sqlalchemy import create_engine
# import logging

# logger = logging.getLogger(__name__)

# change localhost to minio if running inside docker
minio_client = Minio(
    "localhost:9000",  #use "minio:9000" if inside docker
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# PostgreSQL connection details
db_user = 'airflow'
db_password = 'airflow'
db_host = 'localhost'
db_port = '5432'
db_name = 'airflow'
f1_schema = 'f1_data'

bucket_name = 'datalake'


prefix = 'bronce/session_result/'

dataframes = {}

try:
    # List all objects in the bucket with the specified prefix
    print(f"Connecting to MinIO and listing objects in bucket '{bucket_name}' with prefix '{prefix}'")
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            # Get the object from MinIO
            response = minio_client.get_object(bucket_name, obj.object_name)

            # Read the CSV data from the response stream into a DataFrame
            df = pd.read_csv(BytesIO(response.read()))


except Exception as err:
    print(f"An error occurred: {err}")
finally:
    if 'response' in locals():
        response.close()
        response.release_conn()


engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')  

# Write df_clean to PostgreSQL table 'session_results_clean'
df.to_sql('session_results', engine,schema = f1_schema  ,if_exists='replace', index=False)


engine.dispose() # Closes all connections in the pool
