# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from minio import Minio
from io import BytesIO
# import logging

# logger = logging.getLogger(__name__)

# change localhost to minio if running inside docker
minio_client = Minio(
    "localhost:9000",  #use "minio:9000" if inside docker
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

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

print(df)
print(df.head())