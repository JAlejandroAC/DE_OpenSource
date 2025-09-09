# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
import pandas as pd
import json
from datetime import datetime
import os
from minio import Minio
import io 
from io import BytesIO
# import logging

# logger = logging.getLogger(__name__)


minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = 'datalake'
prefix = 'bronce/sessions/'

dataframes = {}

try:
    # List all objects in the bucket with the specified prefix
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

    for obj in objects:
        if obj.object_name.endswith('.csv'):
            # Get the object from MinIO
            response = minio_client.get_object(bucket_name, obj.object_name)

            # Read the CSV data from the response stream into a DataFrame
            df = pd.read_csv(BytesIO(response.read()))

            # Store the DataFrame using the filename as the key
            file_name = obj.object_name.split('/')[-1]
            minio_client[file_name] = df
            print(f"Successfully read {file_name} into a DataFrame.")

except Exception as err:
    print(f"An error occurred: {err}")
finally:
    if 'response' in locals():
        response.close()
        response.release_conn()

# Now you can access each DataFrame like this:
# df_file1 = dataframes['file1.csv']