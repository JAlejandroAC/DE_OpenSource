# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
import pandas as pd
import json
from datetime import datetime
import os
from minio import Minio
import io 
from io import BytesIO
import numpy as np
# import logging

# logger = logging.getLogger(__name__)


minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = 'datalake'
prefix = 'bronce/session_result/'

# ---- helper: list CSV objects ----
def list_csv_objects(bucket_name, prefix=""):
    """Yields object names ending with .csv"""
    for obj in minio_client.list_objects(bucket_name, prefix=prefix, recursive=True):
        if obj.object_name.lower().endswith(".csv"):
            yield obj.object_name


# ---- helper: read object into pandas DataFrame ----
def read_csv_from_minio(bucket_name, object_name, **pd_read_csv_kwargs):
    """Return a pandas DataFrame for the CSV object in MinIO"""
    resp = minio_client.get_object(bucket_name, object_name)
    try:
        data = resp.read()  # bytes
        bio = io.BytesIO(data)
        df = pd.read_csv(bio, **pd_read_csv_kwargs)
        return df
    finally:
        resp.close()
        resp.release_conn()

# ---- orchestrator: process bucket/prefix ----
def process_bucket_csvs(bucket_name, prefix="",):
    for obj_name in list_csv_objects(bucket_name, prefix):
        print("Processing:", obj_name)
        df = read_csv_from_minio(bucket_name, obj_name)
        return df 
    
df = process_bucket_csvs(bucket_name,prefix)
print(df)
print(df.columns)
# select columns
df_select = df[['meeting_key','session_key','driver_number', 'position','points']]
print(df_select)

df = df['Status'] = np.where(
     df['dns'].between(0, 30, inclusive=False), 
    'DNS', 
     np.where(
        df['dnf'].between(0, 30, inclusive=False), 'Medium', 'Unknown'
     )
)

