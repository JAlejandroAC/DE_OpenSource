from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
# from f1_driver_test import extract_f1_sessions
from urllib.request import urlopen
import pandas as pd
import json
from datetime import datetime
import os
from minio import Minio
import io 
from include.utils import minio_client, f1_minio_dict, default_args
# import logging

# logger = logging.getLogger(__name__)


@dag(
    dag_id="f1_sessions_etl",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Extracts F1 session data from OpenF1 API and stores it in Minio",
    tags=["f1", "etl", "minio"],
)

def f1_sessions_etl_migrations():

    @task
    def extract_f1_sessions():
        # logger.info("Starting extraction of F1 sessions")
        # logger.info("Connecting to OpenF1 API")
        response = urlopen('https://api.openf1.org/v1/sessions', timeout=60)
        data = json.loads(response.read().decode('utf-8'))
        # logger.info(f"API returned {len(data)} records")
        df = pd.DataFrame(data)
        print(df)
        csv_df = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            bucket_name='datalake',
            object_name='bronce/sessions/f1_sessions.csv',
            data=io.BytesIO(csv_df),
            length=len(csv_df),
            content_type='text/csv'
        )
        print("Data successfully uploaded to Minio bucket 'datalake' as 'f1_sessions.csv'")
        # logger.info("Data successfully uploaded to Minio bucket 'datalake' as 'f1_sessions.csv'")


    @task
    def extract_f1_drivers():
        # logger.info("Starting extraction of F1 sessions")
        # logger.info("Connecting to OpenF1 API")
        response = urlopen('https://api.openf1.org/v1/drivers')
        data = json.loads(response.read().decode('utf-8'))
        # logger.info(f"API returned {len(data)} records")
        df = pd.DataFrame(data)
        print(df)
        csv_df = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            bucket_name='datalake',
            object_name='bronce/drivers/f1_drivers.csv',
            data=io.BytesIO(csv_df),
            length=len(csv_df),
            content_type='text/csv'
        )
        print("Data successfully uploaded to Minio bucket 'datalake' as 'f1_drivers.csv'")
        # logger.info("Data successfully uploaded to Minio bucket 'datalake' as 'f1_sessions.csv'")


    @task
    def extract_f1_results():
        # logger.info("Starting extraction of F1 sessions")
        # logger.info("Connecting to OpenF1 API")
        response = urlopen('https://api.openf1.org/v1/session_result')
        data = json.loads(response.read().decode('utf-8'))
        # logger.info(f"API returned {len(data)} records")
        df = pd.DataFrame(data)
        print(df)
        csv_df = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            bucket_name='datalake',
            object_name='bronce/session_result/f1_session_result.csv',
            data=io.BytesIO(csv_df),
            length=len(csv_df),
            content_type='text/csv'
        )
        print("Data successfully uploaded to Minio bucket 'datalake' as 'f1_session_result.csv'")
        # logger.info("Data successfully uploaded to Minio bucket 'datalake' as 'f1_sessions.csv'")

    @task
    def extract_f1_starting_grid():
        # logger.info("Starting extraction of F1 sessions")
        # logger.info("Connecting to OpenF1 API")
        response = urlopen('https://api.openf1.org/v1/starting_grid')
        data = json.loads(response.read().decode('utf-8'))
        # logger.info(f"API returned {len(data)} records")
        df = pd.DataFrame(data)
        print(df)
        csv_df = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            bucket_name='datalake',
            object_name='bronce/start_grid/f1_starting_grid.csv',
            data=io.BytesIO(csv_df),
            length=len(csv_df),
            content_type='text/csv'
        )
        print("Data successfully uploaded to Minio bucket 'datalake' as 'f1_starting_grid.csv'")
        # logger.info("Data successfully uploaded to Minio bucket 'datalake' as 'f1_sessions.csv'")

    @task
    def extract_f1_weather():
        # logger.info("Starting extraction of F1 sessions")
        # logger.info("Connecting to OpenF1 API")
        response = urlopen('https://api.openf1.org/v1/weather?meeting_key>=1228')
        data = json.loads(response.read().decode('utf-8'))
        # logger.info(f"API returned {len(data)} records")
        df = pd.DataFrame(data)
        print(df)
        csv_df = df.to_csv(index=False).encode("utf-8")
        minio_client.put_object(
            bucket_name='datalake',
            object_name='bronce/weather/f1_weather.csv',
            data=io.BytesIO(csv_df),
            length=len(csv_df),
            content_type='text/csv'
        )
        print("Data successfully uploaded to Minio bucket 'datalake' as 'f1_weather.csv'")
        # logger.info("Data successfully uploaded to Minio bucket 'datalake' as 'f1_sessions.csv'")
        
    extract_f1_sessions() >>  extract_f1_drivers() >> extract_f1_results() >> extract_f1_starting_grid() >> extract_f1_weather()


f1_sessions_etl_migrations() 