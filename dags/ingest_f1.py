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
# import logging

# logger = logging.getLogger(__name__)


minio_client = Minio(
    "minio:9000",
    access_key="pgjZqyfjEWz6jRAFUkmj",
    secret_key="ekltvkmJFpXWbI6Dc7IfG7A3vZ7vKjo7d5n1YEgg",
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

@dag(
    dag_id="f1_sessions_etl",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Extracts F1 session data from OpenF1 API and stores it in Minio",
    tags=["f1", "etl", "minio"],
)