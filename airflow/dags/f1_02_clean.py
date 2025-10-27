"""
F1 Data Cleaning DAG
====================
Cleans F1 data from MinIO bronze layer and stores in silver layer.

Tasks:
1. Clean all F1 datasets (sessions, drivers, results, starting_grid, weather)

Cleaning operations:
- Remove duplicates
- Handle missing values
- Convert data types
- Sort and organize data
- Apply dataset-specific transformations

Data flow: Bronze Layer → Silver Layer

Author: Data Engineering Team
Date: 2025-01-27
"""

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

@dag(
    dag_id="f1_02_clean",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Clean F1 data from Bronze to Silver Layer",
    tags=["f1", "clean", "silver", "transformation"],
    schedule_interval=None,  # Manual trigger or triggered by extraction DAG
)
def f1_clean_pipeline():
    """
    F1 Data Cleaning Pipeline
    
    This DAG processes raw F1 data from the bronze layer, applies cleaning
    and transformation rules, and stores the cleaned data in the silver layer.
    
    Cleaning operations include:
    - Deduplication
    - Missing value handling
    - Data type conversions
    - Sorting and organization
    - Dataset-specific business logic
    """
    
    # Clean all F1 datasets
    clean_all_data = DockerOperator(
        task_id="clean_all_f1_datasets",
        image="custom-python:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="data_platform_net",
        command="python /app/scripts/clean_all_f1_data.py",
        environment={
            "MINIO_HOST": "minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
        },
        mount_tmp_dir=False,
    )

f1_clean_pipeline()
