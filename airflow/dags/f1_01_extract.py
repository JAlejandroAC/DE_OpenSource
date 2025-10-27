"""
F1 Data Extraction DAG
======================
Orchestrates the extraction of F1 data from OpenF1 API using python-env container.

Tasks:
1. Extract Sessions data
2. Extract Drivers data
3. Extract Session Results data
4. Extract Starting Grid data
5. Extract Weather data

All tasks run in parallel using DockerOperator with python-env container.
Data is stored in MinIO datalake (bronze layer).

Author: Data Engineering Team
Date: 2025-01-27
"""

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

@dag(
    dag_id="f1_01_extract",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Extract F1 data from OpenF1 API to MinIO (Bronze Layer)",
    tags=["f1", "extract", "bronze", "api"],
    schedule_interval=None,  # Manual trigger only
)
def f1_extract_pipeline():
    """
    F1 Data Extraction Pipeline
    
    This DAG extracts raw data from the OpenF1 API and stores it in the
    MinIO datalake bronze layer. Each extraction task runs independently
    in the python-env container with all required dependencies.
    """
    
    # Common Docker configuration
    docker_config = {
        "image": "custom-python:latest",
        "api_version": "auto",
        "auto_remove": True,
        "network_mode": "data_platform_net",
        "environment": {
            "MINIO_HOST": "minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
        },
        "mount_tmp_dir": False,
    }
    
    # Task 1: Extract Sessions
    extract_sessions = DockerOperator(
        task_id="extract_sessions",
        command="python /app/scripts/extract_f1_sessions.py",
        **docker_config
    )
    
    # Task 2: Extract Drivers
    extract_drivers = DockerOperator(
        task_id="extract_drivers",
        command="python /app/scripts/extract_f1_drivers.py",
        **docker_config
    )
    
    # Task 3: Extract Session Results
    extract_results = DockerOperator(
        task_id="extract_results",
        command="python /app/scripts/extract_f1_results.py",
        **docker_config
    )
    
    # Task 4: Extract Starting Grid
    extract_starting_grid = DockerOperator(
        task_id="extract_starting_grid",
        command="python /app/scripts/extract_f1_starting_grid.py",
        **docker_config
    )
    
    # Task 5: Extract Weather
    extract_weather = DockerOperator(
        task_id="extract_weather",
        command="python /app/scripts/extract_f1_weather.py",
        **docker_config
    )
    
    # All extractions run in parallel (no dependencies)
    # This allows for maximum parallelism and faster execution

f1_extract_pipeline()
