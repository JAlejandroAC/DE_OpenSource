"""
F1 Data Loading DAG
===================
Loads cleaned F1 data from MinIO silver layer into PostgreSQL database.

Tasks:
1. Load all F1 datasets to PostgreSQL

Tables created in schema 'f1_data':
- sessions
- drivers
- results (from session_result)
- starting_grid
- weather

Data flow: Silver Layer → PostgreSQL

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
    dag_id="f1_03_load",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Load F1 data from Silver Layer to PostgreSQL",
    tags=["f1", "load", "postgresql", "warehouse"],
    schedule_interval=None,  # Manual trigger or triggered by cleaning DAG
)
def f1_load_pipeline():
    """
    F1 Data Loading Pipeline
    
    This DAG loads cleaned F1 data from the MinIO silver layer into a
    PostgreSQL data warehouse. Data is organized in the 'f1_data' schema
    with properly typed tables.
    
    Target Database: postgres (database container)
    Target Schema: f1_data
    Load Strategy: Replace (can be changed to append or upsert)
    """
    
    # Load all F1 datasets to PostgreSQL
    load_to_sql = DockerOperator(
        task_id="load_all_f1_to_postgresql",
        image="custom-python:latest",
        api_version="auto",
        auto_remove=True,
        network_mode="data_platform_net",
        command="python /app/scripts/load_f1_to_sql.py",
        environment={
            "MINIO_HOST": "minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
            "DWH_USER": "aleDWH",
            "DWH_PASSWORD": "pruebaale1234",
            "DWH_HOST": "database-postgres-1",  # Database container name
            "DWH_PORT": "5432",
            "DWH_DB": "f1_datawarehouse",
        },
        mount_tmp_dir=False,
    )

f1_load_pipeline()
