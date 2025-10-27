"""
F1 Master Pipeline DAG
======================
Orchestrates the complete F1 data pipeline from extraction to loading.

Pipeline Stages:
1. Extract: Fetch data from OpenF1 API → Bronze Layer (MinIO)
2. Clean: Transform and clean data → Silver Layer (MinIO)
3. Load: Load cleaned data → PostgreSQL Data Warehouse

This master DAG triggers the individual DAGs in sequence using TriggerDagRunOperator.

Author: Data Engineering Team
Date: 2025-01-27
"""

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

@dag(
    dag_id="f1_00_master_pipeline",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Master pipeline orchestrating F1 data ETL (Extract → Clean → Load)",
    tags=["f1", "master", "pipeline", "etl"],
    schedule_interval="0 6 * * 0",  # Run every Sunday at 6 AM (weekly)
)
def f1_master_pipeline():
    """
    F1 Complete ETL Pipeline
    
    This master DAG orchestrates the complete F1 data pipeline:
    
    1. **Extract Stage**: Fetches fresh data from OpenF1 API
       - Sessions, Drivers, Results, Starting Grid, Weather
       - Stores raw data in MinIO bronze layer
       
    2. **Clean Stage**: Processes and cleans the raw data
       - Removes duplicates
       - Handles missing values
       - Applies transformations
       - Stores cleaned data in MinIO silver layer
       
    3. **Load Stage**: Loads cleaned data into PostgreSQL
       - Creates/updates tables in f1_data schema
       - Ready for analytics and visualization
    
    Each stage is a separate DAG for modularity and independent execution.
    """
    
    # Stage 1: Extract (Bronze Layer)
    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract_stage",
        trigger_dag_id="f1_01_extract",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )
    
    # Stage 2: Clean (Silver Layer)
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_clean_stage",
        trigger_dag_id="f1_02_clean",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )
    
    # Stage 3: Load (PostgreSQL)
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_stage",
        trigger_dag_id="f1_03_load",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )
    
    # Pipeline flow
    trigger_extract >> trigger_clean >> trigger_load

f1_master_pipeline()
