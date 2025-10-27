from minio import Minio
import pandas as pd
import os

f1_data_dict = {
    "results": "session_result",
    "sessions": "sessions",
    "drivers": "drivers",
    "starting_grid": "starting_grid",
    "weather": "weather"
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pd.Timedelta(minutes=1),
}

# Detect if running inside Docker
IS_DOCKER = os.path.exists("/.dockerenv")

# ---- SQL ----
sql_db_user = os.getenv("POSTGRES_USER", "airflow")
sql_db_password = os.getenv("POSTGRES_PASSWORD", "airflow")
sql_db_host = "postgres" if IS_DOCKER else "localhost"
sql_db_port = os.getenv("POSTGRES_PORT", "5432")
sql_db_name = os.getenv("POSTGRES_DB", "airflow")
sql_f1_schema = "f1_data"

# ---- MinIO ----
minio_host = "minio:9000" if IS_DOCKER else "localhost:9000"
minio_client = Minio(
    minio_host,
    access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    secure=False,
)