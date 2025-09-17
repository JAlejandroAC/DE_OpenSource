from minio import Minio
import pandas as pd


f1_minio_dict = {
    "sessions": "sessions",
    "drivers": "drivers",
    "results": "session_result",
    "starting_grid": "starting_grid",
    "weather": "weather"
}


minio_client = Minio(
    "minio:9000",
    access_key="yvKv2KtQRBCbUuzK3l9H",
    secret_key="2asgmDmzj8uEbS0TVpQeVVKrvCcMrOfCpSV3i5Jb",
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


