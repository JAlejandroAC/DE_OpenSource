from minio import Minio

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
