from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from minio import Minio
from datetime import datetime
from io import BytesIO
from include.utils import  f1_data_dict,default_args, sql_f1_schema, sql_db_user, sql_db_password,  sql_db_name, sql_db_port,sql_db_host, minio_client
import sqlalchemy 
# import logging

# logger = logging.getLogger(__name__)

# MinIO connection details

# change localhost to minio if running inside docker
# minio_client = Minio(
#     "localhost:9000",  #use "minio:9000" if inside docker
#     access_key="minioadmin",
#     secret_key="minioadmin",
#     secure=False
# )
# sql_db_host = 'localhost'


# sqlengine = create_engine(f'postgresql+psycopg2://{sql_db_user}:{sql_db_password}@{sql_db_host}:{sql_db_port}/{sql_db_name}')  
# --- CONFIG ---
bucket_name = 'datalake'
# conn_str = f"postgresql+psycopg2://{sql_db_user}:{sql_db_password}@{sql_db_host}:{sql_db_port}/{sql_db_name}"




@dag(
    dag_id="f1_transform_load",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    description="Transforms F1 data from Minio and loads it into PostgreSQL",
    tags=["f1", "etl", "minio","sql"],
)


def f1_transform_load():
    
    @task
    def transform_and_load():
        print(f"Schema: {sql_f1_schema}")
        print(sqlalchemy.__version__)
        print(pd.__version__)
        postgres_hook = PostgresHook(postgres_conn_id="f1_postgresql")
        engine = postgres_hook.get_sqlalchemy_engine()  
        print("✅ SQLAlchemy engine created successfully!")

        for key, value in f1_data_dict.items():
            prefix = f'bronce/{value}/'
            print(f" Processing prefix: {prefix}")

            objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

            for obj in objects:
                if obj.object_name.endswith('.csv'):

                    # Read CSV from MinIO
                    response = minio_client.get_object(bucket_name, obj.object_name)
                    df = pd.read_csv(BytesIO(response.read()))
                    response.close()
                    response.release_conn()

                    print(f"🧹 Loaded {len(df)} rows from {obj.object_name}")

                    df.to_sql(key, con=engine, if_exists="replace", index=False, schema=sql_f1_schema)

                    print(f"✅ Data from {value} written to table {key} in schema {sql_f1_schema}")

        print("🎯 All F1 data successfully loaded into PostgreSQL!")

    transform_and_load()

f1_transform_load()
