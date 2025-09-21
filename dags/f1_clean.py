# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
import pandas as pd
from minio import Minio
from io import BytesIO
from include.utils import  f1_data_dict,sql_f1_schema,sqlengine
# import logging

# logger = logging.getLogger(__name__)


# MinIO connection details
bucket_name = 'datalake'

# change localhost to minio if running inside docker
minio_client = Minio(
    "localhost:9000",  #use "minio:9000" if inside docker
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)


for key, value in f1_data_dict.items():
    prefix = f'bronce/{value}/'
    print(f"Processing : {prefix}")
    print(repr(prefix))
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in objects:
        print(obj.object_name)
        if obj.object_name.endswith('.csv'):
            response = minio_client.get_object(bucket_name, obj.object_name)
            df = pd.read_csv(BytesIO(response.read()))
            # Write to SQL table named after the key
            df.to_sql(key, sqlengine, schema=sql_f1_schema, if_exists='replace', index=False)
            print(f"Data from {value} written to table {key} in schema {sql_f1_schema}")
response.close()
response.release_conn()
    
