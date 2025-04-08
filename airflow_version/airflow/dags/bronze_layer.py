from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
# from airflow.plugins.utils import write_pandas_df
import logging
from minio import Minio
from io import BytesIO
import json

def write_pandas_df(df, bucket_name, file_path): 
    minio_client = Minio("minio1:9000",access_key="1P3GiKLwK2by9Hik5mCA",secret_key="vy9ArqSajIvjphcrxeDKuVAgaLO9Wub84R3Upo5A",secure=False)
    check_bucket = minio_client.bucket_exists(bucket_name)
    if not check_bucket:
        minio_client.make_bucket(bucket_name)
    else : print(f"Bucket {bucket_name} already exists")


    try :
        csv = df.to_csv(index=False).encode('utf-8')
        minio_client.put_object(
            bucket_name,
            file_path,
            data=BytesIO(csv),
            length=len(csv),
            content_type='application/csv'
        )
        print(f"Write {file_path} to {bucket_name} successfully")
    except Exception as e:
        print(f"Error: {e}")


def process_bronze_product_data(**kwargs):
    file_name = kwargs['file_name']
    saved_name = file_name.split(".")[0]
    bucket_name = "tiki-datalake"
    minio_directory = "bronze"
    local_directory = "/opt/airflow/data/raw_product_files"
    logging.info(f"Start uploading file from {local_directory} to {minio_directory} in Minio")
    try:
        df = pd.read_parquet(f"{local_directory}/{file_name}")
        write_pandas_df(df,bucket_name,f"{minio_directory}/{saved_name}.csv")
        logging.info(f"Successfully upload file from {local_directory} to {minio_directory} in Minio")
        return df.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error when uploading file from local to Minio: {e}")

        return json.dumps([])

def process_bronze_review_data():
    file_name = "full_review_data.csv"
    bucket_name = "tiki-datalake"
    minio_directory = "bronze"
    local_directory = "/opt/airflow/data/review"
    logging.info(f"Start uploading file from {local_directory} to {minio_directory} in Minio")
    try:
        
        df = pd.read_csv(f"{local_directory}/{file_name}")
        write_pandas_df(df,bucket_name,f"{minio_directory}/{file_name}")
        logging.info(f"Successfully upload file from {local_directory} to {minio_directory} in Minio")
        return df.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error when uploading file from local to Minio: {e}")
        return json.dumps([])