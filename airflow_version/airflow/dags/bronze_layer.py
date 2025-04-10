from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
# from airflow.plugins.utils import write_pandas_df
import logging
from minio import Minio
from io import BytesIO
import json
from dotenv import load_dotenv
import os
load_dotenv()

def write_pandas_df(df, bucket_name, file_path): 
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_PRIVATE_KEY = os.getenv("MINIO_PRIVATE_KEY")
    minio_client = Minio("minio1:9000",access_key=MINIO_ACCESS_KEY,secret_key=MINIO_PRIVATE_KEY,secure=False)
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


def process_bronze_product_data():

    all_files = [
        'data_bach-hoa-online.parquet', 'data_balo-va-vali.parquet', 'data_cham-soc-nha-cua.parquet',
        'data_cross-border-hang-quoc-te.parquet', 'data_dien-gia-dung.parquet', 'data_dien-thoai-may-tinh-bang.parquet',
        'data_dien-tu-dien-lanh.parquet', 'data_do-choi-me-be.parquet', 'data_dong-ho-va-trang-suc.parquet',
        'data_giay-dep-nam.parquet', 'data_giay-dep-nu.parquet', 'data_lam-dep-suc-khoe.parquet',
        'data_laptop-may-vi-tinh-linh-kien.parquet', 'data_may-anh.parquet', 'data_ngon.parquet',
        'data_nha-cua-doi-song.parquet', 'data_o-to-xe-may-xe-dap.parquet', 'data_phu-kien-thoi-trang.parquet',
        'data_the-thao-da-ngoai.parquet', 'data_thiet-bi-kts-phu-kien-so.parquet', 'data_thoi-trang-nam.parquet',
        'data_thoi-trang-nu.parquet', 'data_tui-thoi-trang-nam.parquet', 'data_tui-vi-nu.parquet', 
        'data_voucher-dich-vu.parquet'
    ]

    try:
        for file in all_files :
            saved_name = file.split(".")[0]
            bucket_name = "tiki-datalake"
            minio_directory = "bronze"
            local_directory = "/opt/airflow/data/raw_product_files"
            logging.info(f"Start uploading file from {local_directory} to {minio_directory} in Minio")
            df = pd.read_parquet(f"{local_directory}/{file}")
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