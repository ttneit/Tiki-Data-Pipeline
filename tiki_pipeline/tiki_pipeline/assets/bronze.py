from dagster import AssetExecutionContext,StaticPartitionsDefinition,asset,Output
from minio import Minio
import findspark
findspark.init()
import pandas as pd
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from contextlib import contextmanager
import os
from io import BytesIO

import json

with open('credentials.json', 'r') as f:
    data = json.load(f)
minio_access_key = data['accessKey']
minio_private_key = data['secretKey']
@contextmanager
def initialize_spark():
    try:
        spark = SparkSession.builder.appName("Tiki Pipeline") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.cores.max", "1") \
            .config("spark.executor.cores", "1") \
            .config("fs.s3a.access.key",minio_access_key) \
            .config("fs.s3a.secret.key",minio_private_key) \
            .config("fs.s3a.endpoint","http://localhost:9000") \
            .config("fs.s3a.path.style.access","true") \
            .config("fs.s3a.connection.ssl.enabled","false") \
            .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        yield spark
    finally:
        spark.stop()

def write_to_minio(df,bucket_name,file_name):
    minio_client = Minio("localhost:9000",access_key=minio_access_key,secret_key=minio_private_key,secure=False)

    check_bucket = minio_client.bucket_exists(bucket_name)
    if not check_bucket:
        minio_client.make_bucket(bucket_name)
    else : print(f"Bucket {bucket_name} already exists")


    try :
        df.write.mode("overwrite").parquet(f"s3a://{bucket_name}/{file_name}")
        print(f"Write {file_name} to {bucket_name} successfully")
    except Exception as e:
        print(f"Error: {e}")

def write_pandas_df(df, bucket_name, file_path): 
    minio_client = Minio("localhost:9000",access_key=minio_access_key,secret_key=minio_private_key,secure=False)
    check_bucket = minio_client.bucket_exists(bucket_name)
    if not check_bucket:
        minio_client.make_bucket(bucket_name)
    else : print(f"Bucket {bucket_name} already exists")


    try :
        csv = df.to_csv().encode('utf-8')
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
    
    

@asset(
    partitions_def=StaticPartitionsDefinition(partition_keys=['data_bach-hoa-online.parquet',
        'data_balo-va-vali.parquet',
        'data_cham-soc-nha-cua.parquet',
        'data_cross-border-hang-quoc-te.parquet',
        'data_dien-gia-dung.parquet',
        'data_dien-thoai-may-tinh-bang.parquet',
        'data_dien-tu-dien-lanh.parquet',
        'data_do-choi-me-be.parquet',
        'data_dong-ho-va-trang-suc.parquet',
        'data_giay-dep-nam.parquet',
        'data_giay-dep-nu.parquet',
        'data_lam-dep-suc-khoe.parquet',
        'data_laptop-may-vi-tinh-linh-kien.parquet',
        'data_may-anh.parquet',
        'data_ngon.parquet',
        'data_nha-cua-doi-song.parquet',
        'data_o-to-xe-may-xe-dap.parquet',
        'data_phu-kien-thoi-trang.parquet',
        'data_the-thao-da-ngoai.parquet',
        'data_thiet-bi-kts-phu-kien-so.parquet',
        'data_thoi-trang-nam.parquet',
        'data_thoi-trang-nu.parquet',
        'data_tui-thoi-trang-nam.parquet',
        'data_tui-vi-nu.parquet',
        'data_voucher-dich-vu.parquet']),
        description="Ingest product data from local data source to DataLake in Minio",
        group_name="bronze",
        key_prefix=["bronze","product"]
)
def bronze_product_data(context: AssetExecutionContext) :
    partition_str = context.partition_key
    context.log.info(f"Start ingesting data about {partition_str} from local data source to Minio")
    bucket_name = "tiki-datalake"
    file_name = partition_str.split(".")[0]
    with initialize_spark() as spark:
        df = spark.read.format('parquet').load(f"../data/raw_product_files/{partition_str}")

        context.log.info(f"Write {partition_str} to staging file at local")
        os.makedirs("../data/staging/bronze",exist_ok=True)
        df.write.mode("overwrite").parquet(f"../data/staging/bronze/{partition_str}")

        write_to_minio(df,bucket_name,f"bronze/{file_name}")
        context.log.info(f"Write {file_name} to {bucket_name} successfully")

        context.log.info(f"Finish ingesting data about {file_name} from local data source to Minio")

    pandas_df = pd.read_parquet(f"../data/staging/bronze/{partition_str}")

    return Output(
        value=pandas_df,
        metadata={
            "table_name" : file_name,
            "row_numbers" : pandas_df.shape[0],
            "column_numbers": pandas_df.shape[1]
        }
    )


@asset(
    description="Ingest review data from local data source to DataLake in Minio",
    group_name="bronze",
    key_prefix=["bronze","review"]
)
def bronze_review_data(context: AssetExecutionContext):
    context.log.info(f"Start ingesting data about review products from local data source to Minio")
    review_df = pd.read_csv("../data/review/full_review_data.csv")
    context.log.info(f"Finish ingesting data about review products from local data source to Minio")

    bucket_name = "tiki-datalake"
    file_path = "bronze/review.csv"
    write_pandas_df(review_df,bucket_name,file_path)

    return Output(
        value=review_df,
        metadata={
            "table_name" : "review_data",
            "row_numbers" : review_df.shape[0],
            "column_numbers": review_df.shape[1]
        }
    )