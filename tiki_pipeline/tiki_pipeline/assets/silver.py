from dagster import AssetExecutionContext,StaticPartitionsDefinition,asset,Output,AssetIn
from minio import Minio
import findspark
findspark.init()
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from contextlib import contextmanager
import os
import re
import numpy as np

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
    

def remove_emoji(txt):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', txt)



def preprocess_text(s: str) : 
    if s == None or s is np.nan : 
        return ''
    s = s.lower()
    s = s.strip()
    s = re.sub(r'[\n\t\r]', ' ', s)
    s = remove_emoji(s)
    s = re.sub(r'http\S+','',s)
    html=re.compile(r'<.*?>') 
    
    s = html.sub(r'',s)

    s = re.sub(r"[\(\)\*\!\@\#\$\%\^]","",s)
    return s

    

@asset(
    description="Cleanse, Transform product data from bronze layer",
    partitions_def= StaticPartitionsDefinition(['data_bach-hoa-online.parquet',
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
    group_name="silver",
    key_prefix=["silver","product"],
    ins={
        "bronze_product_data" : AssetIn(key_prefix=["bronze","product"])
    }
)
def silver_product_data(context: AssetExecutionContext, bronze_product_data: pd.DataFrame) : 
    partition_str = context.partition_key
    
    file_name = partition_str.split(".")[0]

    context.log.info(f"Start transformation steps for {file_name} data from Bronze Layer")

    bucket_name = "tiki-datalake"
    with initialize_spark() as spark: 
        df = spark.read.format('parquet').load(f'../data/staging/bronze/{partition_str}')

        selected_df = df.select('id','sku','name','url_key','url_path','availability','visible_impression_info.amplitude.is_imported','visible_impression_info.amplitude.is_authentic','visible_impression_info.amplitude.tiki_verified','impression_info.metadata.is_tikinow','impression_info.metadata.is_tikipro','impression_info.metadata.is_ad','inventory_status','is_visible','visible_impression_info.amplitude.origin','freegift_items','shippable'
                        ,'seller_id','visible_impression_info.amplitude.seller_type','impression_info.metadata.is_official_store','impression_info.metadata.is_trusted_store'
                        ,'primary_category_path','impression_info.metadata.category','visible_impression_info.amplitude.primary_category_name'
                        ,'visible_impression_info.amplitude.brand_name','visible_impression_info.amplitude.is_top_brand'
                        ,'visible_impression_info.amplitude.is_flash_deal','visible_impression_info.amplitude.is_freeship_xtra','visible_impression_info.amplitude.is_gift_available','bundle_deal'
                        ,'price','list_price','discount','discount_rate','visible_impression_info.amplitude.product_rating','rating_average','review_count','favourite_count','original_price','visible_impression_info.amplitude.all_time_quantity_sold','visible_impression_info.amplitude.number_of_reviews','visible_impression_info.amplitude.search_rank')
        

        selected_df = selected_df.dropDuplicates(['id','sku','name','url_key','url_path'])
        selected_df = selected_df.na.fill({'availability':0})
        selected_df = selected_df.withColumn('is_tikinow',sf.col('is_tikinow')[0])
        selected_df = selected_df.withColumn('is_tikipro',sf.col('is_tikipro')[0])
        selected_df = selected_df.withColumn('is_ad',sf.col('is_ad')[0])
        selected_df = selected_df.withColumn('inventory_status',sf.lit(sf.col('availability')))
        selected_df = selected_df.na.fill({'origin':'Không rõ nguồn gốc'})
        selected_df = selected_df.withColumn('freegift_items',sf.lit(sf.size(sf.col('freegift_items'))))
        selected_df = selected_df.withColumn('is_official_store',sf.when(sf.col('seller_type') == 'OFFICIAL_STORE',1).otherwise(0))
        selected_df = selected_df.withColumn('is_trusted_store',sf.when(sf.col('seller_type') == 'TRUSTED_STORE',1).otherwise(0))
        selected_df = selected_df.withColumn('category',sf.col('category')[0])
        selected_df = selected_df.withColumn('list_price',sf.col('price') - sf.col('discount'))

        os.makedirs("../data/stagin/silver",exist_ok=True)
        selected_df.write.mode("overwrite").parquet(f"../data/staging/silver/{file_name}")
        context.log.info(f"Writing {file_name} to ../data/staging/silver")

        write_to_minio(selected_df,bucket_name,f"silver/{file_name}")

        context.log.info(f"Write {file_name} to {bucket_name} successfully")

        context.log.info(f"Finish all transformation steps for {file_name} data from Bronze Layer")

        pandas_df = selected_df.toPandas()

    return Output(
        value=pandas_df,
        metadata={
            "table_name": file_name,
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    )

@asset(
    group_name="silver",
    key_prefix=["silver","review"],
    ins={
        "bronze_review_data" : AssetIn(key_prefix=["bronze","review"])
    }
)
def silver_review_data(context: AssetExecutionContext, bronze_review_data: pd.DataFrame):

    bucket_name = "tiki-datalake"
    file_path = "silver/review.csv"
    context.log.info(f"Start transformation steps for review data from Bronze Layer")

    pandas_df = bronze_review_data
    pandas_df.drop_duplicates(subset=['id','product_id','seller_id'],keep='first',inplace=True)
    pandas_df.dropna(subset=['id','product_id','seller_id'],inplace=True)
    pandas_df.fillna({'content' : ''},inplace=True)
    pandas_df['seller_id'] = pandas_df['seller_id'].astype(int)
    pandas_df.drop(['seller_name','customer_id'],axis=1,inplace=True)

    pandas_df['content'] = pandas_df['content'].apply(lambda x : preprocess_text(x))

    
    write_pandas_df(pandas_df,bucket_name,file_path)

    return Output(
        value=pandas_df,
        metadata={
            "table_name": "review_data",
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    ) 
    