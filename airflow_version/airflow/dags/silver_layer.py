from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
# from plugins.utils import write_pandas_df
import logging
from minio import Minio
from io import BytesIO
import json
import re
import numpy as np
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


def read_file_from_minio(bucket_name, object_name) :
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_PRIVATE_KEY = os.getenv("MINIO_PRIVATE_KEY")
    client = Minio("minio1:9000",access_key=MINIO_ACCESS_KEY,secret_key=MINIO_PRIVATE_KEY,secure=False)

    response = client.get_object(bucket_name, object_name)

    df = pd.read_csv(BytesIO(response.read()))
    return df


def list_files_in_directory(bucket_name, prefix):
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_PRIVATE_KEY = os.getenv("MINIO_PRIVATE_KEY")
    client = Minio("minio1:9000",access_key=MINIO_ACCESS_KEY,secret_key=MINIO_PRIVATE_KEY,secure=False)
    files = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    
    file_list = [obj.object_name for obj in files]
    return file_list

def process_table(df) : 
    selected_df = df[['id','sku','name','url_key','url_path','availability','inventory_status','is_visible','freegift_items','shippable'
                        ,'seller_id'
                        ,'primary_category_path'
                        ,'bundle_deal'
                        ,'price','list_price','discount','discount_rate','rating_average','review_count','favourite_count','original_price']]
    selected_df['is_imported'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['is_imported'])
    selected_df['is_authentic'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['is_authentic'])
    selected_df['tiki_verified'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['tiki_verified'])
    selected_df['is_tikinow'] = df['impression_info'].apply(lambda x: x[0]['metadata']['is_tikinow'])
    selected_df['is_tikipro'] = df['impression_info'].apply(lambda x: x[0]['metadata']['is_tikipro'])
    selected_df['is_ad'] = df['impression_info'].apply(lambda x: x[0]['metadata']['is_ad'])
    selected_df['origin'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['origin'])
    selected_df['seller_type'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['seller_type'])
    selected_df['is_official_store'] = df['impression_info'].apply(lambda x: x[0]['metadata']['is_official_store'])
    selected_df['is_trusted_store'] = df['impression_info'].apply(lambda x: x[0]['metadata']['is_trusted_store'])

    selected_df['category'] = df['impression_info'].apply(lambda x: x[0]['metadata']['category'])
    selected_df['primary_category_name'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['primary_category_name'])
    selected_df['brand_name'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['brand_name'])
    selected_df['is_top_brand'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['is_top_brand'])

    selected_df['is_flash_deal'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['is_flash_deal'])
    selected_df['is_freeship_xtra'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['is_freeship_xtra'])
    selected_df['is_gift_available'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['is_gift_available'])

    selected_df['product_rating'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['product_rating'])
    selected_df['all_time_quantity_sold'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['all_time_quantity_sold'])
    selected_df['number_of_reviews'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['number_of_reviews'])
    selected_df['search_rank'] = df['visible_impression_info'].apply(lambda x: x['amplitude']['search_rank'])


    selected_df.drop_duplicates(subset=['id','sku','name','url_key','url_path'],keep='first',inplace=True,ignore_index=True)
    selected_df['availability'].fillna(value=0,inplace=True)
    selected_df['inventory_status'] = selected_df['availability'].apply(lambda x: x)
    selected_df['origin'].fillna(value='Không rõ nguồn gốc',inplace=True)
    selected_df['freegift_items'] = selected_df['freegift_items'].apply(lambda x: len(x))
    selected_df['is_official_store'] = selected_df['seller_type'].apply(lambda x: 1 if x == 'OFFICIAL_STORE' else 0)
    selected_df['is_trusted_store'] = selected_df['seller_type'].apply(lambda x: 1 if x == 'TRUSTED_STORE' else 0)
    selected_df['list_price'] = selected_df['price'] - selected_df['discount']

    return selected_df

def process_silver_product_data():
    # file_name = kwargs['file_name']
    # saved_name = file_name.split(".")[0]
    # json_records = ti.xcom_pull(task_ids=f"bronze_product_{file_name.replace('.parquet','').replace('-', '_')}")  
    # if not json_records or len(json_records) == 2:
    #     logging.warning("No data received from bronze_product_task.")
    #     return json.dumps([])
    
    # try:
    #     df = pd.read_json(json_records, orient='records')
    # except Exception as e:
    #     logging.error(f"Failed to parse JSON records: {e}")
    #     return json.dumps([])
        
    bucket_name = "tiki-datalake"
    minio_directory = "bronze"
    saved_directory = "silver"
    all_files = list_files_in_directory(bucket_name,f"{minio_directory}.")
    
    try:
        total_df = []
        for file_name in all_files:

            logging.info(f"Start process data in {file_name} from {minio_directory} in Minio")
            df = read_file_from_minio(bucket_name, f"{minio_directory}/{file_name}")
            selected_df = process_table(df)
            total_df.append(selected_df)
            logging.info(f"Finish processing data in {file_name} from {minio_directory} in Minio")
        try:
            logging.info(f"Start merging all data ")    
            merged_df = pd.concat(total_df, ignore_index=True)
            write_pandas_df(merged_df,bucket_name,f"{saved_directory}/total_silver_product.csv")
            return merged_df.to_json(orient='records')
        except Exception as e:
            logging.error(f"Error when merging all data: {e}")
            return json.dumps([]) 
        
    except Exception as e:
        logging.error(f"Error when processing data in {file_name} from {minio_directory} in Minio: {e}")
        return json.dumps([]) 


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

def process_silver_review_data(ti):
    file_name = "full_review_data.csv"
    json_records = ti.xcom_pull(task_ids="upload_bronze_review_data")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from bronze_review_task.")
        return json.dumps([])
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return json.dumps([])
    
    bucket_name = "tiki-datalake"
    minio_directory = "bronze"
    saved_directory = "silver"
    logging.info(f"Start process review data from {minio_directory} in Minio")
    try:
        df.drop_duplicates(subset=['id','product_id','seller_id'],keep='first',inplace=True)
        df.dropna(subset=['id','product_id','seller_id'],inplace=True)
        df.fillna({'content' : ''},inplace=True)
        df['seller_id'] = df['seller_id'].astype(int)
        df.drop(['seller_name','customer_id'],axis=1,inplace=True)

        df['content'] = df['content'].apply(lambda x : preprocess_text(x))
        write_pandas_df(df,bucket_name,f"{saved_directory}/{file_name}")
        logging.info(f"Start process data in {file_name} from {minio_directory} in Minio")
        return df.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error when processing review data from {minio_directory} in Minio: {e}")
        return json.dumps([]) 
