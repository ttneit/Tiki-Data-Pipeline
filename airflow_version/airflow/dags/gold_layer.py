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
from sqlalchemy import create_engine
import traceback
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


def read_table_from_sql_server(server, database, username, password, table_name, driver) :
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)
    sql_query = f'SELECT * FROM {table_name}'

    df = pd.read_sql(sql_query, con=engine)

    return df

def insert_into_sql_server(df, driver, server, database, username, password, mode, table_name) : 
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)
    logging.info(f"Start inserting data into {table_name}")
    try:
        df.to_sql(name=table_name, con=engine, schema='dbo', if_exists=mode, index=False)
        logging.info(f"Finish inserting data into {table_name}")
    except Exception as e:
        logging.error(f"Error when inserting data into {table_name}: {e}")

def insert_dim_seller(ti, **kwargs):
    file_name = kwargs['file_name']
    saved_name = file_name.split(".")[0]
    json_records = ti.xcom_pull(task_ids=f"silver_product_{file_name.replace('.parquet','').replace('-', '_')}")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from bronze_product_task.")
        return json.dumps([])
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return json.dumps([])
        
    bucket_name = "tiki-datalake"
    saved_directory = "gold"
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'append'
    table_name = "dim_seller"
    logging.info(f"Start inserting data into {table_name}")
    try:
        new_seller_dim_df = df[["seller_id", "seller_type", "is_official_store", "is_trusted_store"]].copy()
    
        old_seller_dim_df = read_table_from_sql_server(server, database, username, password, table_name, driver)

        new_seller_dim_df.rename(columns={
            "seller_id": "seller_id_new",
            "seller_type": "seller_type_new",
            "is_official_store": "is_official_store_new",
            "is_trusted_store": "is_trusted_store_new"
        }, inplace=True)

        merged_df = pd.merge(old_seller_dim_df, new_seller_dim_df, how="right", left_on="seller_id", right_on="seller_id_new")
        filtered_df = merged_df[merged_df['seller_id'].isnull()]
        if filtered_df.empty:
            logging.info(f"There is no new data for {table_name}")
            return
        else:
            filtered_df = filtered_df.drop(columns=['seller_id', 'seller_type', 'is_official_store', 'is_trusted_store'])
            filtered_df = filtered_df.rename(columns={
                'seller_id_new': 'seller_id',
                'seller_type_new': 'seller_type',
                'is_official_store_new': 'is_official_store',
                'is_trusted_store_new': 'is_trusted_store'
            })

            filtered_df = filtered_df.drop_duplicates()

            write_pandas_df(filtered_df,bucket_name,f"{saved_directory}/dim/{saved_name}.csv")
            insert_into_sql_server(filtered_df, driver, server, database, username, password, mode, table_name)
        logging.info(f"Finish inserting data into {table_name}")
    except Exception as e:
        logging.error(f"Error when inserting data into {table_name}: {e}")
    return 


def split_result(primary_path, position) :
    if primary_path is None: return None
    split_res = primary_path.split('/')
    if position >= len(split_res) :
        return None
    else: return split_res[position]

def insert_dim_category(ti, **kwargs):
    file_name = kwargs['file_name']
    saved_name = file_name.split(".")[0]
    json_records = ti.xcom_pull(task_ids=f"silver_product_{file_name.replace('.parquet','').replace('-', '_')}")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from silver_product_task.")
        return json.dumps([])
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return json.dumps([])
        
    bucket_name = "tiki-datalake"
    saved_directory = "gold"
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'append'
    table_name = "dim_category"
    logging.info(f"Start inserting data into {table_name}")
    try:
        new_category_dim_df = df[["primary_category_path","category","primary_category_name"]].copy()

        old_category_dim_df = read_table_from_sql_server(server, database, username, password, table_name, driver)

        new_category_dim_df.rename(columns={'primary_category_path' : 'primary_category_path_new',
                                            'category' : 'category_new',
                                            'primary_category_name' : 'primary_category_name_new'}, inplace=True)

        merged_df = pd.merge(old_category_dim_df, new_category_dim_df, how="right", left_on=["primary_category_path","category","primary_category_name"], 
                            right_on=["primary_category_path_new","category_new","primary_category_name_new"])

        filtered_df = merged_df[
            merged_df['primary_category_path'].isna() |
            merged_df['primary_category_name'].isna() |
            merged_df['category'].isna()
        ]
        if filtered_df.empty:
            logging.info(f"There is no new data for {table_name}")
            return
        else:
            category_df = filtered_df[['category_new', 'primary_category_path_new', 'primary_category_name_new']].copy()
            category_df.drop_duplicates(keep='first',inplace=True)
            category_df.rename(columns={'primary_category_path_new' : 'primary_category_path',
                                                        'category_new' : 'category',
                                                        'primary_category_name_new' : 'primary_category_name'}, inplace=True)
            category_df['category_l1_name'] = category_df['primary_category_path'].apply(lambda x : split_result(x,2))
            category_df['category_l2_name'] = category_df['primary_category_path'].apply(lambda x : split_result(x,3))
            category_df['category_l3_name'] = category_df['primary_category_path'].apply(lambda x : split_result(x,4))
            category_df['category_l4_name'] = category_df['primary_category_path'].apply(lambda x : split_result(x,5))

            category_df['category_l3_name'].fillna('None', inplace=True)
            category_df['category_l4_name'].fillna('None', inplace=True)

            category_df = category_df.drop_duplicates()

            write_pandas_df(category_df,bucket_name,f"{saved_directory}/dim/{saved_name}.csv")
            insert_into_sql_server(category_df, driver, server, database, username, password, mode, table_name)
            logging.info(f"Finish inserting data into {table_name}")
    except Exception as e:
        logging.error(f"Error when inserting data into {table_name}: {e}")
        traceback.print_exc()
    return 



def insert_dim_brand(ti, **kwargs):
    file_name = kwargs['file_name']
    saved_name = file_name.split(".")[0]
    json_records = ti.xcom_pull(task_ids=f"silver_product_{file_name.replace('.parquet','').replace('-', '_')}")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from bronze_product_task.")
        return json.dumps([])
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return json.dumps([])
        
    bucket_name = "tiki-datalake"
    saved_directory = "gold"
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'append'
    table_name = "dim_brand"
    logging.info(f"Start inserting data into {table_name}")
    try:
        new_brand_dim_df = df[["brand_name","is_top_brand"]].copy()
    
        old_brand_dim_df = read_table_from_sql_server(server, database, username, password, table_name, driver)

        new_brand_dim_df.rename(columns={'brand_name' : 'brand_name_new','is_top_brand' : 'is_top_brand_new'}, inplace=True)

        merged_df = pd.merge(old_brand_dim_df, new_brand_dim_df, how="right", left_on="brand_name", right_on="brand_name_new")
        filtered_df = merged_df[merged_df['brand_name'].isnull()]

        if filtered_df.empty:
            logging.info(f"There is no new data for {table_name}")
            return
        else:
            filtered_df = filtered_df.drop(columns=['brand_name','is_top_brand','brand_id'])
            filtered_df = filtered_df.rename(columns={'brand_name_new' : 'brand_name','is_top_brand_new' : 'is_top_brand'})

            filtered_df = filtered_df.drop_duplicates()

            write_pandas_df(filtered_df,bucket_name,f"{saved_directory}/dim/{saved_name}.csv")
            insert_into_sql_server(filtered_df, driver, server, database, username, password, mode, table_name)
            logging.info(f"Finish inserting data into {table_name}")
    except Exception as e:
        logging.error(f"Error when inserting data into {table_name}: {e}")
        traceback.print_exc()
    return 


def insert_dim_product(ti, **kwargs):
    file_name = kwargs['file_name']
    saved_name = file_name.split(".")[0]
    json_records = ti.xcom_pull(task_ids=f"silver_product_{file_name.replace('.parquet','').replace('-', '_')}")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from bronze_product_task.")
        return json.dumps([])
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return json.dumps([])
        
    bucket_name = "tiki-datalake"
    saved_directory = "gold"
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'append'
    table_name = "dim_product"
    logging.info(f"Start inserting data into {table_name}")
    try:
        new_product_id  = df[['id']].copy()
        new_product_id.rename(columns={'id': 'new_id'}, inplace=True)
        old_product_id  = read_table_from_sql_server(server, database, username, password, table_name, driver)[['product_id']]

        full_id_df = pd.merge(old_product_id, new_product_id, how="right", left_on="product_id", right_on="new_id")
        filtered_id_df = full_id_df[full_id_df['product_id'].isnull()]
        filtered_id_df = filtered_id_df[['new_id']]
        filtered_id_df.rename(columns={'new_id': 'id'}, inplace=True)
        if filtered_id_df.empty:
            logging.info(f"There is no new data for {table_name}")
            return
        else:
            filtered_product_df = pd.merge(filtered_id_df, df, on='id', how='inner')[[
                'id', 'sku', 'name', 'url_key', 'url_path', 'availability', 'is_imported', 'is_authentic', 'tiki_verified',
                'is_tikinow', 'is_tikipro', 'is_ad', 'inventory_status', 'is_visible', 'origin', 'freegift_items', 'shippable'
            ]]

            filtered_product_df = filtered_product_df.rename(columns={'id': 'product_id'}).drop_duplicates()
            filtered_product_df['product_id'] = filtered_product_df['product_id'].astype('int64')
            filtered_product_df['sku'] = filtered_product_df['sku'].astype('int64')
            filtered_product_df = filtered_product_df.drop_duplicates(subset=['product_id'])

        write_pandas_df(filtered_product_df,bucket_name,f"{saved_directory}/dim/{saved_name}.csv")
        insert_into_sql_server(filtered_product_df, driver, server, database, username, password, mode, table_name)
        logging.info(f"Finish inserting data into {table_name}")
    except Exception as e:
        logging.error(f"Error when inserting data into {table_name}: {e}")
        traceback.print_exc()
    return 


def insert_fact_table(ti, **kwargs):
    file_name = kwargs['file_name']
    saved_name = file_name.split(".")[0]
    json_records = ti.xcom_pull(task_ids=f"silver_product_{file_name.replace('.parquet','').replace('-', '_')}")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from bronze_product_task.")
        return json.dumps([])
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return json.dumps([])
        
    bucket_name = "tiki-datalake"
    saved_directory = "gold"
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'append'
    table_name = "fact_table"
    logging.info(f"Start inserting data into {table_name}")
    try:
        new_product_df = df[['id','brand_name','seller_id','primary_category_path','price', 
                         'list_price', 'discount', 'discount_rate', 'rating_average', 'review_count', 'favourite_count'
                        , 'original_price', 'all_time_quantity_sold', 'number_of_reviews', 'search_rank']].copy()
    
        brand_df = read_table_from_sql_server(server, database, username, password, "dim_brand", driver)[['brand_id','brand_name']]
        category_df = read_table_from_sql_server(server, database, username, password, "dim_category", driver)[['category_id','primary_category_path']]
        seller_df = read_table_from_sql_server(server, database, username, password, "dim_seller", driver)[['seller_id']]
        product_df = read_table_from_sql_server(server, database, username, password, "dim_product", driver)[['product_id']]

        full_df = pd.merge(new_product_df, product_df, left_on='id', right_on='product_id', how='inner').drop('id', axis=1)

        full_df = pd.merge(full_df, seller_df, on='seller_id', how='inner')

        full_df = pd.merge(full_df, category_df, on='primary_category_path', how='inner').drop('primary_category_path', axis=1)

        full_df = pd.merge(full_df, brand_df, on='brand_name', how='inner').drop('brand_name', axis=1)

        full_df = full_df.fillna(0)
        write_pandas_df(full_df,bucket_name,f"{saved_directory}/dim/{saved_name}.csv")
        insert_into_sql_server(full_df, driver, server, database, username, password, mode, table_name)
        logging.info(f"Finish inserting data into {table_name}")
    except Exception as e:
        logging.error(f"Error when inserting data into {table_name}: {e}")
    return 







def insert_review_data(ti):
    file_name = "full_review_data.csv"
    json_records = ti.xcom_pull(task_ids="process_silver_review_data")  
    if not json_records or len(json_records) == 2:
        logging.warning("No data received from process_silver_review_data task.")
        return 
    
    try:
        df = pd.read_json(json_records, orient='records')
    except Exception as e:
        logging.error(f"Failed to parse JSON records: {e}")
        return 
    
    bucket_name = "tiki-datalake"
    minio_directory = "silver"
    saved_directory = "gold"
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'append'
    table_name = 'review'
    logging.info(f"Start inserting review data from {minio_directory} in Minio to SQL Server")
    try:
        
        write_pandas_df(df,bucket_name,f"{saved_directory}/{file_name}")

        insert_into_sql_server(df,driver,server,database, username,password,mode, table_name)
        logging.info(f"Finish inserting review data from {minio_directory} in Minio to SQL Server")
    except Exception as e:
        logging.error(f"Error when inserting review data from {minio_directory} in Minio to SQL Server: {e}")
    return 
