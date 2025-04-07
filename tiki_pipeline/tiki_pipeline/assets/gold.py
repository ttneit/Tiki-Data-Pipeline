from dagster import AssetExecutionContext,StaticPartitionsDefinition,asset,Output,AssetIn
from minio import Minio
import findspark
findspark.init()
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from contextlib import contextmanager
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
    

def write_to_SQLServer(df,table_name, mode ,database) : 
    with open('credentials.json', 'r') as f:
        database_info = json.load(f)
    url = f"jdbc:sqlserver://{database_info['server_name']}:{database_info['port']};databaseName={database}"
    properties = {
        "user": database_info['user'],
        "password": database_info['password'],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
    print(f"Write {table_name} in SQL Server successfully")


def read_data_from_SQLServer(spark, table_name, database) : 
    with open('credentials.json', 'r') as f:
        database_info = json.load(f)
    url = f"jdbc:sqlserver://{database_info['server_name']}:{database_info['port']};databaseName={database}"
    properties = {
        "user": database_info['user'],
        "password": database_info['password'],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df = spark.read.jdbc(url = url,table = table_name,properties=properties)
    return df



@asset(
    description="Insert new data for Seller Dimension table",
    group_name="gold",
    key_prefix=["gold","dim"],
    ins={
        "silver_product_data" : AssetIn(key_prefix=["silver","product"])
    }
)
def insert_dim_seller(context: AssetExecutionContext, silver_product_data) : 
    bucket_name = "tiki-datalake"
    database = "tiki_datamart"
    table_name = "dim_seller"
    mode = "append"
    context.log.info(f"Merge all partitioned upstream assets from silver layer")
    all_silver_product_data = pd.concat(silver_product_data.values(), ignore_index=True)
    context.log.info(f"Start inserting new data for Seller Dimension Table in gold layer")
    
    with initialize_spark() as spark: 
        context.log.info("Extracting the Seller Information from silver layer")
        df = spark.createDataFrame(all_silver_product_data)

        new_seller_dim_df = df.select("seller_id","seller_type","is_official_store","is_trusted_store")
        context.log.info("Extracting the Seller Information from Data Mart")
        old_seller_dim_df = read_data_from_SQLServer(spark,table_name,database)

        new_seller_dim_df = new_seller_dim_df.withColumnsRenamed({'seller_id' : 'seller_id_new','seller_type' : 'seller_type_new'
                                                                  ,'is_official_store' : 'is_official_store_new','is_trusted_store' : 'is_trusted_store_new'})
        
        full_df = old_seller_dim_df.join(new_seller_dim_df,new_seller_dim_df.seller_id_new == old_seller_dim_df.seller_id,how='right')

        filtered_df = full_df.where(sf.col('seller_id').isNull())

        if filtered_df.count() == 0 :
            context.log.info("There is no new data for seller dimension data")
        else:
            filtered_df = filtered_df.drop('seller_id','seller_type','is_official_store','is_trusted_store')
            filtered_df = filtered_df.withColumnsRenamed({'seller_id_new' : 'seller_id','seller_type_new' : 'seller_type'
                                                                  ,'is_official_store_new' : 'is_official_store','is_trusted_store_new' : 'is_trusted_store'})
            
            filtered_df = filtered_df.dropDuplicates()
            context.log.info(f"Writing {table_name} to Data Warehouse")
            write_to_SQLServer(filtered_df,table_name,mode,database)
            context.log.info(f"Finishing updating {table_name} in Data Mart")

            write_to_minio(filtered_df,bucket_name,f"gold/{table_name}")

            context.log.info(f"Write {table_name} to {bucket_name} successfully")

            context.log.info(f"Finish inserting new data for Seller Dimension Table in gold layer")

        dim_seller_df = read_data_from_SQLServer(spark,table_name,database)      
        pandas_df = dim_seller_df.toPandas()
    return Output(
        value=pandas_df,
        metadata={
            "table_name": table_name,
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    )


@asset(
    description="Insert new data for Category Dimension table",
    group_name="gold",
    key_prefix=["gold","dim"],
    ins={
        "silver_product_data" : AssetIn(key_prefix=["silver","product"])
    }
)
def insert_dim_category(context: AssetExecutionContext, silver_product_data) :
    bucket_name = "tiki-datalake"
    database = "tiki_datamart"
    table_name = "dim_category"
    mode = "append"
    context.log.info(f"Merge all partitioned upstream assets from silver layer")
    all_silver_product_data = pd.concat(silver_product_data.values(), ignore_index=True)
    context.log.info(f"Start inserting new data for Category Dimension Table in gold layer")
    with initialize_spark() as spark: 
        context.log.info("Extracting the Category Information from silver layer")
        df = spark.createDataFrame(all_silver_product_data)

        new_category_dim_df = df.select("primary_category_path","category","primary_category_name")
        new_category_dim_df.show()
        context.log.info("Extracting the Category Information from Data Mart")
        old_category_dim_df = read_data_from_SQLServer(spark,table_name,database)

        new_category_dim_df = new_category_dim_df.withColumnsRenamed({'primary_category_path' : 'primary_category_path_new','category' : 'category_new',
                                                                      'primary_category_name' : 'primary_category_name_new'})
        old_category_dim_df.show()
        full_df = old_category_dim_df.join(new_category_dim_df,(new_category_dim_df.primary_category_path_new == old_category_dim_df.primary_category_path) & \
                                            (new_category_dim_df.category_new == old_category_dim_df.category) & \
                                            (new_category_dim_df.primary_category_name_new == old_category_dim_df.primary_category_name)
                                           ,how='right')

        filtered_df = full_df.where(
            sf.col('primary_category_path').isNull() | 
            sf.col('primary_category_name').isNull() | 
            sf.col('category').isNull()
        )
        if filtered_df.count() == 0 :
            context.log.info("There is no new data for Category dimension data")
        else:
            category_df = filtered_df.select('category_new','primary_category_path_new','primary_category_name_new').distinct()
            category_df = category_df.withColumnsRenamed({'primary_category_path_new' : 'primary_category_path','category_new' : 'category',
                                                                                'primary_category_name_new' : 'primary_category_name'})
            category_df = category_df.withColumn('category_l1_name',sf.split(sf.col('primary_category_path'),'/')[2])
            category_df = category_df.withColumn('category_l2_name',sf.split(sf.col('primary_category_path'),'/')[3])
            category_df = category_df.withColumn('category_l3_name',sf.split(sf.col('primary_category_path'),'/')[4])
            category_df = category_df.withColumn('category_l4_name',sf.split(sf.col('primary_category_path'),'/')[5])
            category_df = category_df.na.fill({'category_l4_name':'None', 'category_l3_name': 'None'})
            category_df = category_df.dropDuplicates()
            context.log.info(f"Writing {table_name} to Data Warehouse")
            write_to_SQLServer(category_df,table_name,mode,database)
            context.log.info(f"Finishing updating {table_name} in Data Mart")

            write_to_minio(category_df,bucket_name,f"gold/{table_name}")

            context.log.info(f"Write {table_name} to {bucket_name} successfully")

            context.log.info(f"Finish inserting new data for Category Dimension Table in gold layer")

        dim_category_df = read_data_from_SQLServer(spark,table_name,database)      
        pandas_df = dim_category_df.toPandas()
    return Output(
        value=pandas_df,
        metadata={
            "table_name": table_name,
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    )
 

@asset(
    description="Insert new data for Brand Dimension table",
    group_name="gold",
    key_prefix=["gold","dim"],
    ins={
        "silver_product_data" : AssetIn(key_prefix=["silver","product"])
    }
)
def insert_dim_brand(context: AssetExecutionContext, silver_product_data) :
    bucket_name = "tiki-datalake"
    database = "tiki_datamart"
    table_name = "dim_brand"
    mode = "append"
    context.log.info(f"Merge all partitioned upstream assets from silver layer")
    all_silver_product_data = pd.concat(silver_product_data.values(), ignore_index=True)
    context.log.info(f"Start inserting new data for Brand Dimension Table in gold layer")
    with initialize_spark() as spark: 
        context.log.info("Extracting the Brand Information from silver layer")
        df = spark.createDataFrame(all_silver_product_data)

        new_brand_dim_df = df.select("brand_name","is_top_brand")
        context.log.info("Extracting the Brand Information from Data Mart")
        old_brand_dim_df = read_data_from_SQLServer(spark,table_name,database)

        new_brand_dim_df = new_brand_dim_df.withColumnsRenamed({'brand_name' : 'brand_name_new','is_top_brand' : 'is_top_brand_new'})
        
        full_df = old_brand_dim_df.join(new_brand_dim_df,new_brand_dim_df.brand_name_new == old_brand_dim_df.brand_name,how='right')

        filtered_df = full_df.where(sf.col('brand_name').isNull())

        if filtered_df.count() == 0 :
            context.log.info("There is no new data for brand dimension data")
        else:
            filtered_df = filtered_df.drop('brand_name','is_top_brand','brand_id')
            filtered_df = filtered_df.withColumnsRenamed({'brand_name_new' : 'brand_name','is_top_brand_new' : 'is_top_brand'})
            filtered_df = filtered_df.dropDuplicates()
            context.log.info(f"Writing {table_name} to Data Warehouse")
            write_to_SQLServer(filtered_df,table_name,mode,database)
            context.log.info(f"Finishing updating {table_name} in Data Mart")

            write_to_minio(filtered_df,bucket_name,f"gold/{table_name}")

            context.log.info(f"Write {table_name} to {bucket_name} successfully")

            context.log.info(f"Finish inserting new data for brand Dimension Table in gold layer")

            dim_brand_df = read_data_from_SQLServer(spark,table_name,database)      
            pandas_df = dim_brand_df.toPandas()
    return Output(
        value=pandas_df,
        metadata={
            "table_name": table_name,
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    )


@asset(
    description="Insert new data for Product Dimension table",
    group_name="gold",
    key_prefix=["gold","dim"],
    ins={
        "silver_product_data" : AssetIn(key_prefix=["silver","product"])
    }
)
def insert_dim_product(context: AssetExecutionContext, silver_product_data) :
    bucket_name = "tiki-datalake"
    database = "tiki_datamart"
    table_name = "dim_product"
    mode = "append"
    context.log.info(f"Merge all partitioned upstream assets from silver layer")
    all_silver_product_data = pd.concat(silver_product_data.values(), ignore_index=True)
    context.log.info(f"Start inserting new data for Product Dimension Table in gold layer")
    with initialize_spark() as spark: 
        context.log.info("Extracting the Product Information from silver layer")
        df = spark.createDataFrame(all_silver_product_data)
        new_product_id = df.select('id').withColumnsRenamed({'id' : 'new_id'})
        context.log.info("Extracting the Brand Information from Data Mart")
        old_product_id = read_data_from_SQLServer(spark,table_name,database).select('product_id')
        full_id_df = old_product_id.join(new_product_id,new_product_id.new_id == old_product_id.product_id,how='right')


        filtered_id_df = full_id_df.where(sf.col('product_id').isNull()).select('new_id').withColumnsRenamed({'new_id' : 'id'})
        if filtered_id_df.count() == 0 :
            context.log.info("There is no new data for Product dimension data")
        else:
            filtered_product_df = filtered_id_df.join(df,on='id',how='inner') \
                .select('id', 'sku', 'name', 'url_key', 'url_path', 'availability', 'is_imported', 'is_authentic'
                , 'tiki_verified', 'is_tikinow', 'is_tikipro', 'is_ad', 'inventory_status'
                , 'is_visible', 'origin', 'freegift_items', 'shippable')
            filtered_product_df = filtered_product_df.withColumnsRenamed({'id' : 'product_id'}).dropDuplicates()
            filtered_product_df = filtered_product_df.withColumn("product_id", sf.col("product_id").cast("long"))
            filtered_product_df = filtered_product_df.withColumn("sku", sf.col("product_id").cast("long"))
            filtered_product_df = filtered_product_df.dropDuplicates(["product_id"])
            context.log.info(f"Writing {table_name} to Data Warehouse")
            write_to_SQLServer(filtered_product_df,table_name,mode,database)
            context.log.info(f"Finishing updating {table_name} in Data Mart")

            write_to_minio(filtered_product_df,bucket_name,f"gold/{table_name}")

            context.log.info(f"Write {table_name} to {bucket_name} successfully")

            context.log.info(f"Finish inserting new data for Product Dimension Table in gold layer")

        dim_product_df = read_data_from_SQLServer(spark, table_name, database)  
        pandas_df = dim_product_df.toPandas()
    return Output(
        value=pandas_df,
        metadata={
            "table_name": table_name,
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    ) 

# def insert_dim_promotion():
#     pass




@asset(
    description="Insert new data for Fact table",
    group_name="gold",
    key_prefix=["gold","dim"],
    ins={
        "silver_product_data" : AssetIn(key_prefix=["silver","product"]),
        "insert_dim_product" : AssetIn(key_prefix=["gold","dim"]),
        "insert_dim_brand" : AssetIn(key_prefix=["gold","dim"]),
        "insert_dim_category" : AssetIn(key_prefix=["gold","dim"]),
        "insert_dim_seller" : AssetIn(key_prefix=["gold","dim"]),
    }
)
def insert_fact_table(context: AssetExecutionContext, silver_product_data,
                      insert_dim_brand: pd.DataFrame, 
                      insert_dim_category: pd.DataFrame,
                      insert_dim_seller: pd.DataFrame,
                      insert_dim_product: pd.DataFrame):
    bucket_name = "tiki-datalake"
    database = "tiki_datamart"
    table_name = "fact_tiki"
    mode = "append"
    context.log.info(f"Merge all partitioned upstream assets from silver layer")
    all_silver_product_data = pd.concat(silver_product_data.values(), ignore_index=True)
    context.log.info(f"Start inserting new data for Fact Table in gold layer")
    with initialize_spark() as spark:
        df = spark.createDataFrame(all_silver_product_data).select('id','brand_name','seller_id','primary_category_path','price', 'list_price', 'discount', 'discount_rate'
                                                               , 'rating_average', 'review_count', 'favourite_count'
                                                               , 'original_price', 'all_time_quantity_sold', 'number_of_reviews', 'search_rank')
        
        brand_df = spark.createDataFrame(insert_dim_brand).select('brand_id','brand_name')
        category_df = spark.createDataFrame(insert_dim_category).select('category_id','primary_category_path')
        seller_df = spark.createDataFrame(insert_dim_seller).select('seller_id')
        product_df = spark.createDataFrame(insert_dim_product).select('product_id')

        full_df = df.join(product_df,df.id == product_df.product_id,how='inner').drop('id')
        full_df = full_df.join(seller_df,on='seller_id',how='inner')
        full_df = full_df.join(category_df,on='primary_category_path',how='inner').drop('primary_category_path')

        full_df = full_df.join(brand_df,on='brand_name',how='inner').drop('brand_name')

        full_df = full_df.fillna(0)
        context.log.info(f"Writing {table_name} to Data Warehouse")
        write_to_SQLServer(full_df,table_name,mode,database)
        context.log.info(f"Finishing updating {table_name} in Data Mart")

        write_to_minio(full_df,bucket_name,f"gold/{table_name}")

        context.log.info(f"Write {table_name} to {bucket_name} successfully")

        context.log.info(f"Finish inserting new data for Fact Table in gold layer")

        fact_df = read_data_from_SQLServer(spark,table_name,database)      
        pandas_df = fact_df.toPandas()
    return Output(
        value=pandas_df,
        metadata={
            "table_name": table_name,
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    ) 


@asset(
    group_name="gold",
    key_prefix=["gold","review"],
    ins={
        "silver_review_data" : AssetIn(key_prefix=["silver","review"])
    }
)
def gold_review_data(context: AssetExecutionContext, silver_review_data: pd.DataFrame):

    bucket_name = "tiki-datalake"
    file_path = "gold/review.csv"
    context.log.info(f"Start transformation steps for review data from Silver Layer")

    pandas_df = silver_review_data
    # pandas_df.drop_duplicates(subset=['id','product_id','seller_id'],keep='first',inplace=True)
    # pandas_df.dropna(subset=['id','product_id','seller_id'],inplace=True)
    # pandas_df.fillna({'content' : ''},inplace=True)
    # pandas_df['seller_id'] = pandas_df['seller_id'].astype(int)
    # pandas_df.drop(['seller_name','customer_id'],axis=1,inplace=True)

    # pandas_df['content'] = pandas_df['content'].apply(lambda x : preprocess_text(x))

    
    write_pandas_df(pandas_df,bucket_name,file_path)

    return Output(
        value=pandas_df,
        metadata={
            "table_name": "review_data",
            "row_numbers": pandas_df.shape[0],
            "column_numbers" : pandas_df.shape[1]
        }
    ) 



