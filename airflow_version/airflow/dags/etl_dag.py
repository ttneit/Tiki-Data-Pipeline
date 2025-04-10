from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bronze_layer import process_bronze_product_data, process_bronze_review_data
from silver_layer import process_silver_product_data, process_silver_review_data
from gold_layer import insert_review_data, insert_dim_brand, insert_dim_category, insert_dim_seller, insert_fact_table, insert_dim_product
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Tiki_Data_Pipeline',
    default_args=default_args,
    description='Upload product parquet + review CSV to Minio',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    upload_review_task = PythonOperator(
        task_id='upload_bronze_review_data',
        python_callable=process_bronze_review_data,
    )

    process_review_task = PythonOperator(
        task_id='process_silver_review_data',
        python_callable=process_silver_review_data,
    )

    insert_review_task = PythonOperator(
        task_id='insert_review_data',
        python_callable=insert_review_data,
    )

    upload_review_task >> process_review_task >> insert_review_task

    


    bronze_task = PythonOperator(
            task_id=f"bronze_product",
            python_callable=process_bronze_product_data,
        )

    silver_task = PythonOperator(
            task_id=f"silver_product",
            python_callable=process_silver_product_data,
    )

    dim_seller_task = PythonOperator(
        task_id=f"dim_seller",
        python_callable=insert_dim_seller,
    )

    dim_brand_task = PythonOperator(
        task_id=f"dim_brand",
        python_callable=insert_dim_brand,
    )

    dim_category_task = PythonOperator(
        task_id=f"dim_category",
        python_callable=insert_dim_category,
    )

    dim_product_task = PythonOperator(
        task_id=f"dim_product",
        python_callable=insert_dim_product,
    )

    fact_table_task = PythonOperator(
        task_id=f"fact_table",
        python_callable=insert_fact_table,
    )

    bronze_task >> silver_task >> [dim_seller_task, dim_brand_task, dim_category_task, dim_product_task] >> fact_table_task

        
