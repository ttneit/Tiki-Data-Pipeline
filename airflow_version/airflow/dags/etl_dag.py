from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bronze_layer import process_bronze_product_data, process_bronze_review_data
from silver_layer import process_silver_product_data, process_silver_review_data
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

    upload_review_task >> process_review_task

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

    for file in all_files:
        base_id = file.replace('.parquet', '').replace('-', '_')

        bronze_task = PythonOperator(
            task_id=f"bronze_product_{base_id}",
            python_callable=process_bronze_product_data,
            op_kwargs={'file_name': file},
        )

        silver_task = PythonOperator(
            task_id=f"silver_product_{base_id}",
            python_callable=process_silver_product_data,
            op_kwargs={'file_name': file},
        )

        bronze_task >> silver_task
