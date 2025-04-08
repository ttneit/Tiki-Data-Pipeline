from minio import Minio
from io import BytesIO
import pandas as pd


def write_pandas_df(df, bucket_name, file_path): 
    minio_client = Minio("localhost:9000",access_key="9ealEXcd3Epkg6EBRFMJ",secret_key="sYoBai4qBMmEabhQKssx4nnQogv8PFCY7Km94IpZ",secure=False)
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
