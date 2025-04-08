import requests
import time
import random
import json
from minio import Minio
import os
import json
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
def crawl_categories_product_id(id : int , name : str, url_name : str,directory : str, limit = 40) : 
    agent_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0'
    ]
    agent = random.choice(agent_list)
    header = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent' : agent,
        'Referer' : f'https://tiki.vn/{url_name}/c{id}',
        'accept-language':'en-US,en;q=0.9'
    }
    params = {
        'limit' : limit,
        'sort' : 'top_seller',
        'urlKey' : 'url_name',
        'page' : 1,
        'category' : id
    }
    print(f"Crawling data from category {name} with id {id}")
    count = 0
    i = 1
    all_data = []
    while True : 
        print(f"Crawling Page {i}")
        params['page'] = i
        res = requests.get("https://tiki.vn/api/personalish/v1/blocks/listings",headers=header,params=params)
        if(res.status_code != 200 or len(res.json()['data']) == 0) : 
            print(f"Stop crawling due to empty data")
            break
        count += len(res.json()['data'])
        all_data.extend(res.json()['data'])
        time.sleep(random.randint(1,5))
        print(count)
        i+=1
        
    
    print(f"Finish crawling data from category {name} with id {id}")
    print(f"Length of data : {count}")


    df = pd.DataFrame(all_data)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{directory}/data_{url_name}.parquet")

    print(f"Finish writing file data_{url_name}.parquet into directory {directory}")



def import_to_minio_bucket(bucket_name : str, file_name : str , client ,data_directory : str) : 
    file_path = data_directory+'/'+file_name
    client.fput_object(bucket_name,file_path,file_path)
    print(
        file_path, "successfully uploaded as object",
        file_path, "to bucket", bucket_name,
    )



def main(minio_access_key : str, minio_secret_key : str) : 
    categories = {
        8322: {"id": 8322, "name": "Nhà Sách Tiki", "urlname": "nha-sach-tiki"},
        1883: {"id": 1883, "name": "Nhà Cửa - Đời Sống", "urlname": "nha-cua-doi-song"},
        1789: {"id": 1789, "name": "Điện Thoại - Máy Tính Bảng", "urlname": "dien-thoai-may-tinh-bang"},
        2549: {"id": 2549, "name": "Đồ chơi Mẹ và Bé", "urlname": "do-choi-me-be"},
        1815: {"id": 1815, "name": "Thiết Bị Số - Phụ Kiện Số", "urlname": "thiet-bi-kts-phu-kien-so"},
        1882: {"id": 1882, "name": "Điện Gia Dụng", "urlname": "dien-gia-dung"},
        1520: {"id": 1520, "name": "Làm Đẹp - Sức Khỏe", "urlname": "lam-dep-suc-khoe"},
        8594: {"id": 8594, "name": "Ô Tô - Xe Máy - Xe Đạp", "urlname": "o-to-xe-may-xe-dap"},
        931: {"id": 931, "name": "Thời trang nữ", "urlname": "thoi-trang-nu"},
        4384: {"id": 4384, "name": "Bách Hóa Online", "urlname": "bach-hoa-online"},
        1975: {"id": 1975, "name": "Thể Thao - Dã Ngoại", "urlname": "the-thao-da-ngoai"},
        915: {"id": 915, "name": "Thời trang nam", "urlname": "thoi-trang-nam"},
        17166: {"id": 17166, "name": "Cross Border - Hàng Quốc Tế", "urlname": "cross-border-hang-quoc-te"},
        1846: {"id": 1846, "name": "Laptop - Máy Vi Tính - Linh kiện", "urlname": "laptop-may-vi-tinh-linh-kien"},
        1686: {"id": 1686, "name": "Giày - Dép nam", "urlname": "giay-dep-nam"},
        4221: {"id": 4221, "name": "Điện Tử - Điện Lạnh", "urlname": "dien-tu-dien-lanh"},
        1703: {"id": 1703, "name": "Giày - Dép nữ", "urlname": "giay-dep-nu"},
        1801: {"id": 1801, "name": "Máy Ảnh - Máy Quay Phim", "urlname": "may-anh"},
        27498: {"id": 27498, "name": "Phụ kiện thời trang", "urlname": "phu-kien-thoi-trang"},
        44792: {"id": 44792, "name": "NGON", "urlname": "ngon"},
        8371: {"id": 8371, "name": "Đồng hồ và Trang sức", "urlname": "dong-ho-va-trang-suc"},
        6000: {"id": 6000, "name": "Balo và Vali", "urlname": "balo-va-vali"},
        11312: {"id": 11312, "name": "Voucher - Dịch vụ", "urlname": "voucher-dich-vu"},
        976: {"id": 976, "name": "Túi thời trang nữ", "urlname": "tui-vi-nu"},
        27616: {"id": 27616, "name": "Túi thời trang nam", "urlname": "tui-thoi-trang-nam"},
        15078: {"id": 15078, "name": "Chăm sóc nhà cửa", "urlname": "cham-soc-nha-cua"}
    }


    bucket_name = 'rawbucket'
    data_directory = "data"
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    for key in categories.keys() : 
        id,name,url_name = categories[key]['id'],categories[key]['name'],categories[key]['urlname']
        crawl_categories_product_id(id,name,url_name,data_directory)
        


    client = Minio('localhost:9000',
               access_key=minio_access_key,
               secret_key=minio_secret_key,
               secure=False)
    
    found = client.bucket_exists(bucket_name)
    if not found : 
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else : print("Bucket", bucket_name, "already exists")

    walk_dir = "./"+ data_directory
    for _, _, files in os.walk(walk_dir):
        for file in files : 
            import_to_minio_bucket(bucket_name,file,client,data_directory) 
    


if __name__ == "__main__" : 
    with open('credentials.json', 'r') as file:
        data = json.load(file)
    minio_access_key, minio_secret_key = data['accessKey'],data['secretKey']
    main(minio_access_key,minio_secret_key)