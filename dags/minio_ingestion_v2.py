from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime as dt
import pandas as pd
import random
import os
import sys
import uuid
import time
import json
from airflow.decorators import dag, task #type:ignore
import requests
#from scripts.pipeline import run_pipeline  # sua função já existente
#from scripts.pipeline import generate_fake_data, meta_data, extract_data, send_to_minio
import io

SRC_PATH = "/opt/airflow/src"
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from utils.minio_connection import connect_minio

BUCKET = "bronze-layer"
DATA_FOLDER = "/opt/airflow/data"

@dag(
    dag_id="orders_pipeline_v2",
    start_date = dt(2025, 12, 1, 0, 0),
    schedule = "* * * * *",
    catchup=False,
)
def orders_pipeline():
    
    @task
    def t_extract_data():
        response = requests.get('https://fakestoreapi.com/products')
        if response.status_code == 200:
            products_list = response.json()
        else:
            products_list = []
        return products_list
     
    @task
    def t_start_timer():
        start = time.perf_counter()
        return start

    @task
    def t_generate(products_list, data_folder):
        # 1 - Definir um dataframe inicial, vazio, com as colunas definidas + coluna quantidade
        columns = [
            'order_id',
            'product_id',
            'product_title',
            'price', # qty * price
            'description',
            'category',
            'qty',
            'created_at'
        ]
        df = pd.DataFrame(columns=columns)
        
        # 2 - Randomizar a quantidade de vezes que vai fazer a requisição (quantas ordens)
        order_qty = random.randint(1,3)
        
        timestamp = dt.now().strftime("%d-%m-%Y - %H:%M:%S")
        
        # 4 - Fazer uma busca pegando os Ids escolhidos e colocando os materiais num dataframe temporário
        
        rows = []
        
        for order in range(order_qty):
            
            order_id = str(uuid.uuid4())
            
            # 2.5 - Randomizar a quantidade de materiais por ordem
            random_item_qty = random.randint(3,15)
            
            # 3 - Randomizar quais IDs serão pegos (quais itens)
            ids_to_get = random.sample(range(1,21),random_item_qty) # sort para aumentar a eficiência na busca?

            for product in products_list:
                if isinstance(product, dict) and 'id' in product:
                    if product['id'] in ids_to_get:
                        
                        random_qty = random.randint(0,8)
                        
                        row = {
                            'order_id': order_id,
                            'product_id': product['id'],
                            'product_title': product['title'],
                            'price': product['price'],
                            'description': product['description'],
                            'category': product['category'],
                            'qty': random_qty,
                            'created_at': timestamp
                        }

                        rows.append(row)
            
            temp_df = pd.DataFrame(rows, columns=columns)
            
        df = pd.concat([df, temp_df], ignore_index=True) 
        
        # Gerar o nome do arquivo raw
        
        file_name = "orders_" + df["created_at"].unique()[0] 
        
        output_path = os.path.join(data_folder, "raw", file_name + ".parquet")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Saída Bronze
        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
            
        return  {
        'file_name': file_name,
        'file_path': output_path,
        'row_count': len(df),
        'timestamp': timestamp
        }
        
    @task
    def t_meta_data(file_info, elapsed_time, status, data_folder):
        
        file_name = file_info["file_name"]
        
        try:
            output_path = os.path.join(data_folder, "raw", file_name + "_meta.json")
            profile_id = 1
            df_row_count = file_info["row_count"]
            price_currency = "BRL"
            source_name = "Fake Store API"
            automation_status = status
            environment = "dev"
            duration = round(elapsed_time,3)
            timestamp = file_info["timestamp"]
            
            metadata = {
            "profile_id": profile_id,
            "row_count": df_row_count,
            "price_currency": price_currency,
            "source_name": source_name,
            "automation_status": automation_status,
            "environment": environment,
            "duration_seconds": duration,
            "timestamp": timestamp
            }
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=4, ensure_ascii=False)
                
            return {'metadata': metadata,'output_path': output_path, 'file_info': file_info }
        
        except Exception as e:
            print(f"An error was found while building the meta data {e}")
            raise ValueError(f"Error while building the metadata for order {file_name}")
    
    @task
    def t_send_order_to_minio(file_info, target_bucket):
        
        # Conectando ao MinIO
        client = connect_minio()
        
        file_name = "orders_" + file_info["timestamp"]
        
        #csv_buffer = io.StringIO()
        parquet_buffer = io.BytesIO()
        
        df = pd.read_parquet(file_info["file_path"])
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
        
        content_bytes = parquet_buffer.getvalue()
        extension = ".parquet"
        
        parquet_buffer.seek(0)
        
        byte_stream = io.BytesIO(content_bytes)
    
        client.put_object(
            target_bucket,
            f"{file_name}{extension}", 
            byte_stream,
            len(content_bytes)     
        )
        
        return True
    
    @task
    def t_send_metadata_to_minio(target_bucket, meta_result):
        
        output_path = meta_result['output_path']
        file_info = meta_result['file_info'] 
        file_name = "orders_" + file_info["timestamp"] + "_meta"
        
        # Conectando ao MinIO
        client = connect_minio()
        
        with open(output_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            
        content_bytes = json.dumps(json_data, indent=2, ensure_ascii=False).encode("utf-8")
        extension = ".json"
        
        byte_stream = io.BytesIO(content_bytes)
    
        client.put_object(
            target_bucket,
            f"{file_name}{extension}", 
            byte_stream,
            len(content_bytes)     
        )
    
        return "upload_complete"
    
    @task
    def t_finish_timer(start, upload_status):
        end = time.perf_counter()
        elapsed_time = end - start
        return elapsed_time

    start = t_start_timer()
    products = t_extract_data()
    file_info = t_generate(products, DATA_FOLDER)
    
    upload_status = t_send_order_to_minio(file_info, BUCKET)
    
    elapsed_time = t_finish_timer(start, upload_status)
    
    meta_result = t_meta_data(file_info, elapsed_time, "success", DATA_FOLDER)
    
    t_send_metadata_to_minio(BUCKET, meta_result)
    # # Enviar as ordens para o MinIO
    # t_send_order_to_minio(file_info, BUCKET)
    
    # # Gerar os metadados
    # elapsed_time = t_finish_timer(start)
    # meta_result = t_meta_data(file_info, elapsed_time, "success", DATA_FOLDER)
    # t_send_metadata_to_minio(file_info, BUCKET, meta_result)

orders_pipeline()
# Quebrar a pipeline em tasks (atualmente tem só 1 task)
# Adicionar a task de atualizar o banco de dados por procedure (exemplo de status - uploading to Minio, uploading to BigQuery )