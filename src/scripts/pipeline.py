import requests
import os
from datetime import datetime
import logging
import json
import pandas as pd
import random
import uuid
from datetime import datetime as dt
from datetime import timedelta
import subprocess
import time
import io
from utils.minio_connection import connect_minio

def extract_data():
    response = requests.get('https://fakestoreapi.com/products')
    if response.status_code == 200:
        products_list = response.json()
    else:
        products_list = []
    return products_list

def generate_fake_data(products_list, data_folder):
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
    
    output_path = os.path.join(data_folder, "raw", file_name + ".csv")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8")
        
    # Saída Bronze
    return df, file_name

def meta_data(df, file_name, elapsed_time, status, data_folder):
    
    try:
        output_path = os.path.join(data_folder, "raw", file_name + "_meta.json")
        profile_id = 1
        df_row_count = len(df)
        price_currency = "BRL"
        source_name = "Fake Store API"
        automation_status = status
        environment = "dev"
        duration = elapsed_time
        timestamp = dt.now().strftime("%d-%m-%Y - %H:%M:%S")
        
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
            
        return True
    
    except Exception as e:
        print(f"An error was found while building the meta data {e}")
        return False

def prepare_data(df, file_name, data_folder):
    
    df = df.astype(
        {
        'order_id': str,
        'product_id': int,
        'product_title': str,
        'price': float, 
        'description': str,
        'category': str,
        'qty': int
        }
    )
    
    df["created_at"] = pd.to_datetime(
        df['created_at'],
        format ="%d-%m-%Y - %H:%M:%S",
        errors='coerce'
    )
    
    if (df["price"] < 0).any():
        linhas_invalidas = df[df["price"] < 0]
        raise ValueError(f"Preços negativos encontrados:\n{linhas_invalidas}")
    
    df["total_value"] = (df["qty"] * df["price"]).round(2)
    
    df = df[df["qty"] > 0]

    df['category'] = df['category'].str.lower()
    
    output_path = os.path.join(data_folder, "processed", file_name + ".csv")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding="utf-8")
    
    # Saída Silver
    return df

def send_to_minio(data, client, target_bucket, file_format="csv"):
    """
    Envia dados para o MinIO em formato CSV ou JSON
    
    Args:
        data: DataFrame (para csv) ou dict/list (para json)
        client: Cliente MinIO
        target_bucket: Nome do bucket
        file_format: "csv" ou "json"
    """
    
    if file_format == "csv":
        file_name = "orders_" + data["created_at"].unique()[0]
        
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer, index=False)
        content_bytes = csv_buffer.getvalue().encode("utf-8")
        extension = ".csv"
        
    elif file_format == "json":
        # Se for DataFrame, converte para dict
        if hasattr(data, 'to_dict'):
            json_data = data.to_dict(orient='records')
            file_name = "orders_" + data["created_at"].unique()[0]
        else:
            json_data = data
            file_name = "orders_" + str(datetime.now().strftime("%d-%m-%Y - %H:%M:%S")) +"_meta"
        
        content_bytes = json.dumps(json_data, indent=2, ensure_ascii=False).encode("utf-8")
        extension = ".json"
    
    else:
        raise ValueError("file_format deve ser 'csv' ou 'json'")
    
    byte_stream = io.BytesIO(content_bytes)
    
    client.put_object(
        target_bucket,
        f"{file_name}{extension}", 
        byte_stream,
        len(content_bytes)     
    )
    
def run_pipeline(data_folder):
    
    start = time.perf_counter()
    #random.seed(50)
    client = connect_minio()
    status = "FAILED"
    
    df_products = None
    file_name = None

    try:
        products_list = extract_data()

        if not products_list:
            raise ValueError("A API não retornou produtos; abortando pipeline.")

        df_products, file_name = generate_fake_data(products_list, data_folder)
        
        # Sending CSV
        send_to_minio(df_products, client, "bronze-layer", file_format="csv")
        
        #prepare_data(df_products, file_name, data_folder)

        status = "SUCCESS"

    except Exception as e:
        error_message = str(e)
        status = "FAILED"
        print(error_message)
        
    finally:
        end = time.perf_counter()
        elapsed_time = end - start
        meta_data(df_products, file_name, elapsed_time, status, data_folder)
        meta = meta_data(df_products, file_name, elapsed_time, status, data_folder)
        
        send_to_minio(meta, client, "bronze-layer", file_format="json")