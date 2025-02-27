# Импортируем необходимые библиотеки
import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from clickhouse_driver import Client  # для подключения к ClickHouse

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
from dotenv import load_dotenv
import json
import re
import logging


load_dotenv()

KEY_API = os.getenv('KEY_API')
HOST = os.getenv('HOST')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DATABASE = os.getenv('DATABASE')

CH_CLIENT = Client(
    host='clickhouse', 
    user='user',  
    password='password', 
    database='default',  
)

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, s_file, currency, start_date, end_date,**kwargs):
    url_get = f"{url}?access_key=422bab0c9e08a5476912061877017b8d&source={currency}"\
                      f"&start_date={start_date}" \
                      f"&end_date={end_date}"
    
    logging.info(url_get)
    
    request = req.get(url_get)
    
    if request.status_code != 200:
        print(f"Произошла ошибка: статус-код {request.status_code}")
    else:
        print("Ответ API:", request.text)  # Вывод ответа API в лог
        with open(s_file, "w", encoding="utf-8") as tmp_file:
            tmp_file.write(request.text)

def transform_data(s_file, csv_file, **kwargs):
    if not os.path.exists(s_file):
        raise FileNotFoundError(f"Файл {s_file} не найден.")

    with open(s_file, 'r') as file:
        request_json = json.load(file)

    # Проверка наличия ключей
    required_keys = ["source", "quotes"]
    for key in required_keys:
        if key not in request_json:
            raise KeyError(f"Ключ '{key}' отсутствует в JSON-файле: {s_file}")

    result_list = []
    current = request_json["source"]
    pattern = rf'{current}([A-Z]+)'

    for date, quotes in request_json['quotes'].items():
        for currency, value in quotes.items():
            match = re.match(pattern, currency)
            if match:
                result_object = {
                    "source": current,
                    "currency": match.group(1),
                    "value": float(value),
                    "date": date
                }
                result_list.append(result_object)

    with open(csv_file, 'w') as output_file:
        output_file.write("source,currency,value,date\n")
        for obj in result_list:
            output_line = f"{obj['source']},{obj['currency']},{obj['value']},{obj['date']}\n"
            logging.info(output_line)
            output_file.write(output_line)

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client, **kwargs):
    data_frame = pd.read_csv(csv_file)  
    
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (source String, currency String, value Float, date String) ENGINE Log')
    
    client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))

with DAG(
    dag_id='3_4_2_homework_assignment',
    schedule_interval = '@daily',
    start_date = datetime(2024,1,1),
    end_date = datetime(2024,1,11),
    max_active_runs= 1,
    tags=['Домашнее_задание']
) as dag:
    # Операторы для выполнения шагов
    extract_task = PythonOperator(
        task_id = 'extract_task',
        python_callable=extract_data,
        op_kwargs={
            "url":"https://api.exchangerate.host/timeframe",
            "s_file":"extracted_data.json",
            "currency": "USD",
            "start_date":"{{  ds  }}",
            "end_date":"{{  ds  }}"
        },
        dag=dag
    )

    transform_task = PythonOperator(
        task_id = 'transform_data_task',
        python_callable=transform_data,
        op_kwargs={
            "s_file":"extracted_data.json",
            "csv_file":"extracted_data_csv.csv"
        },
        dag=dag
    )

    upload_task = PythonOperator(
        task_id = 'upload_to_clickhouse_task',
        python_callable=upload_to_clickhouse,
        op_kwargs={
            "csv_file":"extracted_data_csv.csv",
            "table_name":"currency",
            "client":CH_CLIENT
        },
        dag=dag
    )

    # Определение зависимостей между задачами
    extract_task >> transform_task >> upload_task
