from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException # Нужно для вызова исключений в Airflow
import pandas as pd
import requests
import json

# Настройка подключения к базе данных ClickHouse
HOST = BaseHook.get_connection("clickhouse_default").host
USER = BaseHook.get_connection("clickhouse_default").login
PASSWORD = BaseHook.get_connection("clickhouse_default").password
DATABASE = BaseHook.get_connection("clickhouse_default").schema

CH_CLIENT = Client(
    host=HOST,  # IP-адрес сервера ClickHouse
    user=USER,  # Имя пользователя для подключения
    password=PASSWORD,  # Пароль для подключения
    database=DATABASE  # База данных, к которой подключаемся
)

# Создадим объект класса DAG
dag =  DAG('test_xcom', schedule_interval='@daily', start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 4), tags=['Домашнее_задание'])

def fetch_data_to_xcom(api_url, **kwargs):
    
    # Получаем все файлы из Xcom на вчера, если такого ключа нет, то будет возвращен None
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(key='files_full', include_prior_dates=True) # Данный параметр включает доступ к Xcom для всех предыдущих запусков, из одинаковых ключей будет выбран последний ключ по времени
    
    # Получаем данные с сервера
    response = requests.get(api_url + kwargs['ds'])  
    
    if response.status_code == 200:
        data = response.json()
        arr_file = data['files']
        
        if not files is None:
            data_dif = len(arr_file) - len(files) 
            
            task_instance.xcom_push(key = 'file_difference', value=data_dif)
        
        task_instance.xcom_push(key = 'files_full', value=arr_file)
    else:
       raise AirflowException(f"Request failed {response.status_code}")
    
        
# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(url, table_name, client, **kwargs):
    
    # Получаем разницу в файлах сегодня и вчера 
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(key='files_full', include_prior_dates=True)

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (campaign String, cost Int64, date  String) ENGINE Log')
    
    # Итеративно проходимся по файлам и добавляем в ClickHouse
    for file in files:        
        # Чтение данных из CSV
        data_frame = pd.read_csv(url + file)  

        # Запись data frame в ClickHouse
        client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records')) 

fetch_data_to_xcom = PythonOperator(
    task_id='fetch_data_to_xcom',
    python_callable=fetch_data_to_xcom,
    op_args = ['http://158.160.116.58:4009/files/'],
    dag=dag,
)

# Задачи для загрузки данных 
upload_to_clickhouse = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    op_args = ['http://158.160.116.58:4009/download/', 'campaign_table', CH_CLIENT],
    dag=dag,
)


fetch_data_to_xcom >> upload_to_clickhouse