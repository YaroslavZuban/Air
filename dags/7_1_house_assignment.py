# Библиотеки для работы с XML, http, data frame и date
import requests as req
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
from clickhouse_driver import Client

from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# Библиотеки для работы с Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Настройка подключения к базе данных ClickHouse

api_url = Variable.get("api_url")

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, date, s_file):
    
    # Выполняем GET-запрос для получения данных за указанную дату
    request = req.get(f"{url}?date_req={date}")  
    with open(s_file, "w", encoding="utf-8") as tmp_file:
        tmp_file.write(request.text)  # Записываем текст ответа в файл

# Функция для обработки данных в формате XML и преобразования их в CSV
def transform_data(s_file, csv_file, date):
    
    rows = list()  # Список для хранения значений из XML
    
    # Парсинг XML дерева
    parser = ET.XMLParser(encoding="utf-8")
    tree = ET.parse(s_file, parser=parser).getroot()
    
    # Получение необходимых значений
    for child in tree.findall("Valute"):
        num_code = child.find("NumCode").text
        char_code = child.find("CharCode").text
        nominal = child.find("Nominal").text
        name = child.find("Name").text
        value = child.find("Value").text
        
        # Добавление одной записи в список для последующих преобразований
        rows.append((num_code, char_code, nominal, name, value)) 

    # Считывание полученного списка в Data Frame, добавление даты и запись в CSV файл
    data_frame = pd.DataFrame(
        rows, columns=["num_code", "char_code", "nominal", "name", "value"]
    )
    data_frame['date'] = date 
    data_frame.to_csv(csv_file, sep=",", encoding="utf-8", index=False) 

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name):
    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)  
    click_db = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    click_db.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records')) 

# Определяем DAG, это контейнер для описания нашего пайплайна
dag = DAG(
    '7_house_assignment',
    schedule_interval='@daily',      
    start_date=datetime(2024,1,1),
    end_date=datetime(2024,1,5),
    max_active_runs=1, 
    tags=['Домашнее_задание']
)

# Задача для извлечения данных 
task_extract = PythonOperator(
    task_id='extract_data',          # Уникальное имя задачи
    python_callable=extract_data,    # Функция которая будет запущена (определена выше)
    # Параметры в виде списка которые будут переданы в функцию "extract_data"
    op_args=[api_url, '{{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}', './extracted_data.xml'],
    dag=dag,                         # DAG к которому приклеплена задача
)

# Задачи для преобразования данных 
task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    # Передача аргументов через словарь, а не список
    op_kwargs = {
        's_file': './extracted_data.xml', 
        'csv_file': './transformed_data.csv', 
        'date': '{{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}'},
    dag=dag,
)

create_order_usd = ClickHouseOperator(
    task_id='create_order_usd',
    sql='CREATE TABLE IF NOT EXISTS order_usd (date String, order_id Int64, purchase_rub Float64, purchase_usd Float64) ENGINE Log',
    clickhouse_conn_id='clickhouse_default', 
    dag=dag,
)

# Оператор для выполнения запроса
create_currency_data = ClickHouseOperator(
    task_id='create_currency_data',
    sql='CREATE TABLE IF NOT EXISTS currency_data (num_code Int64, char_code String, nominal Int64, name String, value String, date String) ENGINE Log',
    clickhouse_conn_id='clickhouse_default', 
    dag=dag,
)

# Задачи для загрузки данных 
task_upload = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    op_args = ['./transformed_data.csv', 'currency_data'],
    dag=dag,
)


insert_join = ClickHouseOperator(
    task_id='insert_join',
    sql=""" INSERT INTO order_usd 
            SELECT date, order_id, purchase_rub, purchase_rub / toFloat64(replace(value, ',', '.')) as purchase_usd
            FROM airflow.orders t1
                left join (select * from currency_cb where name = 'Доллар США' and date = '{{ds}}') t2 on t1.date = t2.date
            where date = '{{ds}}' """,
    clickhouse_conn_id='clickhouse_default', 
    dag=dag,
)

# Связываем задачи в соответствующих дагах. Посмотреть связь можно здесь 
task_extract >> task_transform >> [ create_currency_data, create_order_usd] >> task_upload >> insert_join