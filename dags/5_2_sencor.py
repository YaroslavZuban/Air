from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import json
import logging


dag = DAG(
    dag_id='5_2_sencor',
    schedule_interval=None,  # Запускать вручную
    start_date=days_ago(1),
    tags=['Домашнее_задание']
)

def response_operator(response, **kwargs):
    if not 'report_id' in response.json():
        return False
        
    data = response.json()
    report_id = data['report_id']
    
    logging.info(report_id)
        
    kwargs['ti'].xcom_push(key='report_id', value=report_id)
        
    return True
    
def response_sensor(response, **kwargs):
    if not 'message' in response.json():
        return False
    
    data = response.json()
    result = data['message']
    
    if result == 'The report is ready!':
        return True
    
    return False

# HTTP-оператор для отправки запроса на создание отчёта
start_report_task = SimpleHttpOperator(
    task_id='start_report_task',
    http_conn_id='report_api', 
    endpoint='start_report', 
    method='GET',
    response_check=response_operator,  # Проверка на наличие Report ID
    dag=dag,
)

# HTTP-сенсор для проверки готовности отчёта
check_report_task = HttpSensor(  
    task_id='check_report_task',
    http_conn_id='report_api',  # Укажите ваше соединение
    endpoint='check_report/{{ ti.xcom_pull(task_ids="start_report_task", key="report_id") }}',
    response_check=response_sensor, # Проверка что отчет готов
    method='GET',
    poke_interval=2,  
    timeout=60,  
    mode='poke',  
    dag=dag,
)

# Определение порядка выполнения задач
start_report_task >> check_report_task