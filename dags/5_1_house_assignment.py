from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.exceptions import AirflowException
from datetime import datetime

# Функция, которая вызывает исключение
def raise_exc():
    raise AirflowException("This task is designed to fail.")

# Функция для отправки сообщения в Telegram через on_failure_callback
def notify_failure(context):
    task_instance = context['task_instance']
    error_message = f"Task {task_instance.task_id} failed with exception: {context['exception']}"
    
    # Используем TelegramOperator для отправки сообщения
    telegram_op = TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='telegram_id',  # Укажите ваш conn_id
        #chat_id='YOUR_CHAT_ID',  # Укажите chat_id, если он не указан в соединении
        text=error_message
    )
    telegram_op.execute(context)

# Аргументы DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'on_failure_callback': notify_failure  # Указываем callback для провала
}

# Создание DAG
with DAG('Telegram', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    # Задача, которая всегда завершается с ошибкой
    fail_task = PythonOperator(
        task_id='always_fail_task',
        python_callable=raise_exc
    )

    fail_task