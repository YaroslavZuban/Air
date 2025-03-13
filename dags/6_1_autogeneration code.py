import os
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago

# Этот код поможет вам получить доступ к файлам, по которым нужно пройтись
def read_sql_files():
    # Путь к папке с файлами
    folder_path = "./plugins"
    tasks = []

    # Проходимся по файлам и собираем задачи
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as file:
                file_content = file.read()
                tasks.append((file_name, file_content))
    
    return tasks

# Создаем DAG
dag = DAG(
    '6_1_autogeneration_code',  # Имя DAG без пробелов
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['Домашнее_задание']
)

# Читаем SQL-файлы
sql_tasks = read_sql_files()

# Создаем задачи
previous_task = None
for file_name, sql in sql_tasks:
    # Убираем расширение файла для использования в task_id и имени представления
    task_name = os.path.splitext(file_name)[0]
    
    task = ClickHouseOperator(
        task_id=f'task_{task_name}',  # task_id должен быть уникальным
        sql=f"CREATE OR REPLACE VIEW {task_name} AS {sql}",  # Создаем или заменяем представление
        clickhouse_conn_id='clickhouse_default',
        dag=dag
    )
    
    # Устанавливаем зависимости между задачами (если нужно)
    if previous_task:
        previous_task >> task
    previous_task = task