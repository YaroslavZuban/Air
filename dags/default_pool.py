from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Создаем DAG
dag = DAG(
    'parallel_tasks_example',
    schedule_interval=None,  # Запускать вручную
    start_date=datetime(2024, 1, 1),
)

# Задачи с ограничением на параллельное выполнение через пул
task_1 = BashOperator(
    task_id='task_1',
    pool='non_default_pool',  # Указываем пул для ограничения параллельности
    bash_command="sleep 10",
    dag=dag,
)

task_2 = BashOperator(
    task_id='task_2',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_3 = BashOperator(
    task_id='task_3',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_4 = BashOperator(
    task_id='task_4',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_5 = BashOperator(
    task_id='task_5',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_6 = BashOperator(
    task_id='task_6',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_7 = BashOperator(
    task_id='task_7',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_8 = BashOperator(
    task_id='task_8',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_9 = BashOperator(
    task_id='task_9',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)

task_10 = BashOperator(
    task_id='task_10',
    pool='non_default_pool',
    bash_command="sleep 10",
    dag=dag,
)