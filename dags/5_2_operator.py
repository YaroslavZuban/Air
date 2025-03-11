from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime

# Создаем DAG
dag = DAG(
    'clickhouse_operator_example',
    description='Пример использования ClickHouseOperator',
    schedule_interval='@daily',  # Запускать вручную
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 4),
    tags=['Домашнее_задание']
)


# Оператор для создания таблицы
create_table = ClickHouseOperator(
    task_id='create_table',
    sql='CREATE TABLE IF NOT EXISTS currency_sql (date String, currency_source String, currency String, value Float) ENGINE Log',
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)


# SQL-запрос, вставка данных в нашу таблицу после того как мы их выгрузим из API
sql_query = """ INSERT INTO currency_sql
SELECT '{{ds}}' AS date, currency_source, currency, value   
FROM (
    SELECT arrayJoin(splitByChar(',', replaceAll(replaceAll(replaceAll(visitParamExtractRaw(column, '{{ds}}'), '{', ''), '}', ''), '"', ''))) AS col1,
           LEFT(col1, 3) AS currency_source,
           SUBSTRING(col1, 4, 3) AS currency,
           SUBSTRING(col1, 8) AS value
    FROM url('https://api.exchangerate.host/timeframe?access_key=422bab0c9e08a5476912061877017b8d&source=USD&start_date={{ds}}&end_date={{ds}}'
  , LineAsString, 'column String')
)
   
"""



# Оператор для вствыки данных в таблицу
insert_data = ClickHouseOperator(
    task_id='insert_data',
    sql=sql_query,  # SQL запрос, который нужно выполнить
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)

create_table >> insert_data