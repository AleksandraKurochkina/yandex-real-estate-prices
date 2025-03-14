# part1_airflow/dags/etl.py

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from steps.etl import create_table, extract, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='yandex-flats',
    schedule='@once',
    start_date=pendulum.now('UTC'),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    catchup=False,
    tags=["ETL"]
) as dag:
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_data_step = PythonOperator(task_id='extract', python_callable=extract)
    load_step = PythonOperator(task_id='load', python_callable=load)
    create_table_step >> extract_data_step >> load_step