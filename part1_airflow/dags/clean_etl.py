#part1_airflow/dags/cleant_etl.py

from airflow import DAG
import pendulum
from steps.clean_etl import create_table, extract, transform, load
from airflow.operators.python import PythonOperator
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='yandex-flats-cleandata',
    schedule='@once',
    start_date=pendulum.now('UTC'),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    catchup=False,
    tags=["ETL"]
) as dag:
    create_table_step = PythonOperator(task_id = 'create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_table_step >> extract_step >> transform_step >> load_step
