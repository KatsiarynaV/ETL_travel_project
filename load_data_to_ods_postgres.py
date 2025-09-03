from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.load_to_postgres_ods import load_data
from utils.notifier import notify
import sys
import os

# Добавляем путь к utils, чтобы импортировать run_extraction
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../utils")
sys.path.append(os.path.abspath(SCRIPT_PATH))


default_args = {
    'owner': 'Kate',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
}

with DAG(
    dag_id='load_from_mongo_to_postgres_ods',
    description='Загрузка данных из MongoDB в слой ods в PostgreSQL',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    load_mongo_data = PythonOperator(
        task_id='load_data_from_mongo',
        python_callable=load_data,
        on_success_callback=notify,  # уведомление при успехе
        on_failure_callback=notify   # уведомление при ошибке
    )

    load_mongo_data