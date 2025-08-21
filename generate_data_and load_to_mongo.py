from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import sys
import os
from utils.generate_data import run_generation
from utils.notifier import notify

# Добавляем путь к utils, чтобы импортировать run_extraction
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../utils")
sys.path.append(os.path.abspath(SCRIPT_PATH))


default_args = {
    'owner': 'Kate',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
}


with DAG(
    'Generate_data_and_load_to_mongo',
    description='генерация туристических данных и загрузка в MongoDB',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=run_generation,
        on_success_callback=notify,  # уведомление при успехе
        on_failure_callback=notify   # уведомление при ошибке
    )


    generate_data 