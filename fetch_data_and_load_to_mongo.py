from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import sys
import os

from utils.extract_travel_data import run_extraction
from utils.notifier import notify

# Добавляем путь к utils, чтобы импортировать run_extraction
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../utils")
sys.path.append(os.path.abspath(SCRIPT_PATH))


default_args = {
    'owner': 'Kate',
    'depands_on_past': False,
    'start_date': datetime(2025, 8, 5),
}


with DAG(
    'Fetch_data_and_load_to_mongo',
    description='Извлечение туристических данных из API в MongoDB',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=run_extraction,
        on_success_callback=notify,  # уведомление при успехе
        on_failure_callback=notify   # уведомление при ошибке
    )

    extract_data