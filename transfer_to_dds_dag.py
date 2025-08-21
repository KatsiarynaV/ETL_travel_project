from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.notifier import notify
import sys
import os

# Чтобы импортировать скрипт 
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../utils")
sys.path.append(os.path.abspath(SCRIPT_PATH))
from utils.transfer_to_dds_postgres import load_all_to_dds


default_args = {
    'owner': 'Kate',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
}


with DAG(
    dag_id="transfer_from_ods_to_dds_postgres",
    description='Перенос данных из ods в dds слой в PostgreSQL',
    start_date=datetime(2025, 8, 1),
    default_args=default_args,
    schedule_interval="@daily",  
    catchup=False,
) as dag:

    transfer_data_to_dds = PythonOperator(
        task_id="transfer_data_to_dds",
        python_callable=load_all_to_dds,
        on_success_callback=notify,  # уведомление при успехе
        on_failure_callback=notify   # уведомление при ошибке
    )


    transfer_data_to_dds 