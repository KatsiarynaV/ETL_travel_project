from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.notifier import notify
from datetime import datetime
import sys
import os

SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../utils")
sys.path.append(os.path.abspath(SCRIPT_PATH))

from utils.load_to_clickhouse_functions import execute_ch_sql_file, transfer_table

default_args = {
    'owner': 'Kate',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
}

with DAG(
    dag_id='dds_to_ch_dag',
    description='Перенос таблиц DDS в ClickHouse',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

   # Создание таблиц в ClickHouse
    create_tourists = PythonOperator(
        task_id="create_tourists",
        python_callable=execute_ch_sql_file,
        op_args=["/opt/airflow/dags/sql/ch_create_tourists.sql"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    create_pois = PythonOperator(
        task_id="create_pois",
        python_callable=execute_ch_sql_file,
        op_args=["/opt/airflow/dags/sql/ch_create_pois.sql"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    create_bookings = PythonOperator(
        task_id="create_bookings",
        python_callable=execute_ch_sql_file,
        op_args=["/opt/airflow/dags/sql/ch_create_bookings.sql"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    create_visits = PythonOperator(
        task_id="create_visits",
        python_callable=execute_ch_sql_file,
        op_args=["/opt/airflow/dags/sql/ch_create_visits.sql"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    create_searches = PythonOperator(
        task_id="create_searches",
        python_callable=execute_ch_sql_file,
        op_args=["/opt/airflow/dags/sql/ch_create_searches.sql"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    # Перенос данных
    load_tourists = PythonOperator(
        task_id="load_tourists",
        python_callable=transfer_table,
        op_args=["/opt/airflow/dags/sql/ch_insert_tourists.sql", "tourists"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    load_pois = PythonOperator(
        task_id="load_pois",
        python_callable=transfer_table,
        op_args=["/opt/airflow/dags/sql/ch_insert_pois.sql", "pois"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    load_bookings = PythonOperator(
        task_id="load_bookings",
        python_callable=transfer_table,
        op_args=["/opt/airflow/dags/sql/ch_insert_bookings.sql", "bookings"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    load_visits = PythonOperator(
        task_id="load_visits",
        python_callable=transfer_table,
        op_args=["/opt/airflow/dags/sql/ch_insert_visits.sql", "visits"],
        on_success_callback=notify,
        on_failure_callback=notify
    )

    load_searches = PythonOperator(
        task_id="load_searches",
        python_callable=transfer_table,
        op_args=["/opt/airflow/dags/sql/ch_insert_searches.sql", "searches"],
        on_success_callback=notify,
        on_failure_callback=notify
    )


create_tourists >> create_pois >> create_bookings >> create_visits >> create_searches
[load_tourists, load_pois, load_bookings, load_visits, load_searches] << create_searches