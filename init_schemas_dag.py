from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from utils.notifier import notify

default_args = {
    'owner': 'Kate',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
}

with DAG(
    dag_id="init_schemas_dag",
    description='Создание таблиц в ods и dds слоях в PostgreSQL',
    start_date=datetime(2025, 8, 1),
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
) as dag:

    create_ods = PostgresOperator(
        task_id="create_ods",
        postgres_conn_id="my_postgres_conn",
        sql="sql/create_ods.sql",
        on_success_callback=notify,  # уведомление при успехе
        on_failure_callback=notify   # уведомление при ошибке
    )

    create_dds = PostgresOperator(
        task_id="create_dds",
        postgres_conn_id="my_postgres_conn",
        sql="sql/create_dds.sql",
        on_success_callback=notify,  
        on_failure_callback=notify   
    )

    create_ods >> create_dds