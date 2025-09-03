from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env
load_dotenv()

clickhouse_user = os.getenv("CLICKHOUSE_USER")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
clickhouse_db = os.getenv("CLICKHOUSE_DB")


CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',  
    'port': 9000,
    'user': clickhouse_user,
    'password': clickhouse_password,
    'database': clickhouse_db
}

pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
engine_pg = pg_hook.get_sqlalchemy_engine()

def execute_ch_sql_file(sql_path):
    """Выполнение DDL в ClickHouse"""
    try:
        client = Client(**CLICKHOUSE_CONFIG)
        with open(sql_path, 'r') as f:
            query = f.read()
        client.execute(query)
        logging.info(f"SQL выполнен успешно: {sql_path}")
    except Exception as e:
        logging.error(f"Ошибка при выполнении SQL {sql_path}: {e}")
        raise


def transfer_table(sql_path: str, ch_table: str, batch_size: int = 10000):
    """ Перенос данных из Postgres (DDS) в ClickHouse. """
    try:
        # Читаем SQL-запрос из файла
        with open(sql_path, 'r') as f:
            sql_query = f.read()
        
        # Загружаем данные из Postgres в DataFrame
        df = pd.read_sql(sql_query, engine_pg)
        
        if df.empty:
            logging.info(f"Нет данных для переноса в {ch_table}")
            return

        # Обрабатываем None/NaN:
        # Строковые колонки → пустая строка
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('')

        # Подготовка данных для ClickHouse: список кортежей
        data = [tuple(x) for x in df.to_numpy()]
        columns = list(df.columns)
        
        client = Client(**CLICKHOUSE_CONFIG)

        # Batch insert для больших таблиц
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            insert_query = f"INSERT INTO {ch_table} ({', '.join(columns)}) VALUES"
            client.execute(insert_query, batch)
            logging.info(f"Загружено {len(batch)} строк в {ch_table} (batch {i//batch_size + 1})")

        logging.info(f"Полный перенос завершён: {len(data)} строк в {ch_table}")

    except Exception as e:
        logging.error(f"Ошибка при переносе данных в {ch_table}: {e}")
        raise