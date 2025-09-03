import pandas as pd
from pymongo import MongoClient
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData
from airflow.models import Variable

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к MongoDB
MONGO_URI =  Variable.get("mongo_uri")
DB_NAME = Variable.get("mongo_schema")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[DB_NAME]

# Подключение к Postgres через Airflow Hook
pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
engine = pg_hook.get_sqlalchemy_engine()


def transform_tourists(df):
    """Трансформация данных таблицы tourists"""
    df["registration_date"] = pd.to_datetime(df["registration_date"]).dt.date # Приводим даты к нужному формату.
    return df[['user_id', 'name', 'country', 'registration_date']] # Выбираем только те колонки, которые соответствуют таблице в Postgres

def transform_bookings(df):
    """Трансформация данных таблицы bookings"""
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    return df[['booking_id', 'user_id', 'destination', 'type', 'category', 'status', 'price', 'start_date', 'end_date']]

def transform_visits(df):
    """Трансформация данных таблицы visits"""
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
    return df[['visit_id', 'user_id', 'place_id', 'timestamp']]

def transform_searches(df):
    """Трансформация данных таблицы searches"""
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
    return df[['search_id', 'user_id', 'search_country', 'timestamp', 'converted_to_booking']]

def transform_pois(df):
    """Трансформация данных таблицы pois"""
    # Извлекаем широту из поля 'point'. 
    # Если 'point' является словарём, берём значение по ключу 'lat', иначе ставим None.
    df['latitude'] = df['point'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
    # Извлекаем долготу из поля 'point'.
    # Если 'point' является словарём, берём значение по ключу 'lon', иначе ставим None.
    df['longitude'] = df['point'].apply(lambda x: x.get('lon') if isinstance(x, dict) else None)
    df['poi_timestamp'] = pd.to_datetime(df['timestamp'])
    df['rating'] = df.get('rate') if 'rate' in df.columns else df.get('rating')
    return df[['xid', 'name', 'description', 'image', 'kinds', 'otm_url', 'country_code', 'latitude', 'longitude', 'poi_timestamp', 'rating']]

def transform_countries(df):
    """Трансформация данных таблицы countries"""
    # Извлекаем основное название страны.
    # В исходных данных 'name' — словарь с ключами 'common' (обычное название) и 'official' (официальное название).
    # Если 'name' является словарём, берём 'common', иначе None.
    df['country_name'] = df['name'].apply(lambda x: x.get('common') if isinstance(x, dict) else None)
    df['country_code'] = df['cca2']
    # Извлекаем широту из списка latlng.
    # Если 'latlng' — список и содержит хотя бы один элемент, берём первый (широту), иначе None.
    df['latitude'] = df['latlng'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    # Извлекаем долготу из списка latlng.
    # Если 'latlng' — список и содержит хотя бы два элемента, берём второй (долготу), иначе None.
    df['longitude'] = df['latlng'].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
    # Извлекаем столицу страны.
    # В исходных данных 'capital' — список (может быть несколько столиц), берём первую, иначе None.
    df['capital'] = df['capital'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    df['region'] = df['region']
    df['subregion'] = df['subregion']
    df['population'] = df['population']
    return df[['country_name', 'capital', 'country_code', 'latitude', 'longitude', 'region', 'subregion', 'population']]

def safe_insert(df, table_name, schema='ods', conflict_cols=None):
    """Безопасная вставка с защитой от конфликтов"""
    # Если DataFrame пустой — логируем предупреждение и выходим
    if df.empty:
        logging.warning(f"DataFrame для таблицы '{schema}.{table_name}' пуст.")
        return

    # Загружаем метаданные схемы из PostgreSQL
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=schema)

    # Получаем объект таблицы из метаданных
    table = Table(table_name, metadata, autoload_with=engine, schema=schema)

    # Формируем SQL-выражение INSERT с данными из DataFrame
    stmt = insert(table).values(df.to_dict(orient='records'))

    # Если указаны conflict_cols — добавляем ON CONFLICT DO NOTHING
    if conflict_cols:
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

    # Выполняем вставку в рамках транзакции
    with engine.begin() as conn:
        result = conn.execute(stmt)
        logging.info(f"Вставлено {result.rowcount} строк в '{schema}.{table_name}'")

def transfer_collection(collection_name, table_name, transform, schema='ods', conflict_cols=None):
    """Перенос данных из MongoDB в Postgres с защитой от дубликатов"""
    try:
        # Извлекаем все документы из MongoDB
        data = list(mongo_db[collection_name].find())
        if not data:
            logging.warning(f"Коллекция MongoDB '{collection_name}' пуста!")
            return
        # Преобразуем список документов в DataFrame
        df = pd.DataFrame(data)
        # Применяем функцию трансформации
        df = transform(df)

        if df.empty:
            logging.warning(f"После трансформации коллекция '{collection_name}' пуста.")
            return
        # Выполняем безопасную вставку в PostgreSQL
        safe_insert(df, table_name, schema=schema, conflict_cols=conflict_cols)
    except Exception as e:
        logging.error(f"Ошибка при загрузке '{collection_name}' → '{schema}.{table_name}': {e}")

def load_data():
    """Основная загрузка всех коллекций"""
    try:
        logging.info("🚀 Начинаем загрузку данных из MongoDB в Postgres ODS...")
        transfer_collection("synthetic_tourists", "tourists", transform_tourists, conflict_cols=["user_id"])
        transfer_collection("synthetic_bookings", "bookings", transform_bookings, conflict_cols=["booking_id"])
        transfer_collection("synthetic_visits", "visits", transform_visits, conflict_cols=["visit_id"])
        transfer_collection("synthetic_searches", "search_events", transform_searches, conflict_cols=["search_id"])
        transfer_collection("opentripmap_pois_detailed", "opentripmap_pois", transform_pois, conflict_cols=["id"])
        transfer_collection("rest_countries", "rest_countries", transform_countries, conflict_cols=["id"])
        logging.info("Загрузка завершена успешно!")
    except Exception as e:
        logging.exception(f"Ошибка в процессе загрузки: {e}")
    
