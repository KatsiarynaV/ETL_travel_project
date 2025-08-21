import pandas as pd
from pymongo import MongoClient
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from airflow.models import Variable

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к MongoDB
MONGO_URI =  Variable.get("mongo_uri")
DB_NAME = Variable.get("travel_raw")

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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df[['visit_id', 'user_id', 'place_id', 'timestamp']]

def transform_searches(df):
    """Трансформация данных таблицы searches"""
    df["timestamp"] = pd.to_datetime(df["timestamp"])
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
    return df[['xid', 'name', 'description', 'image', 'kinds', 'otm_url', 'country_code', 'latitude', 'longitude', 'poi_timestamp']]

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

def transfer_collection(collection_name, table_name, transform, schema='ods'):
    """Перенос данных из MongoDB в Postgres"""
    try:
        # Извлекаем все документы из коллекции MongoDB и преобразуем в список
        data = list(mongo_db[collection_name].find())
        # Если коллекция пустая, выводим предупреждение и прекращаем выполнение функции
        if not data:
            logging.warning(f"Коллекция MongoDB '{collection_name}' пуста!")
            return
        # Создаём DataFrame из списка словарей (документов MongoDB)
        df = pd.DataFrame(data)
        # Применяем функцию трансформации, чтобы привести данные к нужной структуре для Postgres
        df = transform(df)
        # Загружаем данные в PostgreSQL через SQLAlchemy
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        logging.info(f"Данные из '{collection_name}' успешно загружены в таблицу '{schema}.{table_name}' ({len(df)} записей).")
    except Exception as e:
        logging.error(f"Ошибка при загрузке коллекции '{collection_name}' в таблицу '{schema}.{table_name}': {e}")

def load_data():
    """Основная загрузка всех коллекций"""
    try:
        logging.info("Начинаем загрузку данных из MongoDB в Postgres ODS...")
        transfer_collection("synthetic_tourists", "tourists", transform_tourists)
        transfer_collection("synthetic_bookings", "bookings", transform_bookings)
        transfer_collection("synthetic_visits", "visits", transform_visits)
        transfer_collection("synthetic_searches", "search_events", transform_searches)
        transfer_collection("opentripmap_pois_detailed", "opentripmap_pois", transform_pois)
        transfer_collection("rest_countries", "rest_countries", transform_countries)
        logging.info("Загрузка завершена успешно!")
    except Exception as e:
        logging.exception(f"Ошибка в процессе генерации: {e}")
    
