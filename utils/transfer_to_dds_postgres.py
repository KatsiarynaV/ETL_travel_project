import pandas as pd

import logging
import random
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from .pydantic_models import (
    RestCountryModel, POIModel, TouristModel,
    BookingModel, VisitModel, SearchEventModel
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
engine = pg_hook.get_sqlalchemy_engine()


def clean_and_validate(df: pd.DataFrame, model, drop_duplicates_by=None, not_null_fields=None, custom_clean_func=None):
    """Очистка и валидация данных через Pydantic"""

    # Применяем кастомную функцию очистки, если она задана
    if custom_clean_func:
        df = custom_clean_func(df)

    valid_records = []
    for _, row in df.iterrows():
        try:
            # Проверяем каждую строку через Pydantic-модель
            record = model(**row.to_dict())
            valid_records.append(record.dict()) # Если успешно — добавляем в список
        except Exception as e:
            # Если валидация не прошла — пишем предупреждение и пропускаем запись
            logging.warning(f"Отброшена строка {row.to_dict()} → {e}")

    # Собираем обратно валидные записи в DataFrame
    df_clean = pd.DataFrame(valid_records)

    # Удаление дублей по заданным полям (например, country_code, xid, user_id)
    if drop_duplicates_by:
        df_clean = df_clean.drop_duplicates(subset=drop_duplicates_by)

    # Удаление строк, где обязательные поля = NULL (например, user_id, booking_id)
    if not_null_fields:
        df_clean = df_clean.dropna(subset=not_null_fields)

    return df_clean

def get_random_xids():
    """Получает список всех xid из ods.opentripmap_pois"""
    query = "SELECT xid FROM ods.opentripmap_pois"
    with engine.connect() as conn:
        df_xid = pd.read_sql(query, conn)
    xids = df_xid["xid"].dropna().tolist()
    logging.info(f"Получено {len(xids)} xid для рандомной замены")
    return xids

def clean_visits(df_visits: pd.DataFrame):
    """Заменяет place_id на случайный xid"""
    xids = get_random_xids()
    if not xids:
        logging.warning("Список xid пуст — замена невозможна")
        return df_visits

    df_visits["place_id"] = [random.choice(xids) for _ in range(len(df_visits))]
    return df_visits

def filter_existing_records(df: pd.DataFrame, table: str, key_fields: list):
    """Удаляет строки из df, которые уже существуют в целевой таблице по ключевым полям"""
    if not key_fields:
        return df

    with engine.connect() as conn:
        keys_str = ", ".join(key_fields)
        query = f"SELECT {keys_str} FROM dds.{table}"
        existing_df = pd.read_sql(query, conn)

    # Приводим ключевые поля к строковому типу для сравнения
    for field in key_fields:
        df[field] = df[field].astype(str)
        existing_df[field] = existing_df[field].astype(str)

    # Удаляем строки, которые уже есть в целевой таблице
    merged = df.merge(existing_df, on=key_fields, how="left", indicator=True)
    df_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    return df_new


def load_table(sql_query: str, model, table: str, drop_duplicates_by=None, not_null_fields=None, custom_clean_func=None):
    with engine.connect() as conn:
        # Загружаем данные из ODS с помощью SQL-запроса
        df = pd.read_sql(sql_query, conn)

    # Чистим и валидируем через Pydantic
    df_clean = clean_and_validate(df, model, drop_duplicates_by, not_null_fields, custom_clean_func)

    # Фильтруем уже существующие записи в целевой таблице
    df_new = filter_existing_records(df_clean, table, drop_duplicates_by)

    if not df_new.empty:
        df_new.to_sql(table, engine, schema="dds", if_exists="append", index=False)
        logging.info(f"Загружено {len(df_new)} новых строк в dds.{table}")
    else:
        logging.info(f"Нет новых строк для загрузки в dds.{table}")


def load_all_to_dds():
    """Перенос всех таблиц из ODS в DDS"""
    logging.info("=== Начинаем перенос из ODS в DDS ===")

    load_table("SELECT * FROM ods.rest_countries",
               RestCountryModel, "rest_countries",
               drop_duplicates_by=["country_code"], not_null_fields=["country_code"])

    load_table("SELECT * FROM ods.opentripmap_pois",
               POIModel, "opentripmap_pois",
               drop_duplicates_by=["xid"], not_null_fields=["xid"])

    load_table("SELECT * FROM ods.tourists",
               TouristModel, "tourists",
               drop_duplicates_by=["user_id"], not_null_fields=["user_id", "name"])

    load_table("SELECT * FROM ods.bookings",
               BookingModel, "bookings",
               drop_duplicates_by=["booking_id"], not_null_fields=["booking_id", "user_id"])

    load_table("SELECT * FROM ods.visits",
                VisitModel, "visits",
                drop_duplicates_by=["visit_id"], not_null_fields=["visit_id", "user_id"],
                custom_clean_func=clean_visits)

    load_table("SELECT * FROM ods.search_events",
               SearchEventModel, "search_events",
               drop_duplicates_by=["search_id"], not_null_fields=["search_id", "user_id"])

    logging.info("=== Перенос завершён успешно! ===")
