import random
from datetime import datetime, timedelta
from faker import Faker
from uuid import uuid4
from pymongo import MongoClient, errors
import logging
from airflow.models import Variable


# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Инициализация генератора фейковых данных
faker = Faker()

# Конфигурация подключения к MongoDB
MONGO_URI =  Variable.get("mongo_uri")
DB_NAME = Variable.get("travel_raw")


def get_mongo_conn():
    """Создает соединение с MongoDB и возвращает объект базы данных"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Проверка подключения
        client.admin.command('ping')
        logging.info("Успешное подключение к MongoDB")
        return client[DB_NAME]
    except errors.ServerSelectionTimeoutError as e:
        logging.error(f"Ошибка подключения к MongoDB: {e}")
        raise

def generate_tourists():
    """Генерация синтетических туристов"""
    tourists = []
    for _ in range(200):
        tourists.append({
            "user_id": str(uuid4()),
            "name": faker.name(),
            "country": faker.country(),
            "registration_date": faker.date_between(start_date='-2y', end_date='today').isoformat(),
        })
    return tourists

def generate_bookings(tourists):
    """Генерация синтетических бронирований"""
    bookings = []
    for _ in range(900):
        tourist = random.choice(tourists)
        start = faker.date_between(start_date='-1y', end_date='today')
        duration_days = random.randint(3, 14)
        end = start + timedelta(days=duration_days)
        bookings.append({
            "booking_id": str(uuid4()),
            "user_id": tourist["user_id"],
            "destination": faker.country(),
            "type": random.choice(["flight", "hotel", "tour"]),
            "category": random.choice(['beach', 'museum', 'city', 'nature', 'history', 'shopping', 'relax']),
            "status": random.choice(["confirmed", "cancelled"]),
            "price": round(random.uniform(100, 3000), 2),
            "start_date": start.isoformat(),
            "end_date": end.isoformat()
        })
    return bookings

def generate_visits(tourists):
    """Генерация синтетических визитов к достопримечательностям"""
    visits = []
    for _ in range(2500):
        visits.append({
            "visit_id": str(uuid4()),
            "user_id": random.choice(tourists)["user_id"],
            "place_id": str(uuid4()),
            "timestamp": faker.date_time_between(start_date='-1y', end_date='now').isoformat()
        })
    return visits

def generate_search_events(tourists):
    """Генерация поисковых событий от туристов"""
    searches = []
    for _ in range(5000):
        searches.append({
            "search_id": str(uuid4()),
            "user_id": random.choice(tourists)["user_id"],
            "search_country": faker.country(),
            "timestamp":faker.date_time_between(start_date='-1y', end_date='now').isoformat(),
            "converted_to_booking": random.choices([True, False], weights=[0.3, 0.7])[0]
        })
    return searches


def run_generation():
    """Основной процесс генерации и загрузки всех данных в MongoDB"""
    try:
        db = get_mongo_conn()

        logging.info("Генерация туристов...")
        tourists = generate_tourists()
        db.synthetic_tourists.drop()
        db.synthetic_tourists.insert_many(tourists)
        logging.info(f"Туристы: {len(tourists)} записей загружено.")

        logging.info("Генерация бронирований...")
        bookings = generate_bookings(tourists)
        db.synthetic_bookings.drop()
        db.synthetic_bookings.insert_many(bookings)
        logging.info(f"Бронирования: {len(bookings)} записей загружено.")

        logging.info("Генерация визитов...")
        visits = generate_visits(tourists)
        db.synthetic_visits.drop()
        db.synthetic_visits.insert_many(visits)
        logging.info(f"Визиты: {len(visits)} записей загружено.")

        logging.info("Генерация поисков...")
        searches = generate_search_events(tourists)
        db.synthetic_searches.drop()
        db.synthetic_searches.insert_many(searches)
        logging.info(f"Поисковые события: {len(searches)} записей загружено.")

        logging.info("Все данные успешно сгенерированы и загружены в MongoDB.")

    except Exception as e:
        logging.exception(f"Ошибка в процессе генерации: {e}")