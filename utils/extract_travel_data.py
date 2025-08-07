import requests
from pymongo import MongoClient, errors
import time
from datetime import datetime, timedelta, timezone
import logging
import os
from dotenv import load_dotenv

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения из .env
load_dotenv()

OTM_API_KEY = os.getenv("OTM_API_KEY")
MONGO_URI = "mongodb://root:root@mongodb:27017/"
DB_NAME = "travel_raw"


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

def load_to_mongo(collection_name, data, drop=False):
    """Загрузка данных в коллекцию MongoDB"""
    try:
        db = get_mongo_conn()
        # Удаляем коллекцию, если указано drop=True
        if drop:
            db[collection_name].drop()
            logging.info(f"Коллекция '{collection_name}' очищена.")
        # Если передан список документов — загружаем все сразу
        if isinstance(data, list):
            db[collection_name].insert_many(data)
        #Если data — это один документ, используем insert_one.
        else:
            db[collection_name].insert_one(data)
        logging.info(f"Данные загружены в коллекцию '{collection_name}' ({len(data) if isinstance(data, list) else 1} документов).")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных в MongoDB: {e}", exc_info=True)

    
def fetch_countries():
    """ Извлечение данных REST Countries API """
    url = "https://restcountries.com/v3.1/all?fields=name,capital,cca2,latlng,region,subregion,population,currency"
    try:
        res = requests.get(url)
        res.raise_for_status()
        logging.info("Список стран успешно получен.")
        return res.json()
    except requests.RequestException as e:
        logging.error(f"Ошибка при получении списка стран: {e}")
        return []

def fetch_opentripmap_pois(lat, lon, country_code):
    """Получение POI из OpenTripMap по координатам"""
    radius = 50000  # 50 km радиус поиска
    url = "https://api.opentripmap.com/0.1/en/places/radius"
    params = {
        "apikey": OTM_API_KEY,
        "radius": radius,
        "lon": lon,
        "lat": lat,
        "limit": 50, # Максимум 100 объектов
        "rate": 1,    # Минимальный рейтинг
        "format": "json"
    }
    try:
        # Отправляем GET-запрос с параметрами
        res = requests.get(url, params=params)
        res.raise_for_status()
        pois = res.json()

        # Дополняем каждый POI дополнительными полями: код страны и отметка времени
        for poi in pois:
            poi.update({
                "country_code": country_code,
                "timestamp": datetime.now(timezone.utc)
            })

        logging.info(f"Получено {len(pois)} POI для страны {country_code}.")
        return pois

    except requests.RequestException as e:
        logging.error(f"Ошибка при получении POI для {country_code}: {e}")
        return []

def fetch_poi_details(xid):
    """Получение подробностей по POI по xid"""
    url = f"https://api.opentripmap.com/0.1/en/places/xid/{xid}"
    params = {"apikey": OTM_API_KEY}
    try:
        res = requests.get(url, params=params)
        res.raise_for_status()
        return res.json()
    except requests.RequestException as e:
        logging.warning(f"Не удалось получить детали POI xid={xid}: {e}")
        return {}


def run_extraction():
    logging.info("Загружаем список стран...")
    countries = fetch_countries()
    if not countries:
        # Если API вернул пустой список, логируем и прерываем процесс
        logging.error("Список стран пуст")
        return
    
    # Сохраняем список стран в MongoDB, предварительно очищая коллекцию
    load_to_mongo("rest_countries", countries, drop=True)
    logging.info(f"Загружено стран: {len(countries)}")
    
    # Обрабатываем каждую страну из полученного списка
    for country in countries:
        try:
            # Извлекаем нужные поля
            name = country.get("name", {}).get("common")
            cca2 = country.get("cca2")
            latlng = country.get("latlng", [])

            # Пропускаем страну, если нет кода или координат
            if not latlng or not cca2:
                logging.warning(f"Пропуск страны без координат или кода: {name}")
                continue

            lat, lon = latlng[0], latlng[1]
            logging.info(f"\nОбработка страны: {name} ({cca2})")

            # Извлечение POI (points of interest)
            pois = fetch_opentripmap_pois(lat, lon, cca2)
            # Детализированные POI
            detailed_pois = []
            for poi in pois:
                xid = poi.get("xid")   # Уникальный идентификатор места
                if xid:
                    # Получаем подробности по XID
                    details = fetch_poi_details(xid)
                    if details:
                        # Добавляем для POI дополнительную информацией
                        poi.update({
                            "name": details.get("name"),
                            "description": details.get("wikipedia_extracts", {}).get("text"),
                            "image": details.get("preview", {}).get("source"),
                            "kinds": details.get("kinds"),
                            "otm_url": details.get("otm")
                        })
                        # Добавляем в итоговый список
                        detailed_pois.append(poi)

            # Загружаем детализированные POI в MongoDB
            if detailed_pois:
                load_to_mongo("opentripmap_pois_detailed", detailed_pois)
                logging.info(f"Детализированных POI: {len(detailed_pois)}")
            
            # Пауза, чтобы не перегружать API (ограничение частоты запросов)
            time.sleep(1)

        except Exception as e:
            logging.error(f"Ошибка при обработке страны {name}: {e}")
            continue

    logging.info("Процесс завершён.")
