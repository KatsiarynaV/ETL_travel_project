import requests
from airflow.models import Variable


TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
TELEGRAM_TOKEN = Variable.get("telegram_token")


def send_telegram_message(message: str):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка отправки сообщения в Telegram: {e}")


def notify(context):
    """Универсальное уведомление о статусе задачи (успех или ошибка)"""
    task = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    state = task.state

    if state == 'success':
        message = (
            f"Задача <b>{task_id}</b> DAG <b>{dag_id}</b> успешно завершена.\n"
            f"Дата выполнения: {execution_date}"
        )
    else:
        message = (
            f"Ошибка в задаче <b>{task_id}</b> DAG <b>{dag_id}</b>\n"
            f"Дата выполнения: {execution_date}\n"
            f"Исключение:\n<pre>{exception}</pre>"
        )
    send_telegram_message(message)