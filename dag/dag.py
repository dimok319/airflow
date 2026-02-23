from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import time as ti
import requests as re
from sqlalchemy import create_engine
import os

# === Telegram ===
TELEGRAM_TOKEN = "8012518868:AAEr1ZV_6PXTh9nv4-ce4oH21SyiAkuZ1rY"
CHAT_ID = "6376001761"  # замените на ваш chat_id


def send_telegram(msg):
    try:
        re.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")


def run_pagination():
    try:
        url = "https://jsonplaceholder.typicode.com/posts"

        items = []
        page = 1
        limit = 50

        while True:
            resp = re.get(url, params={"_page": page, "_limit": limit})
            resp.raise_for_status()
            data = resp.json()

            if not data:
                break

            print(f"Проход {page}")
            items.extend(data)

            page += 1
            ti.sleep(2)

        df = pd.DataFrame(items)

        # Переименовываем колонку userId в user_id для БД
        df = df.rename(columns={'userId': 'user_id'})

        print(f"Получено записей: {len(df)}")
        print(df.head())

        # === ПОДКЛЮЧЕНИЕ К SQLITE НА ДИСКЕ E: ===
        # Путь к вашей БД внутри контейнера (диск E смонтирован в /mnt/e)
        db_path = "/mnt/e/Тренировка.db"

        # Проверяем, доступен ли файл
        if os.path.exists(db_path):
            print(f"✅ База данных найдена: {db_path}")
        else:
            print(f"⚠️ База данных не найдена, будет создана новая: {db_path}")

        # Создаем подключение к SQLite
        engine = create_engine(f'sqlite:///{db_path}')

        # Загружаем данные в таблицу posts (создаст если нет)
        df.to_sql('posts', engine, if_exists='append', index=False)

        print(f"✅ Данные загружены в таблицу posts")

        # Проверяем, сколько записей стало
        result = pd.read_sql("SELECT COUNT(*) as count FROM posts", engine)
        total = result['count'].iloc[0]

        send_telegram(f"✅ Загружено {len(df)} записей\nВсего в таблице posts: {total}")

    except Exception as e:
        error_msg = f"❌ Ошибка: {str(e)}"
        print(error_msg)
        try:
            send_telegram(error_msg)
        except:
            pass
        raise


with DAG(
        dag_id="pagination_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    PythonOperator(
        task_id="pagination_task",
        python_callable=run_pagination
    )