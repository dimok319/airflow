from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests as re
import sqlite3
from sqlalchemy import create_engine, text
import os

# ================= TELEGRAM =================
TELEGRAM_TOKEN = "8012518868:AAEr1ZV_6PXTh9nv4-ce4oH21SyiAkuZ1rY"
CHAT_ID = "6376001761"

def send_telegram(msg):
    try:
        re.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")


# ================= ETL =================
def transfer_sqlite_to_postgres():

    send_telegram("🚀 Старт DAG: sqlite_to_postgres_incremental")

    try:
        sqlite_path = "/mnt/e/ТРенировка.db"

        if not os.path.exists(sqlite_path):
            raise Exception(f"SQLite база не найдена: {sqlite_path}")

        sqlite_conn = sqlite3.connect(sqlite_path)

        pg_engine = create_engine(
            "postgresql+psycopg2://admin:1234@target-postgres:5432/mydb"
        )

        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table';",
            sqlite_conn
        )["name"].tolist()

        total_rows = 0
        processed_tables = 0

        for table in tables:

            print(f"Обработка таблицы: {table}")

            df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)

            if df.empty:
                continue

            # создаём таблицу если её нет
            df.head(0).to_sql(table, pg_engine, if_exists='append', index=False)

            if "id" in df.columns:
                max_id_query = text(f"SELECT MAX(id) FROM {table}")
                with pg_engine.connect() as conn:
                    result = conn.execute(max_id_query).scalar()
                max_id = result if result else 0
                df = df[df["id"] > max_id]

            if not df.empty:
                df.to_sql(table, pg_engine, if_exists='append', index=False)
                total_rows += len(df)
                processed_tables += 1

        # проверяем общее количество строк в Postgres
        total_pg_rows = 0
        for table in tables:
            try:
                result = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", pg_engine)
                total_pg_rows += result["count"].iloc[0]
            except:
                pass

        success_msg = (
            f"✅ Инкрементальная загрузка завершена\n"
            f"📂 Обработано таблиц: {processed_tables}\n"
            f"➕ Добавлено строк: {total_rows}\n"
            f"📊 Всего строк в Postgres: {total_pg_rows}"
        )

        send_telegram(success_msg)

    except Exception as e:
        error_msg = f"❌ Ошибка ETL: {str(e)}"
        print(error_msg)
        try:
            send_telegram(error_msg)
        except:
            pass
        raise


# ================= DAG =================
with DAG(
    dag_id="sqlite_to_postgres_incremental",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    PythonOperator(
        task_id="transfer_task",
        python_callable=transfer_sqlite_to_postgres
    )