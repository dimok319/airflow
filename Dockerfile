# Базовый образ Airflow
FROM apache/airflow:2.7.1

# Переключаемся на root для установки пакетов
USER root

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл с зависимостями Python
COPY requirements.txt /requirements.txt

# Устанавливаем Python-пакеты
RUN pip install --no-cache-dir -r /requirements.txt

# Создаем папку для скриптов
RUN mkdir -p /opt/airflow/scripts

# Копируем дополнительные скрипты (если будут)
# COPY scripts/ /opt/airflow/scripts/

# Даем права на выполнение
RUN chmod +x /opt/airflow/scripts/*.sh || true

# Возвращаем пользователя airflow
USER airflow

# Команда по умолчанию
CMD ["airflow", "version"]