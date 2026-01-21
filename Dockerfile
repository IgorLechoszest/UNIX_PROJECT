FROM apache/airflow:2.8.4-python3.10

USER root

# 1. Instalacja zależności systemowych (jako root)
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Przygotowanie folderów
RUN mkdir -p /opt/airflow/data /opt/airflow/src /opt/airflow/logs /opt/airflow/dags

# 3. Instalacja bibliotek Python
# Przełączamy się na airflow TYLKO na czas pip install, żeby obraz nie protestował
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /requirements.txt

# 4. Powrót do root dla reszty operacji i dla działania kontenerów
USER root
COPY --chown=root:root src/ /opt/airflow/src/
