# Używamy lekkiego obrazu bazowego z Pythonem (np. Python 3.10 slim)
FROM python:3.10-slim

# Ustawiamy katalog domowy Airflow w kontenerze
ENV AIRFLOW_HOME=/opt/airflow

# (Opcjonalnie) Instalujemy zależności systemowe potrzebne Airflow i Snowflake
RUN apt-get update && apt-get install -y build-essential libssl-dev libffi-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalacja Apache Airflow i dodatkowych pakietów Python (Snowflake itp.)
# Wykorzystujemy plik requirements.txt dla powtarzalności budowy
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Skopiowanie plików projektu do obrazu
COPY dags/ $AIRFLOW_HOME/dags/
COPY src/ $AIRFLOW_HOME/src/

# Skopiowanie skryptu entrypoint i nadanie mu praw wykonania
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Otwieramy port 8080 dla interfejsu webowego Airflow
EXPOSE 8080

# Ustawiamy entrypoint kontenera
ENTRYPOINT ["/entrypoint.sh"]
