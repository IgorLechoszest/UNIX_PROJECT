FROM apache/airflow:2.8.1-python3.10

# Przełączamy się na roota, żeby przygotować foldery
USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

# Wracamy do użytkownika airflow
USER airflow

# Kopiujemy wymagania i instalujemy biblioteki
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
