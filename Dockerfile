FROM apache/airflow:2.7.1-python3.10

# Przełączamy się na roota, żeby przygotować foldery
USER root

# Tworzymy folder data i nadajemy mu uprawnienia dla użytkownika airflow
# Dzięki temu skrypt Python będzie mógł tam zapisywać pliki
RUN mkdir -p /opt/airflow/data && chown -R airflow: /opt/airflow/data

# Wracamy do użytkownika airflow
USER airflow

# Kopiujemy wymagania i instalujemy biblioteki
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
