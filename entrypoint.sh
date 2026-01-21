#!/bin/bash
# Ustawienie zmiennej środowiskowej AIRFLOW_HOME (na wszelki wypadek, jeśli nie zdefiniowano w Dockerfile)
export AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}

# Inicjalizacja bazy danych Airflow (tworzy plik SQLite lub schemat w DB, jeśli używamy zewnętrznej bazy)
airflow db init

# (Opcjonalnie) Utworzenie użytkownika admin, jeżeli to pierwszy start i nie mamy jeszcze użytkowników.
# Można pominąć, gdy korzystamy z domyślnego użytkownika admin generowanego przez `airflow standalone`.
# airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Automatyczne uruchomienie DAG-a inicjalizacyjnego przy pierwszym starcie
if [ ! -f "$AIRFLOW_HOME/init_completed" ]; then
    echo "Triggering initial DAG run: init_pipeline_dag"
    airflow dags trigger init_pipeline_dag
    touch $AIRFLOW_HOME/init_completed
fi

# Uruchomienie schedulera w tle
airflow scheduler &

# Uruchomienie webservera (proces główny kontenera)
exec airflow webserver
