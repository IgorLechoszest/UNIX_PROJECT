from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Dodanie ścieżki src
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.setup_warehouse import setup_snowflake_objects
from src.ingest import fetch_and_save_reddit
from src.train import train_model

SUBREDDITS = ['python', 'Arsenal', 'anime', 'gaming', 'Polska']

def ingest_history_wrapper():
    s_date = "2024-12-01"
    e_date = "2025-12-31"
    for sub in SUBREDDITS:
        # Limit 20 dla historii
        fetch_and_save_reddit(sub, limit=20, start_date=s_date, end_date=e_date)

with DAG('01_init_project_pipeline', 
        start_date=datetime(2026, 1, 1), 
        schedule_interval='@once', 
        catchup=False) as dag:

    t1_setup = PythonOperator(
        task_id='setup_snowflake_db',
        python_callable=setup_snowflake_objects
    )

    t2_ingest_history = PythonOperator(
        task_id='ingest_historical_data',
        python_callable=ingest_history_wrapper
    )

    t3_train_base = PythonOperator(
        task_id='train_base_model',
        python_callable=train_model
    )

    t1_setup >> t2_ingest_history >> t3_train_base