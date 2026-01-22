from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os


from src.ingest import fetch_and_save_reddit
from src.predict import predict_daily_batch

SUBREDDITS = ['python', 'Arsenal', 'anime', 'gaming', 'Polska']

def ingest_daily_wrapper():
    start_date = datetime.now() - timedelta(days =7)
    end_date = datetime.now()
    
    print(f"--- Daily Ingest: Pobieranie postów nowszych niż {start_date} ---")

    for sub in SUBREDDITS:
        fetch_and_save_reddit(sub, limit=5, start_date=start_date, end_date = end_date)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('02_daily_inference_pipeline', 
        default_args=default_args,
        start_date=datetime(2026, 1, 1), 
        schedule_interval='0 3 * * *',
        catchup=False) as dag:

    t1_ingest_daily = PythonOperator(
        task_id='ingest_daily_data',
        python_callable=ingest_daily_wrapper
    )

    t2_predict = PythonOperator(
        task_id='predict_new_batch',
        python_callable=predict_daily_batch
    )

    t1_ingest_daily >> t2_predict
