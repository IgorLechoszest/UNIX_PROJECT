from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Dynamiczne ustalanie ścieżki do folderu 'data' 
# Zakładamy, że DAG jest w /dags/, więc wychodzimy poziom wyżej do głównego folderu
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_FOLDER)
OUTPUT_FOLDER = os.path.join(PROJECT_ROOT, 'data')

def fetch_and_save_reddit(subreddit, limit=10):
    # Tworzymy folder data, jeśli jeszcze nie istnieje
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"reddit_{subreddit}_{timestamp}.csv"
    full_path = os.path.join(OUTPUT_FOLDER, file_name)

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) project_student_v1'}
    url = f"https://www.reddit.com/r/{subreddit}/top/.json?t=week&limit={limit}"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        posts = data['data']['children']
        
        extracted_data = []
        for post in posts:
            p = post['data']
            extracted_data.append({
                'title': p.get('title'),
                'score': p.get('score'),
                'author': p.get('author'),
                'num_comments': p.get('num_comments'),
                'content': p.get('selftext', ''),
                'url': f"https://reddit.com{p.get('permalink')}",
                'created_utc': datetime.fromtimestamp(p.get('created_utc'))
            })
        
        df = pd.DataFrame(extracted_data)
        df.to_csv(full_path, index=False, encoding='utf-8')
        print(f"Dane zapisane w folderze DVC: {full_path}")
    else:
        raise Exception(f"Reddit API Error: {response.status_code}")

# Konfiguracja DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'reddit_single_task_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_run = PythonOperator(
        task_id='fetch_and_save_to_csv',
        python_callable=fetch_and_save_reddit,
        op_kwargs={'subreddit': 'machinelearning', 'limit': 15},
    )
