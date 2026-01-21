import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from snowflake.connector.pandas_tools import pd_writer
from src.database import get_db_engine

def fetch_and_save_reddit(subreddit, limit=20, start_date=None, end_date=None):
    """
    Pobiera posty i filtruje je po dacie.
    
    :param subreddit: nazwa subreddita
    :param limit: ile postów pobrać z API (przed filtrowaniem!)
    :param start_date: (opcjonalnie) obiekt datetime lub string 'YYYY-MM-DD' - posty starsze niż to będą odrzucone
    :param end_date: (opcjonalnie) obiekt datetime lub string 'YYYY-MM-DD' - posty nowsze niż to będą odrzucone
    """
    print(f"--- Ingest: r/{subreddit} (API limit={limit}) ---")
    
    # Konwersja stringów na datetime (jeśli podano stringi)
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Logika zakresu
    if start_date and end_date:
        print(f"    Filtrowanie: szukam postów od {start_date.date()} do {end_date.date()}")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) student_project_v3'}
    
    # WAŻNE: Zmieniamy t=month na t=year, żeby sięgnąć głębiej w historię
    # Jeśli chcesz posty sprzed 2 miesięcy, t=month nic nie da (bo daje tylko ostatnie 30 dni).
    url = f"https://www.reddit.com/r/{subreddit}/top/.json?t=year&limit={limit}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"⚠️ API Error {response.status_code}")
            return

        data = response.json()
        posts = data.get('data', {}).get('children', [])
        
        valid_posts_data = []
        
        for post in posts:
            p_data = post['data']
            
            # Konwersja timestampa z Reddita (UTC) na datetime
            post_date = datetime.fromtimestamp(p_data['created_utc'])
            
            # FILTROWANIE
            if start_date and post_date < start_date:
                continue # Za stary
            if end_date and post_date > end_date:
                continue # Za nowy (zbyt aktualny)
                
            # Jeśli przeszedł filtry, dodajemy do listy
            valid_posts_data.append(json.dumps(p_data))

        if not valid_posts_data:
            print(f"⚠️ Pobranno {len(posts)} postów, ale żaden nie mieścił się w zadanym przedziale dat.")
            return

        # Zapis do Snowflake (tak jak wcześniej)
        df = pd.DataFrame({'RAW_DATA': valid_posts_data})
        
        engine = get_db_engine()
        df.to_sql(
            'raw_reddit_posts', 
            engine, 
            if_exists='append', 
            index=False,
            method=pd_writer
        )
        print(f"✅ Zapisano {len(df)} postów z r/{subreddit} (po filtrowaniu dat).")
        
    except Exception as e:
        print(f"❌ Błąd ingestu: {e}")
        raise e
