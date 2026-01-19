import json
import pandas as pd
import requests
from database import get_db_engine
from snowflake.connector.pandas_tools import pd_writer

def fetch_and_save_reddit(subreddit, limit=5):
    print(f"--- Pobieranie r/{subreddit} limit={limit} ---")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) student_project'}
    url = f"https://www.reddit.com/r/{subreddit}/top/.json?t=week&limit={limit}"
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code}")

    data = response.json()
    posts = data['data']['children']
    
    raw_json_list = [json.dumps(post['data']) for post in posts]
    df = pd.DataFrame({
        'RAW_DATA': raw_json_list 
    })
    
    try:
        engine = get_db_engine()
        
        df.to_sql(
            'raw_reddit_posts',  
            engine, 
            if_exists='append', 
            index=False,
            method=pd_writer  
        )
        print(f"✅ Zapisano {len(df)} wierszy (VARIANT).")
    except Exception as e:
        print(f"❌ Błąd bazy: {e}")
        raise e

if __name__ == "__main__":
    fetch_and_save_reddit('polska', limit=21)