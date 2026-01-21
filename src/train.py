import pandas as pd
import joblib
import os
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from database import get_db_engine

def train_model():
    print("--- Trening: Pobieranie danych historycznych ---")
    engine = get_db_engine()
    
    # Pobieramy wszystko co mamy w bazie
    df = pd.read_sql("SELECT title, subreddit FROM processed_posts WHERE title IS NOT NULL", engine)
    
    if df.empty or df['subreddit'].nunique() < 2:
        print("❌ Za mało danych do treningu!")
        return

    X = df['title']
    y = df['subreddit']

    print(f"--- Trening: Start na {len(df)} wierszach ---")
    
    model_pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(stop_words='english', max_features=2000)),
        ('clf', RandomForestClassifier(n_estimators=100, random_state=42))
    ])
    
    model_pipeline.fit(X, y)
    
    # Zapis
    if not os.path.exists('models'):
        os.makedirs('models')
        
    joblib.dump(model_pipeline, 'models/topic_classifier.pkl')
    print("✅ Model wytrenowany i zapisany w models/topic_classifier.pkl")