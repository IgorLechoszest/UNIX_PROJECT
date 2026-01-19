import pandas as pd
import joblib
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from database import get_db_engine

def load_data():
    print("--- 1. Pobieranie danych ---")
    engine = get_db_engine()
    query = "SELECT title, subreddit FROM processed_posts WHERE title IS NOT NULL AND subreddit IS NOT NULL"
    df = pd.read_sql(query, engine)
    return df

def train_topic_classifier():
    df = load_data()
    
    if df['subreddit'].nunique() < 2:
        print("❌ Za mało klas! Uruchom ingest dla różnych subredditów.")
        return

    X = df['title']
    y = df['subreddit']

    # Podział danych
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("--- 2. Trenowanie modelu ---")
    model_pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(stop_words='english', max_features=1000)),
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
    ])
    
    model_pipeline.fit(X_train, y_train)

    # Predykcja na zbiorze testowym
    y_pred = model_pipeline.predict(X_test)

    # --- NOWA CZĘŚĆ: ZAPIS DO TABELI ---
    print("--- 3. Zapisywanie predykcji do Snowflake ---")
    
    # Tworzymy DataFrame z wynikami
    # X_test to tytuły, y_test to prawdziwe etykiety, y_pred to przewidywania
    results_df = pd.DataFrame({
        'original_text': X_test,
        'real_label': y_test,
        'predicted_label': y_pred
    })
    
    # Dodajemy znacznik czasu, żebyś wiedział, z którego treningu pochodzą dane
    results_df['training_run_at'] = datetime.now()

    engine = get_db_engine()
    
    try:
        # Zapisujemy do nowej tabeli 'model_predictions'
        # if_exists='append' -> dopisuje nowe wyniki do historii
        # if_exists='replace' -> kasuje stare wyniki i tworzy tabelę od nowa
        results_df.to_sql('model_predictions', engine, if_exists='append', index=False)
        print("✅ Zapisano wyniki do tabeli 'model_predictions'!")
    except Exception as e:
        print(f"❌ Błąd zapisu tabeli wyników: {e}")

    # Ewaluacja w konsoli (żebyś też widział od razu)
    acc = accuracy_score(y_test, y_pred)
    print(f"✅ Accuracy: {acc:.2%}")

    joblib.dump(model_pipeline, 'models/topic_classifier.pkl')

if __name__ == "__main__":
    train_topic_classifier()