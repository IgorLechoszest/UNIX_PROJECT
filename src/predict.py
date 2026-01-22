import pandas as pd
import joblib
import os
from datetime import datetime
from src.database import get_db_engine


def predict_daily_batch():
    model_path = 'models/topic_classifier.pkl'
    if not os.path.exists(model_path):
        raise FileNotFoundError("Brak modelu! Uruchom najpierw DAG inicjalizujący.")

    print("--- Inference: Ładowanie modelu ---")
    model = joblib.load(model_path)
    
    engine = get_db_engine()
    
    # Pobieramy tylko posty z ostatnich 26 godzin (z lekkim zapasem dla daily run)
    print("--- Inference: Pobieranie NOWYCH danych ---")
    query = """
    SELECT post_id, title, subreddit as real_subreddit
    FROM processed_posts 
    WHERE ingested_at > DATEADD(hour, -26, CURRENT_TIMESTAMP())
    """
    df_new = pd.read_sql(query, engine)
    
    if df_new.empty:
        print("ℹ️ Brak nowych postów do przetworzenia.")
        return

    print(f"--- Inference: Predykcja dla {len(df_new)} postów ---")
    preds = model.predict(df_new['title'])
    
    results_df = pd.DataFrame({
        'post_id': df_new['post_id'],
        'post_title': df_new['title'],
        'real_subreddit': df_new['real_subreddit'],
        'predicted_subreddit': preds,
        'model_version': 'v1_rf'
    })
    
    correct = (results_df['real_subreddit'] == results_df['predicted_subreddit']).sum()
    total = len(results_df)
    accuracy = correct / total

    print(f"📊 DOKŁADNOŚĆ (ACCURACY): {accuracy:.2%}", flush=True)
    print("\n🔍 GDZIE MODEL SIĘ MYLI (Prawdziwe vs Przewidziane):", flush=True)
    confusion_matrix = pd.crosstab(
    results_df['real_subreddit'], 
    results_df['predicted_subreddit'], 
    margins=True)
    print(confusion_matrix, flush=True)
    results_df.to_sql('daily_predictions', engine, if_exists='append', index=False)
    print("✅ Wyniki zapisane w tabeli daily_predictions.")
