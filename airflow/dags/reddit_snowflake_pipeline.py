from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------------------------------------------------------
# Project paths (host layout):
# project/
#   airflow/dags/   <-- this file lives here
#   data/
#   models/
#   src/
#   requirements.txt
# ------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]   # dags -> airflow -> project
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"

DATA_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Ensure "src/" is importable (so we can import project code from Airflow DAG)
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# ------------------------------------------------------------
# Imports from your project (inside src/)
# Adjust these imports to match your actual module names.
# Example: if you have project/src/database.py with get_db_engine().
# ------------------------------------------------------------
from database import get_db_engine  # noqa: E402

# Snowflake helper, same approach as in your ingest script
from snowflake.connector.pandas_tools import pd_writer  # noqa: E402

# ML stack (similar to your train.py idea)
import joblib  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402
from sklearn.pipeline import Pipeline  # noqa: E402
from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: E402
from sklearn.ensemble import RandomForestClassifier  # noqa: E402
from sklearn.metrics import accuracy_score  # noqa: E402


# ------------------------------------------------------------
# Task 1: Fetch Reddit API -> store raw JSON
# ------------------------------------------------------------
def fetch_reddit_to_json(subreddit: str, limit: int = 20, **context) -> str:
    """
    Fetch posts from Reddit API and store the raw response as a JSON file.
    Returns the JSON file path via XCom.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = DATA_DIR / f"reddit_{subreddit}_{timestamp}.json"

    headers = {"User-Agent": "airflow-reddit-pipeline/1.0"}
    url = f"https://www.reddit.com/r/{subreddit}/top/.json?t=week&limit={limit}"

    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Reddit API error {resp.status_code}: {resp.text[:200]}")

    out_path.write_text(json.dumps(resp.json(), ensure_ascii=False), encoding="utf-8")
    print(f"Saved JSON to: {out_path}")
    return str(out_path)


# ------------------------------------------------------------
# Task 2: JSON -> CSV
# ------------------------------------------------------------
def json_to_csv(ti, subreddit: str, **context) -> str:
    """
    Convert Reddit JSON into a structured CSV file.
    Returns the CSV file path via XCom.
    """
    json_path = ti.xcom_pull(task_ids="fetch_json")
    if not json_path or not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
    posts = payload["data"]["children"]

    rows: List[Dict] = []
    for item in posts:
        data = item.get("data", {})
        rows.append(
            {
                "post_id": data.get("id"),
                "subreddit": data.get("subreddit") or subreddit,
                "author": data.get("author"),
                "title": data.get("title"),
                "content": data.get("selftext", ""),
                "score": data.get("score"),
                "num_comments": data.get("num_comments"),
                "upvote_ratio": data.get("upvote_ratio"),
                "created_utc": data.get("created_utc"),
                "permalink": data.get("permalink"),
                # keep full post object as JSON string for RAW load
                "raw_data": json.dumps(data, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = DATA_DIR / f"reddit_{subreddit}_{timestamp}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"Saved CSV to: {csv_path} rows={len(df)}")
    return str(csv_path)


# ------------------------------------------------------------
# Task 3A: CSV -> Snowflake (RAW table load)
# ------------------------------------------------------------
def load_csv_to_snowflake_raw(ti, **context) -> None:
    """
    Load raw Reddit posts into Snowflake RAW table:
    - reads CSV produced by previous task
    - loads only raw JSON strings into a column RAW_DATA
    - appends into raw_reddit_posts using pd_writer
    """
    csv_path = ti.xcom_pull(task_ids="json_to_csv")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df_csv = pd.read_csv(csv_path)
    if "raw_data" not in df_csv.columns:
        raise ValueError("CSV must contain 'raw_data' column")

    df_raw = pd.DataFrame({"RAW_DATA": df_csv["raw_data"].astype(str)})

    engine = get_db_engine()
    df_raw.to_sql(
        "raw_reddit_posts",
        engine,
        if_exists="append",
        index=False,
        method=pd_writer,
    )

    print(f"Inserted {len(df_raw)} rows into raw_reddit_posts")


# ------------------------------------------------------------
# Task 3B: ML branch on CSV + save predictions
# ------------------------------------------------------------
def run_ml_and_save_predictions(ti, **context) -> None:
    """
    Train a simple text classifier on CSV data and save predictions to Snowflake.
    NOTE: With only one subreddit per run, training is not meaningful.
          For real ML, fetch multiple subreddits or use historical data from Snowflake.
    """
    csv_path = ti.xcom_pull(task_ids="json_to_csv")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path).dropna(subset=["title", "subreddit"])
    if df.empty:
        print("No rows to train on. Skipping ML step.")
        return

    # Minimal demo target: predict subreddit from title
    # (Works only if there are >=2 classes; otherwise skip.)
    if df["subreddit"].nunique() < 2:
        print("Not enough classes in this batch. Skipping ML step.")
        return

    X = df["title"].astype(str)
    y = df["subreddit"].astype(str)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = Pipeline(
        steps=[
            ("tfidf", TfidfVectorizer(stop_words="english", max_features=1000)),
            ("clf", RandomForestClassifier(n_estimators=150, random_state=42)),
        ]
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    print(f"Model accuracy: {acc:.2%}")

    # Save model artifact under project/models
    model_path = MODELS_DIR / "reddit_topic_classifier.pkl"
    joblib.dump(model, model_path)
    print(f"Saved model to: {model_path}")

    # Save predictions to Snowflake
    results_df = pd.DataFrame(
        {
            "original_text": X_test.values,
            "real_label": y_test.values,
            "predicted_label": y_pred,
            "run_timestamp": datetime.utcnow(),
        }
    )

    engine = get_db_engine()
    results_df.to_sql("model_predictions", engine, if_exists="append", index=False)
    print("Predictions saved to model_predictions")


# ------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="reddit_json_csv_snowflake_ml_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["reddit", "snowflake", "ml"],
) as dag:

    fetch_json = PythonOperator(
        task_id="fetch_json",
        python_callable=fetch_reddit_to_json,
        op_kwargs={"subreddit": "machinelearning", "limit": 25},
    )

    convert_to_csv = PythonOperator(
        task_id="json_to_csv",
        python_callable=json_to_csv,
        op_kwargs={"subreddit": "machinelearning"},
    )

    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake_raw",
        python_callable=load_csv_to_snowflake_raw,
    )

    ml_task = PythonOperator(
        task_id="ml_pipeline",
        python_callable=run_ml_and_save_predictions,
    )

    # Pipeline: fetch -> convert -> parallel branches (Snowflake load, ML)
    fetch_json >> convert_to_csv >> [load_to_snowflake, ml_task]
