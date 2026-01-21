from sqlalchemy import text
from database import get_db_engine

def setup_snowflake_objects():
    print("--- DDL: Tworzenie tabel i widoków ---")
    engine = get_db_engine()

    # 1. Tabela na surowe dane (JSON)
    raw_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_reddit_posts (
        ingestion_id VARCHAR DEFAULT UUID_STRING(),
        ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        raw_data TEXT
    );
    """

    # 2. Widok parsujący (Srebrna warstwa)
    view_sql = """
    CREATE OR REPLACE VIEW processed_posts AS
    SELECT 
        ingestion_id,
        ingested_at,
        PARSE_JSON(raw_data):id::STRING AS post_id,
        PARSE_JSON(raw_data):subreddit::STRING AS subreddit,
        PARSE_JSON(raw_data):title::STRING AS title,
        PARSE_JSON(raw_data):selftext::STRING AS content,
        PARSE_JSON(raw_data):score::INTEGER AS score,
        TO_TIMESTAMP(PARSE_JSON(raw_data):created_utc::INTEGER) AS created_at_utc
    FROM raw_reddit_posts;
    """

    # 3. Tabela na wyniki predykcji
    preds_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_predictions (
        prediction_id VARCHAR DEFAULT UUID_STRING(),
        prediction_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        post_id VARCHAR,
        post_title VARCHAR,
        real_subreddit VARCHAR,
        predicted_subreddit VARCHAR,
        model_version VARCHAR
    );
    """

    with engine.connect() as conn:
        conn.execute(text(raw_table_sql))
        conn.execute(text(view_sql))
        conn.execute(text(preds_table_sql))
        print("✅ Infrastruktura Snowflake gotowa.")