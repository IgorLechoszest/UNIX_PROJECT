from sqlalchemy import text
from database import get_db_engine

def setup_snowflake_objects():
    print("--- Rozpoczynam konfigurację Snowflake (DDL) ---")
    engine = get_db_engine()

    # Definicja widoku 
    create_view_query = """
    CREATE OR REPLACE VIEW processed_posts AS
    SELECT 
        ingestion_id,
        ingested_at,
        PARSE_JSON(raw_data):id::STRING AS post_id,
        PARSE_JSON(raw_data):author::STRING AS author,
        PARSE_JSON(raw_data):subreddit::STRING AS subreddit,
        
        -- Dane tekstowe
        PARSE_JSON(raw_data):title::STRING AS title,
        PARSE_JSON(raw_data):selftext::STRING AS content,
        PARSE_JSON(raw_data):link_flair_text::STRING AS category_flair,
        
        -- Metryki
        PARSE_JSON(raw_data):score::INTEGER AS score,
        PARSE_JSON(raw_data):num_comments::INTEGER AS num_comments,
        PARSE_JSON(raw_data):upvote_ratio::FLOAT AS upvote_ratio,
        PARSE_JSON(raw_data):subreddit_subscribers::INTEGER AS subreddit_subscribers,
        
        -- Czas
        TO_TIMESTAMP(PARSE_JSON(raw_data):created_utc::INTEGER) AS created_at_utc,
        
        -- Flagi
        PARSE_JSON(raw_data):is_video::BOOLEAN AS is_video,
        PARSE_JSON(raw_data):over_18::BOOLEAN AS is_nsfw,
        
        -- Debug
        LENGTH(PARSE_JSON(raw_data):title::STRING) as title_length
    FROM raw_reddit_posts;
    """

    # Definicja tabeli RAW 
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_reddit_posts (
        ingestion_id VARCHAR DEFAULT UUID_STRING(),
        ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        raw_data VARIANT
    );
    """

    with engine.connect() as connection:
        try:
            print("1. Tworzenie tabeli RAW (jeśli nie istnieje)...")
            connection.execute(text(create_table_query))
            
            print("2. Tworzenie/Aktualizacja widoku PROCESSED_POSTS...")
            connection.execute(text(create_view_query))
            
            print("✅ Sukces! Infrastruktura gotowa.")
            
        except Exception as e:
            print(f"❌ Błąd podczas tworzenia obiektów: {e}")

if __name__ == "__main__":
    setup_snowflake_objects()