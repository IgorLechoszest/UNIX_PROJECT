import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

load_dotenv()

def get_db_engine():
    """
    Tworzy engine SQLAlchemy do Snowflake.
    """
    url = URL(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    
    engine = create_engine(url)
    return engine