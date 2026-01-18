import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text  # <--- ZMIANA 1: Dodano import 'text'
from snowflake.sqlalchemy import URL

# 1. Wczytaj zmienne
load_dotenv()

user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

print(f"--- Próba połączenia ---")
print(f"Użytkownik: {user}")
print(f"Konto:      {account}")
print(f"Magazyn:    {warehouse}")
print(f"------------------------")

try:
    url = URL(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
    )

    # Tworzenie silnika
    engine = create_engine(url)
    
    # Nawiązywanie połączenia
    with engine.connect() as connection:
        print("Wysyłam zapytanie do Snowflake...")
        
        # <--- ZMIANA 2: Użycie funkcji text() zamiast zwykłego stringa
        query = text("SELECT CURRENT_VERSION(), CURRENT_DATE()")
        
        result = connection.execute(query).fetchone()
        
        print("\n✅ SUKCES! Połączono pomyślnie.")
        print(f"Wersja Snowflake: {result[0]}")
        print(f"Data serwera:     {result[1]}")

except Exception as e:
    print("\n❌ BŁĄD POŁĄCZENIA!")
    print("Sprawdź treść błędu poniżej:")
    print(e)