# === IMPORT DATA FETCHING FUNCTIONS ===
# These functions retrieve and store data into the SQLite database.
from fetch_bitcoin import fetch_and_store_bitcoin             # Fetches and stores Bitcoin price data
from fetch_cpi_oil import fetch_and_store_cpi, fetch_and_store_oil  # Fetches and stores CPI and Oil data
from fetch_sp500_gld import fetch_and_store_gold, fetch_and_store_sp500  # Fetches and stores S&P 500 and Gold data
import sqlite3
# === MAIN EXECUTION BLOCK ===
# When the script is run directly, fetch data from all sources.
if __name__ == '__main__':
    with sqlite3.connect("financial_data.db") as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS Combined_Prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    btc_price REAL,
                    sp500_price REAL,
                    gold_open REAL,
                    gold_close REAL,
                    gold_change INTEGER,  -- FK to Gold_Change.id
                    oil_price REAL,
                    cpi_value REAL
                );
            """)
            conn.commit()
    fetch_and_store_bitcoin()     # Insert next 25 rows of Bitcoin data (up to 100 max)
    fetch_and_store_sp500()       # Insert next 25 rows of S&P 500 data
    fetch_and_store_gold()        # Insert next 25 rows of Gold data
    fetch_and_store_cpi()         # Insert next 25 rows of CPI data
    fetch_and_store_oil()         # Insert next 25 rows of Oil price data
