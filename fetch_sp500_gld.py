# === Directory Structure (for a 3-person group) ===
# project_root/
# └── data_fetch/
#     ├── fetch_sp500_gold.py         <- this file
#     ├── fetch_bitcoin.py
#     └── fetch_cpi_oil.py
# analysis/
#     └── process_and_visualize.py
# main.py

# === File: fetch_sp500_gold.py ===

import yfinance as yf
import sqlite3
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from get_api_key import get_api_key

# Load your AlphaVantage API key (for gold prices)
ALPHA_API_KEY = get_api_key(1)

# === Function: Fetch and store S&P 500 (SPY ETF) data ===
def fetch_and_store_sp500():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # === Step 1: Create SP500_Dates (parent) and SP500_Prices (child) tables ===
     

        c.execute("""
            CREATE TABLE IF NOT EXISTS SP500_Dates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS SP500_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_id INTEGER,
                open REAL,
                close REAL,
                volume INTEGER,
                FOREIGN KEY(date_id) REFERENCES SP500_Dates(id)
            )
        """)

        # === Step 2: Count current number of price entries ===
        c.execute("SELECT COUNT(*) FROM SP500_Prices")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 SP500 data points already stored.")
            return

        # === Step 3: Determine which 25-month chunk to fetch ===
        chunk_index = current_count // 25
        chunk_size = 25
        base_start = datetime(2016, 7, 1)
        chunk_start = base_start + relativedelta(months=chunk_index * chunk_size)
        chunk_end = chunk_start + relativedelta(months=chunk_size)

        print(f"Fetching SP500 chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

        # === Step 4: Download daily SPY data ===
        data = yf.download(
            "SPY",
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False
        )

        if data is None or data.empty:
            print("No SPY data returned from yfinance.")
            return

        # === Step 5: Get first data point per month ===
        monthly_data = {}
        for dt, row in data.iterrows():
            dt_obj = dt.to_pydatetime()
            ym_key = (dt_obj.year, dt_obj.month)
            if ym_key not in monthly_data:
                try:
                    open_price = float(row["Open"].item())
                    close_price = float(row["Close"].item())
                    volume = int(row["Volume"].item())
                    monthly_data[ym_key] = (dt_obj, open_price, close_price, volume)
                except:
                    continue

            if len(monthly_data) == 25:
                break

        # === Step 6: Insert dates and data with shared integer key ===
        inserted = 0
        for (_, _), (dt, open_price, close_price, volume) in sorted(monthly_data.items()):
            date_str = dt.strftime("%Y-%m-%d")

            # Insert into SP500_Dates and retrieve its ID
            c.execute("INSERT OR IGNORE INTO SP500_Dates (date) VALUES (?)", (date_str,))
            c.execute("SELECT id FROM SP500_Dates WHERE date = ?", (date_str,))
            date_id = c.fetchone()[0]

            # Insert price data using the date_id
            c.execute("""
                INSERT OR IGNORE INTO SP500_Prices (date_id, open, close, volume)
                VALUES (?, ?, ?, ?)
            """, (date_id, open_price, close_price, volume))

            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} SP500 entries. Total should now be {current_count + inserted}.")

# === Function: Fetch and store Gold (GLD ETF) data ===
def fetch_and_store_gold():
    # NOTE: Replace with secure API key management in production
    ALPHA_API_KEY = "YOUR_API_KEY"  # If you didn't use get_api_key(), hardcode or load safely

    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Create Gold_Prices table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS Gold_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                open REAL,
                close REAL
            )
        """)

        # Check how many rows exist already
        c.execute("SELECT COUNT(*) FROM Gold_Prices")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 Gold entries already exist. Skipping.")
            return

        # Determine chunk range (25-month blocks)
        chunk_index = current_count // 25
        chunk_size = 25
        base_start = datetime(2016, 7, 1)
        chunk_start = base_start + relativedelta(months=chunk_index * chunk_size)
        chunk_end = chunk_start + relativedelta(months=chunk_size)

        print(f"Fetching Gold chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

        # Request full GLD daily data from AlphaVantage
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": "GLD",
            "apikey": ALPHA_API_KEY,
            "outputsize": "full"
        }

        response = requests.get(url, params=params).json()
        time_series = response.get("Time Series (Daily)", {})

        if not time_series:
            print("No time series data found in the response.")
            return

        # Parse one entry per month from the chunk
        monthly_data = {}
        for date_str in sorted(time_series.keys()):
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            if dt < chunk_start or dt >= chunk_end:
                continue

            ym_key = (dt.year, dt.month)
            if ym_key not in monthly_data:
                try:
                    entry = time_series[date_str]
                    open_price = float(entry["1. open"])
                    close_price = float(entry["4. close"])
                    monthly_data[ym_key] = (dt, open_price, close_price)
                except (KeyError, ValueError):
                    continue

            if len(monthly_data) == 25:
                break

        # Insert new rows into Gold_Prices table
        inserted = 0
        for (_, _), (dt, open_price, close_price) in sorted(monthly_data.items()):
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO Gold_Prices (date, open, close) VALUES (?, ?, ?)",
                      (date_str, open_price, close_price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} new Gold entries. Total should now be {current_count + inserted}.")

# === MAIN EXECUTION ===
# Run both fetch functions when this file is executed directly
if __name__ == '__main__':
    fetch_and_store_sp500()
    fetch_and_store_gold()
