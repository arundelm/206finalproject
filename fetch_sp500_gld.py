# Directory Structure (for a 3-person group)
# project_root/
# └── data_fetch/
#     ├── fetch_sp500_gold.py
#     ├── fetch_bitcoin.py
#     └── fetch_cpi_oil.py
# analysis/
#     └── process_and_visualize.py
# main.py

# File: fetch_sp500_gold.py
import yfinance as yf
import requests
import sqlite3
from datetime import datetime
from get_api_key import get_api_key
import pandas as pd

ALPHA_API_KEY = get_api_key(1, 'api_key.txt')

def fetch_and_store_sp500():
    spy = yf.download('SPY', start='2018-01-01', interval='1d')
    spy.reset_index(inplace=True)
    conn = sqlite3.connect("financial_data.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS SP500_Prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            open REAL,
            close REAL,
            volume INTEGER
        )
    """)
    count = 0
    for _, row in spy.iterrows():
        if count >= 25:
            break
        date = str(row['Date'].date())
        c.execute("INSERT OR IGNORE INTO SP500_Prices (date, open, close, volume) VALUES (?, ?, ?, ?)",
                  (date, row['Open'], row['Close'], int(row['Volume'])))
        count += 1
    conn.commit()
    conn.close()

def fetch_and_store_gold():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "GLD",
        "apikey": ALPHA_API_KEY,
        "outputsize": "full"
    }
    response = requests.get(url, params=params).json()
    time_series = response.get("Time Series (Daily)", {})

    conn = sqlite3.connect("financial_data.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS Gold_Prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            open REAL,
            close REAL
        )
    """)
    count = 0
    # Store data starting from 2018-01-01
    for date, data in sorted(time_series.items()):
        if date < "2018-01-01":
            continue
        if count >= 25:
            break
        c.execute("INSERT OR IGNORE INTO Gold_Prices (date, open, close) VALUES (?, ?, ?)",
                  (date, float(data['1. open']), float(data['4. close'])))
        count += 1
    conn.commit()
    conn.close()
    if count > 0:
        print(f"Gold data stored starting from {date} (up to 25 entries per run).")

if __name__ == '__main__':
    fetch_and_store_sp500()
    fetch_and_store_gold()