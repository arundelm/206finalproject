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
    spy = yf.download('SPY', start='2018-01-01', interval='1mo', auto_adjust=False)
    conn = sqlite3.connect("financial_data.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS SP500_Prices")
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
    spy.index = pd.to_datetime(spy.index)
    for date, row in spy.iterrows():
        date_str = date.strftime('%Y-%m-%d')
        if count >= 25:
            break
        if pd.isna(row['Close']).item():
            continue
        c.execute("INSERT OR IGNORE INTO SP500_Prices (date, open, close, volume) VALUES (?, ?, ?, ?)",
                  (date_str, float(row['Open']), float(row['Close']), int(row['Volume'])))
        count += 1
    conn.commit()
    conn.close()

def fetch_and_store_gold():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "GLD",
        "apikey": "YOUR_ALPHA_VANTAGE_KEY",
        "outputsize": "full"
    }
    response = requests.get(url, params=params).json()
    time_series = response.get("Time Series (Daily)", {})

    # Convert to DataFrame
    gold_df = pd.DataFrame.from_dict(time_series, orient='index')
    gold_df.index = pd.to_datetime(gold_df.index)
    gold_df.sort_index(inplace=True)
    gold_df['YearMonth'] = gold_df.index.to_period('M')
    monthly_gold = gold_df.groupby('YearMonth').first().reset_index()
    monthly_gold = monthly_gold[monthly_gold['YearMonth'] >= pd.Period('2018-01')]

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
    for _, row in monthly_gold.iterrows():
        if count >= 25:
            break
        date = row['YearMonth'].start_time.strftime('%Y-%m-%d')
        try:
            open_price = float(row['1. open'])
            close_price = float(row['4. close'])
        except (KeyError, ValueError, TypeError):
            continue
        c.execute("INSERT OR IGNORE INTO Gold_Prices (date, open, close) VALUES (?, ?, ?)",
                  (date, open_price, close_price))
        count += 1
    conn.commit()
    conn.close()
    if count > 0:
        print(f"Gold data stored starting from {date} (up to 25 entries per run).")

if __name__ == '__main__':
    fetch_and_store_sp500()
    fetch_and_store_gold()