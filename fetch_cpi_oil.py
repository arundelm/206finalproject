# Directory Structure (for a 3-person group)
# project_root/
# └── data_fetch/
#     ├── fetch_sp500_gold.py
#     ├── fetch_bitcoin.py
#     └── fetch_cpi_oil.py
# analysis/
#     └── process_and_visualize.py
# main.py

# File: fetch_cpi_oil.py
import requests
import sqlite3
from datetime import datetime
from get_api_key import get_api_key
import pandas as pd

FRED_API_KEY = get_api_key(2, 'api_key.txt')

def fetch_and_store_cpi():
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "CPIAUCSL",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "2018-01-01"
    }
    response = requests.get(url, params=params).json()
    data = [(obs['date'], float(obs['value'])) for obs in response['observations'] if obs['value'] != "."]

    conn = sqlite3.connect("financial_data.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS CPI_Data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            cpi_value REAL
        )
    """)
    for date, value in data[:25]:  # limit to 25 per run
        c.execute("INSERT OR IGNORE INTO CPI_Data (date, cpi_value) VALUES (?, ?)", (date, value))
    conn.commit()
    conn.close()

def fetch_and_store_oil():
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "DCOILWTICO",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "2018-01-01"
    }
    response = requests.get(url, params=params).json()
    data = [(obs['date'], float(obs['value'])) for obs in response['observations'] if obs['value'] != "."]

    df = pd.DataFrame(data, columns=["date", "price"])
    df["date"] = pd.to_datetime(df["date"])
    df["YearMonth"] = df["date"].dt.to_period("M")
    monthly_oil = df.groupby("YearMonth").first().reset_index()

    conn = sqlite3.connect("financial_data.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS Oil_Prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            price REAL
        )
    """)
    for _, row in monthly_oil.head(25).iterrows():
        date = row['YearMonth'].start_time.strftime('%Y-%m-%d')
        price = row['price']
        c.execute("INSERT OR IGNORE INTO Oil_Prices (date, price) VALUES (?, ?)", (date, price))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    fetch_and_store_cpi()
    fetch_and_store_oil()
