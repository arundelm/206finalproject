import requests
import sqlite3
import pandas as pd
import time
import yfinance as yf
from datetime import datetime


def fetch_and_store_bitcoin():
    start_date = pd.to_datetime("2018-01-01")
    end_date = pd.to_datetime(datetime.today().strftime("%Y-%m-%d"))
    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(end_date.timestamp() * 1000)

    url = f"https://api.coincap.io/v2/assets/bitcoin/history?interval=d1&start={start_ms}&end={end_ms}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch Bitcoin data: {e}")
        return

    if not data:
        print("No Bitcoin price data returned.")
        return

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= pd.to_datetime("2018-01-01")]
    df["YearMonth"] = df["date"].dt.to_period("M")
    monthly_btc = df.groupby("YearMonth").first().reset_index()

    conn = sqlite3.connect("financial_data.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS Bitcoin_Prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            price REAL
        )""")
    for _, row in monthly_btc.head(25).iterrows():
        date = row['YearMonth'].start_time.strftime('%Y-%m-%d')
        price = float(row['priceUsd'])
        c.execute("INSERT OR IGNORE INTO Bitcoin_Prices (date, price) VALUES (?, ?)", (date, price))
    conn.commit()
    conn.close()
    print("Bitcoin data stored successfully.")

if __name__ == '__main__':
  
    fetch_and_store_bitcoin()
