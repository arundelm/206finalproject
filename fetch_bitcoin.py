import requests
import sqlite3
import pandas as pd
import time
from datetime import datetime

def fetch_and_store_bitcoin():
    url = "https://api.coindesk.com/v1/bpi/historical/close.json"
    params = {
        "start": "2018-01-01",
        "end": datetime.today().strftime("%Y-%m-%d")
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch Bitcoin data: {e}")
        return

    data = response.json().get("bpi", {})
    if not data:
        print("No Bitcoin price data returned.")
        return

    df = pd.DataFrame(data.items(), columns=["date", "price"])
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
        )
    """)
    for _, row in monthly_btc.head(25).iterrows():
        date = row['YearMonth'].start_time.strftime('%Y-%m-%d')
        price = float(row['price'])
        c.execute("INSERT OR IGNORE INTO Bitcoin_Prices (date, price) VALUES (?, ?)", (date, price))
    conn.commit()
    conn.close()
    print("Bitcoin data stored successfully.")

if __name__ == '__main__':
  
    fetch_and_store_bitcoin()
