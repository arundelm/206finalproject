# store_of_value_showdown.py
# Track Gold, Bitcoin, the S&P500, and Crude Oil compared to the CPI (Inflation Index)

import sqlite3
import requests
import yfinance as yf
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
import time

DB_NAME = "store_of_value.db"

FRED_API_KEY = "YOUR_FRED_API_KEY_HERE"  # Replace with your actual key
EIA_API_KEY = "YOUR_EIA_API_KEY_HERE"    # Replace with your actual key

# ----------------------------
# Database Initialization
# ----------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Create tables
    c.execute("""
    CREATE TABLE IF NOT EXISTS crypto_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        asset TEXT,
        price_usd REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS traditional_assets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        asset TEXT,
        price_usd REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS inflation_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        cpi REAL
    )""")

    conn.commit()
    conn.close()

# ----------------------------
# Fetch and Store CoinGecko Crypto Prices
# ----------------------------
def fetch_and_store_crypto(asset_id, symbol):
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart?vs_currency=usd&days=365&interval=daily"
    response = requests.get(url)
    data = response.json()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    for entry in data.get("prices", []):
        date = datetime.fromtimestamp(entry[0] / 1000).date().isoformat()
        price = entry[1]
        c.execute("INSERT INTO crypto_prices (date, asset, price_usd) VALUES (?, ?, ?)",
                  (date, symbol, price))

    conn.commit()
    conn.close()

# ----------------------------
# Fetch and Store yfinance Asset Prices (SPY, GLD)
# ----------------------------
def fetch_and_store_yfinance(ticker, label, retries=3):
    for attempt in range(retries):
        try:
            data = yf.download(ticker, period="1y", interval="1d", auto_adjust=False)
            if data.empty:
                raise ValueError("Downloaded data is empty.")
            break
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(10)
    else:
        print(f"Failed to download data for {ticker} after {retries} retries.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    for index, row in data.iterrows():
        date = index.date().isoformat()
        price = row['Close']
        if price.empty():
            continue
        price = float(price)
        c.execute("INSERT INTO traditional_assets (date, asset, price_usd) VALUES (?, ?, ?)",
                  (date, label, price))

    conn.commit()
    conn.close()

# ----------------------------
# Fetch and Store CPI Data from FRED
# ----------------------------
def fetch_and_store_cpi():
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCNS&api_key={FRED_API_KEY}&file_type=json"
    response = requests.get(url)
    data = response.json()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    for obs in data.get("observations", []):
        date = obs["date"]
        value = obs["value"]
        if value != ".":  # skip missing data
            c.execute("INSERT INTO inflation_data (date, cpi) VALUES (?, ?)", (date, float(value)))

    conn.commit()
    conn.close()

# ----------------------------
# Fetch and Store Oil Prices from EIA
# ----------------------------
def fetch_and_store_oil_prices():
    url = f"https://api.eia.gov/series/?api_key={EIA_API_KEY}&series_id=PET.RWTC.D"
    response = requests.get(url)
    data = response.json()
    if "series" not in data or not data["series"]:
        print("No data returned from EIA. Check your API key and series ID.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    for entry in data["series"][0].get("data", []):
        date = entry[0]  # format: YYYY-MM-DD
        price = float(entry[1])
        c.execute("INSERT INTO traditional_assets (date, asset, price_usd) VALUES (?, ?, ?)",
                  (date, "OIL", price))

    conn.commit()
    conn.close()

# ----------------------------
# Generate Charts
# ----------------------------
def generate_charts():
    conn = sqlite3.connect(DB_NAME)

    # Load asset prices
    query = """
    SELECT date, asset, price_usd FROM traditional_assets
    UNION ALL
    SELECT date, asset, price_usd FROM crypto_prices
    """
    df = pd.read_sql_query(query, conn)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Drop duplicates before pivoting
    df = df.drop_duplicates(subset=["date", "asset"])

    # Pivot table for plotting
    pivot = df.pivot(index="date", columns="asset", values="price_usd")
    pivot.plot(title="Asset Prices Over Time (Unadjusted)", figsize=(12,6))
    plt.ylabel("Price in USD")
    plt.xlabel("Date")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("asset_prices.png")
    plt.show()

    # Inflation-adjusted chart (CPI)
    cpi_df = pd.read_sql_query("SELECT * FROM inflation_data", conn)
    cpi_df["date"] = pd.to_datetime(cpi_df["date"])
    cpi_df = cpi_df.sort_values("date").set_index("date")

    merged = pivot.join(cpi_df.set_index("date"), how="inner")
    base_cpi = merged["cpi"].iloc[-1]
    adj = merged.drop(columns=["cpi"]).div(merged["cpi"], axis=0).mul(base_cpi)

    adj.plot(title="Inflation-Adjusted Asset Prices", figsize=(12,6))
    plt.ylabel("Real Price in USD")
    plt.xlabel("Date")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("asset_prices_adjusted.png")
    plt.show()

    conn.close()

# ----------------------------
# Main Runner
# ----------------------------
if __name__ == "__main__":
    init_db()

    # Fetch crypto: Bitcoin
    fetch_and_store_crypto("bitcoin", "BTC")

    # Fetch traditional assets: Gold (GLD), S&P500 (SPY)
    fetch_and_store_yfinance("GLD", "GLD")
    fetch_and_store_yfinance("SPY", "SPY")

    # Fetch CPI data from FRED
    fetch_and_store_cpi()

    # Fetch Crude Oil Prices from EIA
    fetch_and_store_oil_prices()

    # Generate Charts
    generate_charts()
