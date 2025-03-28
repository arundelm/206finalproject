# store_of_value_showdown.py
# Track Gold, Bitcoin, the S&P500, and Natural Gas compared to the CPI (Inflation Index)

import sqlite3
import requests
import yfinance as yf
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
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
        if pd.isna(price).item():
            continue
        price = float(price.iloc[0]) if isinstance(price, pd.Series) else float(price)
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
    if "observations" not in data:
        print("No CPI data returned. Check your FRED API key or series ID.")
        return

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    for obs in data.get("observations", []):
        date = obs.get("date")
        value = obs.get("value")
        try:
            value = float(value)
        except ValueError:
            continue
        c.execute("INSERT INTO inflation_data (date, cpi) VALUES (?, ?)", (date, value))

    conn.commit()
    conn.close()

# ----------------------------
# Fetch and Store Natural Gas Prices from EIA
# ----------------------------
def fetch_and_store_natgas_prices():
    url = f"https://api.eia.gov/series/?api_key={EIA_API_KEY}&series_id=NG.RNGWHHD.D"
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
                  (date, "NATGAS", price))

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
    pivot = pivot.ffill()  # forward-fill gaps to fix charting issues

    # Percentage change chart
    pct_change = pivot.pct_change(fill_method=None).add(1).cumprod().sub(1).mul(100)
    pct_change.plot(title="Asset Price Change from One Year Ago (%)", figsize=(12, 6))
    plt.ylabel("% Change")
    plt.xlabel("Date")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("asset_prices_percent_change.png")
    plt.show()

    # Rolling 30-day volatility
    volatility = pivot.pct_change().rolling(window=30).std().mul(100)
    volatility.plot(title="30-Day Rolling Volatility (%)", figsize=(12, 6))
    plt.ylabel("Volatility (%)")
    plt.xlabel("Date")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("asset_volatility.png")
    plt.show()

    # Correlation heatmap
    corr = pivot.pct_change().corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Between Asset Returns")
    plt.tight_layout()
    plt.savefig("asset_correlation.png")
    plt.show()

    conn.close()

# ----------------------------
# Main Runner
# ----------------------------
if __name__ == "__main__":
    init_db()
    fetch_and_store_crypto("bitcoin", "BTC")
    fetch_and_store_yfinance("GLD", "GLD")
    fetch_and_store_yfinance("SPY", "SPY")
    fetch_and_store_cpi()
    fetch_and_store_natgas_prices()
    generate_charts()
