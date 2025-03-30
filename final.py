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

def get_api_key(index, file):
    with open(file, 'r') as f:
        if index == 1:
            return f.readline().strip()
        elif index == 2:
            f.readline()
            return f.readline().strip()

FRED_API_KEY = get_api_key(1, 'api_key.txt')
EIA_API_KEY = get_api_key(2, 'api_key.txt')

# ----------------------------
# Database Initialization
# ----------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

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

def clear_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM crypto_prices")
    c.execute("DELETE FROM traditional_assets")
    c.execute("DELETE FROM inflation_data")
    conn.commit()
    conn.close()
    print("✔ Database tables cleared.")


# ----------------------------
# Fetch and Store CoinGecko Crypto Prices
# ----------------------------
def fetch_and_store_crypto(asset_id, symbol):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM crypto_prices WHERE asset = ?", (symbol,))
    if c.fetchone()[0] > 0:
        print(f"Skipping API call: crypto data for {symbol} already exists.")
        conn.close()
        return

    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/market_chart?vs_currency=usd&days=max&interval=daily"
    response = requests.get(url)
    try:
        data = response.json()
    except Exception:
        print("Failed to parse crypto JSON response.")
        conn.close()
        return

    df = pd.DataFrame(data["prices"], columns=["timestamp", "price_usd"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby("date")["price_usd"].mean().reset_index()

    for _, row in monthly.iterrows():
        date = row["date"].date().isoformat()
        price = row["price_usd"]
        c.execute("INSERT INTO crypto_prices (date, asset, price_usd) VALUES (?, ?, ?)",
                  (date, symbol, price))

    conn.commit()
    conn.close()

# ----------------------------
# Fetch and Store yfinance Asset Prices (SPY, GLD)
# ----------------------------
def fetch_and_store_yfinance(ticker, label, retries=3):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM traditional_assets WHERE asset = ?", (label,))
    if c.fetchone()[0] > 0:
        print(f"Skipping API call: yfinance data for {label} already exists.")
        conn.close()
        return

    start_date = (datetime.today() - timedelta(days=365 * 20)).strftime("%Y-%m-%d")

    for attempt in range(retries):
        try:
            data = yf.download(ticker, start=start_date, interval="1mo", auto_adjust=False)
            if data.empty:
                raise ValueError("Downloaded data is empty.")
            break
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(10)
    else:
        print(f"Failed to download data for {ticker} after {retries} retries.")
        conn.close()
        return

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
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM inflation_data")
    if c.fetchone()[0] > 0:
        print("Skipping API call: CPI data already exists.")
        conn.close()
        return

    url = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCNS&api_key={FRED_API_KEY}&file_type=json"
    response = requests.get(url)
    try:
        data = response.json()
    except Exception:
        print("Failed to parse CPI JSON response.")
        conn.close()
        return

    if "observations" not in data:
        print("No CPI data returned. Check your FRED API key or series ID.")
        conn.close()
        return

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
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM traditional_assets WHERE asset = 'NATGAS'")
    if c.fetchone()[0] > 0:
        print("Skipping API call: Natural Gas data already exists.")
        conn.close()
        return

    url = f"https://api.eia.gov/series/?api_key={EIA_API_KEY}&series_id=NG.RNGWHHD.D"
    response = requests.get(url)
    try:
        data = response.json()
    except Exception:
        print("Failed to parse EIA JSON response.")
        conn.close()
        return

    if "series" not in data or not data["series"]:
        print("No data returned from EIA. Check your API key and series ID.")
        conn.close()
        return

    for entry in data["series"][0].get("data", []):
        date = entry[0]
        price = float(entry[1])
        c.execute("INSERT INTO traditional_assets (date, asset, price_usd) VALUES (?, ?, ?)",
                  (date, "NATGAS", price))

    conn.commit()
    conn.close()

# ----------------------------
# Final Calculations and Visualizations
# ----------------------------
def calculate_and_visualize_joined_data():
    conn = sqlite3.connect(DB_NAME)

    query = """
    SELECT date, asset, price_usd, cpi
    FROM (
        SELECT t.date, t.asset, t.price_usd, cpi.cpi
        FROM traditional_assets t
        JOIN inflation_data cpi ON t.date = cpi.date
        UNION ALL
        SELECT c.date, c.asset, c.price_usd, cpi.cpi
        FROM crypto_prices c
        JOIN inflation_data cpi ON c.date = cpi.date
    )
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna()
    df = df.sort_values("date")

    df.to_csv("joined_asset_cpi_data.txt", index=False, sep="\t")
    print("✔ Data written to joined_asset_cpi_data.txt")

    pivot_prices = df.pivot(index="date", columns="asset", values="price_usd")
    pivot_cpi = df.groupby("date")["cpi"].mean()
    normalized_prices = pivot_prices.div(pivot_prices.iloc[0]).mul(100)
    normalized_cpi = pivot_cpi.div(pivot_cpi.iloc[0]).mul(100)
    normalized_prices["CPI"] = normalized_cpi

    normalized_prices.plot(title="Normalized Asset Prices and CPI Over Time", figsize=(12, 6))
    plt.ylabel("Indexed Price (Start = 100)")
    plt.xlabel("Date")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("visual_line_normalized_prices.png")
    plt.show()

    daily_returns = normalized_prices.pct_change().dropna()
    corr = daily_returns.corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Between Asset Returns and CPI")
    plt.tight_layout()
    plt.savefig("visual_heatmap_correlations.png")
    plt.show()

    volatility = daily_returns.rolling(window=30).std().mul(100)
    volatility.plot(title="30-Day Rolling Volatility (%): Assets and CPI", figsize=(12, 6))
    plt.ylabel("Volatility (%)")
    plt.xlabel("Date")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("visual_volatility_rolling.png")
    plt.show()

# ----------------------------
# Main Runner
# ----------------------------
if __name__ == "__main__":
    init_db()
    #clear_db()
    fetch_and_store_crypto("bitcoin", "BTC")
    fetch_and_store_yfinance("GLD", "GLD")
    fetch_and_store_yfinance("SPY", "SPY")
    fetch_and_store_cpi()
    fetch_and_store_natgas_prices()
    calculate_and_visualize_joined_data()