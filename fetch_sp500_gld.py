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

       
        # Count how many SP500 prices are already inserted
        c.execute("SELECT COUNT(sp500_price) FROM Combined_Prices WHERE sp500_price IS NOT NULL")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 SP500 data points already stored.")
            return

        # Determine date chunk to fetch
        chunk_index = current_count // 25
        chunk_size = 25
        base_start = datetime(2016, 7, 1)
        chunk_start = base_start + relativedelta(months=chunk_index * chunk_size)
        chunk_end = chunk_start + relativedelta(months=chunk_size)

        print(f"Fetching SP500 chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

        # Fetch daily SP500 (SPY) data for the chunk
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

        # Pick first entry for each month
        monthly_data = {}
        for dt, row in data.iterrows():
            dt_obj = dt.to_pydatetime()
            ym_key = (dt_obj.year, dt_obj.month)
            if ym_key not in monthly_data:
                try:
                    price = float(row["Close"].item())
                    monthly_data[ym_key] = (dt_obj, price)
                except:
                    continue
            if len(monthly_data) == 25:
                break

        inserted = 0
        for (_, _), (dt, price) in sorted(monthly_data.items()):
            date_str = dt.strftime("%Y-%m")
            c.execute("""
                INSERT INTO Combined_Prices (date, sp500_price)
                VALUES (?, ?)
                ON CONFLICT(date) DO UPDATE SET sp500_price = excluded.sp500_price
            """, (date_str, price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} SP500 entries. Total should now be {current_count + inserted}.")

# === Function: Fetch and store Gold (GLD) data ===
def fetch_and_store_gold():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()


        # Create Gold_Change table (0 = down, 1 = up)
        c.execute("""
            CREATE TABLE IF NOT EXISTS Gold_Change (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label string
            )
        """)
        c.execute("INSERT OR IGNORE INTO Gold_Change (id, label) VALUES (0, 'down')")
        c.execute("INSERT OR IGNORE INTO Gold_Change (id, label) VALUES (1, 'up')")

        c.execute("SELECT COUNT(gold_open) FROM Combined_Prices WHERE gold_open IS NOT NULL")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 Gold entries already exist.")
            return

        chunk_index = current_count // 25
        base_start = datetime(2016, 7, 1)
        chunk_start = base_start + relativedelta(months=chunk_index * 25)
        chunk_end = chunk_start + relativedelta(months=25)

        print(f"Fetching Gold chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

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
            print("No Gold data returned.")
            return

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
                except:
                    continue
            if len(monthly_data) == 25:
                break

        inserted = 0
        for (_, _), (dt, open_price, close_price) in sorted(monthly_data.items()):
            direction = 1 if close_price > open_price else 0

            date_str = dt.strftime("%Y-%m")
            c.execute("""
            INSERT INTO Combined_Prices (date, gold_open, gold_close, gold_change)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                gold_open = excluded.gold_open,
                gold_close = excluded.gold_close,
                gold_change = excluded.gold_change
        """, (date_str, open_price, close_price, direction))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} new Gold entries. Total now: {current_count + inserted}.")

# === MAIN EXECUTION ===
# Run both fetch functions when this file is executed directly
if __name__ == '__main__':
    fetch_and_store_sp500()
    fetch_and_store_gold()
