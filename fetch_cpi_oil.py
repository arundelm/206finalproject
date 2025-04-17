# === fetch_cpi_oil.py ===
# Fetches and stores CPI and Crude Oil data into the Combined_Prices table (shared date key)

import requests
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from get_api_key import get_api_key

FRED_API_KEY = get_api_key(2)

# === Fetch and store CPI values into Combined_Prices ===
def fetch_and_store_cpi():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Ensure Combined_Prices table exists
        c.execute("""
            CREATE TABLE IF NOT EXISTS Combined_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                btc_price REAL,
                sp500_price REAL,
                gold_open REAL,
                gold_close REAL,
                gold_change INTEGER,
                oil_price REAL,
                cpi_value REAL
            )
        """)

        # Count existing CPI entries (i.e., rows where cpi_value is not NULL)
        c.execute("SELECT COUNT(cpi_value) FROM Combined_Prices WHERE cpi_value IS NOT NULL")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 CPI entries already stored.")
            return

        # Request monthly CPI data from FRED
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "CPIAUCSL",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2016-07-01"
        }
        response = requests.get(url, params=params).json()
        raw = response.get("observations", [])

        # Filter to one entry per month
        monthly_data = {}
        for obs in raw:
            if obs["value"] == ".":
                continue
            try:
                dt = datetime.strptime(obs["date"], "%Y-%m-%d")
                ym_key = (dt.year, dt.month)
                if ym_key not in monthly_data:
                    monthly_data[ym_key] = (dt, float(obs["value"]))
            except:
                continue

        # Select the next 25 values to insert
        sorted_months = sorted(monthly_data.items())
        chunk = sorted_months[current_count:current_count + 25]

        inserted = 0
        for (_, _), (dt, value) in chunk:
            date_str = dt.strftime("%Y-%m")
            c.execute("""
                INSERT INTO Combined_Prices (date, cpi_value)
                VALUES (?, ?)
                ON CONFLICT(date) DO UPDATE SET cpi_value = excluded.cpi_value
            """, (date_str, value))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} CPI entries. Total: {current_count + inserted}")

# === Fetch and store Oil prices into Combined_Prices ===
def fetch_and_store_oil():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Count existing oil entries
        c.execute("SELECT COUNT(oil_price) FROM Combined_Prices WHERE oil_price IS NOT NULL")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 oil price entries already stored.")
            return

        # Request oil data
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DCOILWTICO",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2016-07-01"
        }

        response = requests.get(url, params=params).json()
        raw = response.get("observations", [])

        monthly_data = {}
        for obs in raw:
            if obs["value"] == ".":
                continue
            try:
                dt = datetime.strptime(obs["date"], "%Y-%m-%d")
                ym_key = (dt.year, dt.month)
                if ym_key not in monthly_data:
                    monthly_data[ym_key] = (dt, float(obs["value"]))
            except:
                continue

        sorted_months = sorted(monthly_data.items())
        chunk = sorted_months[current_count:current_count + 25]

        inserted = 0
        for (_, _), (dt, price) in chunk:
            date_str = dt.strftime("%Y-%m")
            c.execute("""
                INSERT INTO Combined_Prices (date, oil_price)
                VALUES (?, ?)
                ON CONFLICT(date) DO UPDATE SET oil_price = excluded.oil_price
            """, (date_str, price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} oil price entries. Total: {current_count + inserted}")

# === MAIN EXECUTION ===
if __name__ == '__main__':
    fetch_and_store_cpi()
    fetch_and_store_oil()
