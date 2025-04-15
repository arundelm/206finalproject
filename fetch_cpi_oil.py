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
from dateutil.relativedelta import relativedelta
from get_api_key import get_api_key

FRED_API_KEY = get_api_key(2)

def fetch_and_store_cpi():

    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS CPI_Data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                cpi_value REAL
            )
        """)

        c.execute("SELECT COUNT(*) FROM CPI_Data")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 CPI entries already stored.")
            return

        # Fetch full CPI data
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "CPIAUCSL",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2016-07-01"
        }
        response = requests.get(url, params=params).json()
        raw = response.get("observations", [])

        # One entry per month, stored by (year, month)
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
        chunk_start = current_count
        chunk_end = min(chunk_start + 25, len(sorted_months))
        chunk = sorted_months[chunk_start:chunk_end]

        inserted = 0
        for (_, _), (dt, value) in chunk:
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO CPI_Data (date, cpi_value) VALUES (?, ?)", (date_str, value))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} CPI entries. Total: {current_count + inserted}")

def fetch_and_store_oil():

    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS Oil_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                price REAL
            )
        """)

        c.execute("SELECT COUNT(*) FROM Oil_Prices")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 oil price entries already stored.")
            return

        # Fetch oil data
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DCOILWTICO",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2016-07-01"
        }

        response = requests.get(url, params=params).json()
        raw = response.get("observations", [])

        # Deduplicate by month
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
        chunk_start = current_count
        chunk_end = min(chunk_start + 25, len(sorted_months))
        chunk = sorted_months[chunk_start:chunk_end]

        inserted = 0
        for (_, _), (dt, price) in chunk:
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO Oil_Prices (date, price) VALUES (?, ?)", (date_str, price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} oil price entries. Total: {current_count + inserted}")

if __name__ == '__main__':
    fetch_and_store_cpi()
    fetch_and_store_oil()
