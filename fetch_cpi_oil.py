# === fetch_cpi_oil.py ===
# Fetches and stores CPI and Crude Oil data from the FRED API into SQLite database.
# Adds 25 new monthly datapoints per run, up to a max of 100 for each dataset.

import requests
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from get_api_key import get_api_key  # Securely fetches your FRED API key

# Load the API key from secure storage
FRED_API_KEY = get_api_key(2)

# === Function: Fetch and store CPI data from FRED ===
def fetch_and_store_cpi():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Create table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS CPI_Data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                cpi_value REAL
            )
        """)

        # Count how many rows already exist in the table
        c.execute("SELECT COUNT(*) FROM CPI_Data")
        current_count = c.fetchone()[0]

        # Stop if we already have 100 rows
        if current_count >= 100:
            print("100 CPI entries already stored.")
            return

        # Request full CPI data (monthly) from FRED
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "CPIAUCSL",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2016-07-01"
        }
        response = requests.get(url, params=params).json()
        raw = response.get("observations", [])

        # Filter to one CPI value per (year, month)
        monthly_data = {}
        for obs in raw:
            if obs["value"] == ".":
                continue  # Skip missing data
            try:
                dt = datetime.strptime(obs["date"], "%Y-%m-%d")
                ym_key = (dt.year, dt.month)
                if ym_key not in monthly_data:
                    monthly_data[ym_key] = (dt, float(obs["value"]))
            except:
                continue

        # Select the next 25 entries to insert
        sorted_months = sorted(monthly_data.items())
        chunk_start = current_count
        chunk_end = min(chunk_start + 25, len(sorted_months))
        chunk = sorted_months[chunk_start:chunk_end]

        # Insert new rows into the database
        inserted = 0
        for (_, _), (dt, value) in chunk:
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO CPI_Data (date, cpi_value) VALUES (?, ?)", (date_str, value))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} CPI entries. Total: {current_count + inserted}")

# === Function: Fetch and store oil price data from FRED ===
def fetch_and_store_oil():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Create Oil_Prices table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS Oil_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                price REAL
            )
        """)

        # Check current row count
        c.execute("SELECT COUNT(*) FROM Oil_Prices")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 oil price entries already stored.")
            return

        # Fetch daily oil data from FRED
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DCOILWTICO",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": "2016-07-01"
        }

        response = requests.get(url, params=params).json()
        raw = response.get("observations", [])

        # Filter to one entry per (year, month)
        monthly_data = {}
        for obs in raw:
            if obs["value"] == ".":
                continue  # Skip missing values
            try:
                dt = datetime.strptime(obs["date"], "%Y-%m-%d")
                ym_key = (dt.year, dt.month)
                if ym_key not in monthly_data:
                    monthly_data[ym_key] = (dt, float(obs["value"]))
            except:
                continue

        # Pick the next 25 monthly entries based on how many exist already
        sorted_months = sorted(monthly_data.items())
        chunk_start = current_count
        chunk_end = min(chunk_start + 25, len(sorted_months))
        chunk = sorted_months[chunk_start:chunk_end]

        # Insert new oil data rows
        inserted = 0
        for (_, _), (dt, price) in chunk:
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO Oil_Prices (date, price) VALUES (?, ?)", (date_str, price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} oil price entries. Total: {current_count + inserted}")

# === MAIN EXECUTION ===
# Call both functions if the file is run directly
if __name__ == '__main__':
    fetch_and_store_cpi()
    fetch_and_store_oil()
