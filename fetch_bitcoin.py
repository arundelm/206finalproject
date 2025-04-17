import yfinance as yf
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === FUNCTION: Fetch and store 25 monthly Bitcoin prices per call ===
def fetch_and_store_bitcoin():
    # Connect to SQLite database
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Ensure Combined_Prices table exists with btc_price column
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

        # Count how many BTC entries already exist (not NULL)
        c.execute("SELECT COUNT(btc_price) FROM Combined_Prices WHERE btc_price IS NOT NULL")
        current_count = c.fetchone()[0]

        # Exit if 100 BTC values already stored
        if current_count >= 100:
            print("100 Bitcoin data points already stored.")
            return

        # Determine chunk to fetch
        chunk_index = current_count // 25
        chunk_size = 25
        base_start = datetime(2016, 7, 1)
        chunk_start = base_start + relativedelta(months=chunk_index * chunk_size)
        chunk_end = chunk_start + relativedelta(months=chunk_size)

        print(f"Fetching chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

        # Download daily BTC data from yfinance
        data = yf.download(
            "BTC-USD",
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False
        )

        # Exit if no data was returned
        if data is None or data.empty:
            print("No data returned from yfinance.")
            return

        # Select one entry per month — first available "Open" value
        monthly_data = {}
        for dt, row in data.iterrows():
            dt_obj = dt.to_pydatetime()
            ym_key = (dt_obj.year, dt_obj.month)
            if ym_key not in monthly_data:
                try:
                    price = float(row["Open"].item())
                    monthly_data[ym_key] = (dt_obj, price)
                except:
                    continue

            if len(monthly_data) == 25:
                break

        # Insert results into Combined_Prices
        inserted = 0
        for (_, _), (dt, price) in sorted(monthly_data.items()):
            date_str = dt.strftime("%Y-%m")
            c.execute("""
                INSERT INTO Combined_Prices (date, btc_price)
                VALUES (?, ?)
                ON CONFLICT(date) DO UPDATE SET btc_price = excluded.btc_price
            """, (date_str, price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} BTC entries. Total should now be {current_count + inserted}.")

# === RUN FUNCTION IF SCRIPT IS CALLED DIRECTLY ===
if __name__ == '__main__':
    fetch_and_store_bitcoin()
