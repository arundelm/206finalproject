import yfinance as yf
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

def fetch_and_store_bitcoin():
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Create table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS Bitcoin_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                price REAL
            )
        """)

        # Get how many rows already exist
        c.execute("SELECT COUNT(*) FROM Bitcoin_Prices")
        current_count = c.fetchone()[0]

        if current_count >= 100:
            print("100 Bitcoin data points already stored.")
            return

        # Determine date chunk
        chunk_index = current_count // 25
        chunk_size = 25
        base_start = datetime(2016, 7, 1)
        chunk_start = base_start + relativedelta(months=chunk_index * chunk_size)
        chunk_end = chunk_start + relativedelta(months=chunk_size)

        print(f"Fetching chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

        # Download daily data for the chunk
        data = yf.download(
            "BTC-USD",
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False
        )

        if data is None or data.empty:
            print("No data returned from yfinance.")
            return

        # Get first price of each month
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

        inserted = 0
        for (_, _), (dt, price) in sorted(monthly_data.items()):
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO Bitcoin_Prices (date, price) VALUES (?, ?)", (date_str, price))
            inserted += 1

        conn.commit()
        print(f"Inserted {inserted} BTC entries. Total should now be {current_count + inserted}.")

if __name__ == '__main__':
  
    fetch_and_store_bitcoin()
