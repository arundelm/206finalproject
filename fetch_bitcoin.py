import yfinance as yf
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === FUNCTION: Fetch and store 25 monthly Bitcoin prices per call ===
def fetch_and_store_bitcoin():
    # Connect to SQLite database
    with sqlite3.connect("financial_data.db") as conn:
        c = conn.cursor()

        # Create the Bitcoin_Prices table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS Bitcoin_Prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                price REAL
            )
        """)

        # Check how many Bitcoin records already exist
        c.execute("SELECT COUNT(*) FROM Bitcoin_Prices")
        current_count = c.fetchone()[0]

        # Stop if we've already inserted 100 datapoints
        if current_count >= 100:
            print("100 Bitcoin data points already stored.")
            return

        # Calculate the chunk to fetch (25 months at a time)
        chunk_index = current_count // 25  # Which 25-month chunk to fetch
        chunk_size = 25  # Fetch 25 months at a time
        base_start = datetime(2016, 7, 1)  # Start from July 2016
        chunk_start = base_start + relativedelta(months=chunk_index * chunk_size)
        chunk_end = chunk_start + relativedelta(months=chunk_size)

        print(f"Fetching chunk {chunk_index + 1}: {chunk_start.date()} to {chunk_end.date()}")

        # Download daily Bitcoin price data from yfinance
        data = yf.download(
            "BTC-USD",
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False
        )

        # Handle case where no data is returned
        if data is None or data.empty:
            print("No data returned from yfinance.")
            return

        # Extract the first available price from each month in the chunk
        monthly_data = {}
        for dt, row in data.iterrows():
            dt_obj = dt.to_pydatetime()
            ym_key = (dt_obj.year, dt_obj.month)
            if ym_key not in monthly_data:
                try:
                    price = float(row["Open"].item())  # Convert price to float
                    monthly_data[ym_key] = (dt_obj, price)
                except:
                    continue  # Skip any rows with bad/missing data

            if len(monthly_data) == 25:
                break  # We only want 25 entries

        # Insert into the database
        inserted = 0
        for (_, _), (dt, price) in sorted(monthly_data.items()):
            date_str = dt.strftime("%Y-%m-%d")
            c.execute("INSERT OR IGNORE INTO Bitcoin_Prices (date, price) VALUES (?, ?)", (date_str, price))
            inserted += 1

        # Commit changes to the database
        conn.commit()
        print(f"Inserted {inserted} BTC entries. Total should now be {current_count + inserted}.")

# === RUN FUNCTION IF SCRIPT IS CALLED DIRECTLY ===
if __name__ == '__main__':
    fetch_and_store_bitcoin()
