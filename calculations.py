import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# === CONNECT TO DATABASE ===
conn = sqlite3.connect("financial_data.db")

# === SQL JOIN QUERY ===
query = """
SELECT
    btc.date AS date,
    btc.close AS btc_price,
    sp.close AS sp500_price,
    gold.close AS gold_price,
    oil.price AS oil_price,
    cpi.cpi_value AS cpi_value
FROM Bitcoin_Prices btc
JOIN SP500_Prices sp ON btc.date = sp.date
JOIN Gold_Prices gold ON btc.date = gold.date
JOIN Oil_Prices oil ON btc.date = oil.date
JOIN CPI_Data cpi ON btc.date = cpi.date
ORDER BY btc.date ASC
"""

# === EXECUTE QUERY ===
merged = pd.read_sql_query(query, conn, parse_dates=["date"])
conn.close()

# === CALCULATIONS ===
merged['btc_to_cpi'] = merged['btc_price'] / merged['cpi_value']
merged['sp500_to_cpi'] = merged['sp500_price'] / merged['cpi_value']
merged['gold_to_cpi'] = merged['gold_price'] / merged['cpi_value']
merged['oil_to_cpi'] = merged['oil_price'] / merged['cpi_value']

# === WRITE TO TEXT FILE ===
with open("calculations_output.txt", "w") as f:
    f.write("Price-to-CPI Ratios (First 10 Rows):\n")
    f.write(merged[['date', 'btc_to_cpi', 'sp500_to_cpi', 'gold_to_cpi', 'oil_to_cpi']].head(10).to_string(index=False))
    f.write("\n\n")

    f.write("Correlation Matrix:\n")
    corr = merged[['btc_price', 'sp500_price', 'gold_price', 'oil_price', 'cpi_value']].corr()
    f.write(corr.to_string())
    f.write("\n")

# === GRAPH 1: Price-to-CPI Ratios Over Time ===
plt.figure(figsize=(12, 6))

# Normalize each series to start at 100
normalized = merged[['date', 'btc_to_cpi', 'sp500_to_cpi', 'gold_to_cpi', 'oil_to_cpi']].copy()
for col in ['btc_to_cpi', 'sp500_to_cpi', 'gold_to_cpi', 'oil_to_cpi']:
    normalized[col] = normalized[col] / normalized[col].iloc[0] * 100

# Plot normalized series
plt.plot(normalized['date'], normalized['btc_to_cpi'], label='Bitcoin / CPI')
plt.plot(normalized['date'], normalized['sp500_to_cpi'], label='S&P500 / CPI')
plt.plot(normalized['date'], normalized['gold_to_cpi'], label='Gold / CPI')
plt.plot(normalized['date'], normalized['oil_to_cpi'], label='Oil / CPI')

plt.xlabel('Date')
plt.ylabel('Normalized Price-to-CPI Ratio (Base = 100)')
plt.title('Normalized Price-to-CPI Ratios Over Time')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("price_to_cpi_ratio.png")
plt.close()

# === GRAPH 2: Bitcoin vs Gold Prices Scatterplot ===
plt.figure(figsize=(8, 6))
sns.scatterplot(data=merged, x='gold_price', y='btc_price')
plt.xlabel("Gold Price (USD)")
plt.ylabel("Bitcoin Price (USD)")
plt.title("Bitcoin vs Gold Prices")
plt.grid(True)
plt.tight_layout()
plt.savefig("btc_vs_gold_scatter.png")
plt.close()

# === RESAMPLE TO MONTHLY AVERAGE PRICES ===
monthly = merged.set_index('date').resample('M').mean().dropna()

# === CALCULATE MONTHLY % RETURNS ===
monthly_returns = monthly[['btc_price', 'sp500_price', 'gold_price', 'oil_price']].pct_change().dropna()
average_returns = monthly_returns.mean() * 100  # Convert to %

# === WRITE TO FILE ===
with open("calculations_output.txt", "a") as f:
    f.write("\nAverage Monthly Returns (%):\n")
    f.write(average_returns.to_string())
    f.write("\n")

# === GRAPH 3: Average Monthly Returns Bar Chart ===
plt.figure(figsize=(8, 6))
average_returns.plot(kind='bar')
plt.title('Average Monthly Returns (%)')
plt.ylabel('Average % Return')
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(True)
plt.savefig("avg_monthly_returns.png")
plt.close()

# === GRAPH 4: Correlation Heatmap ===
plt.figure(figsize=(8, 6))
sns.heatmap(merged[['btc_price', 'sp500_price', 'gold_price', 'oil_price', 'cpi_value']].corr(), annot=True, cmap='coolwarm')
plt.title("Correlation Heatmap Between Assets")
plt.tight_layout()
plt.savefig("correlation_heatmap.png")
plt.close()

# === CALCULATE VOLATILITY (Standard Deviation of Monthly Returns) ===
volatility = monthly_returns.std() * 100  # Convert to %

# === WRITE TO FILE ===
with open("calculations_output.txt", "a") as f:
    f.write("\nVolatility of Monthly Returns (%):\n")
    f.write(volatility.to_string())
    f.write("\n")

# === GRAPH 5: Volatility Bar Chart ===
plt.figure(figsize=(8, 6))
volatility.plot(kind='bar', color='orange')
plt.title('Volatility of Monthly Returns (%)')
plt.ylabel('Standard Deviation (%)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(True)
plt.savefig("volatility_bar_chart.png")
plt.close()
