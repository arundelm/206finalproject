import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# === CONNECT TO DATABASE AND FETCH DATA ===
conn = sqlite3.connect("financial_data.db")
cursor = conn.cursor()

# Fetch data from Combined_Prices table
query = """
SELECT 
    date, 
    btc_price, 
    sp500_price, 
    gold_close, 
    oil_price, 
    cpi_value,
    gold_change
FROM Combined_Prices
ORDER BY date ASC
"""
cursor.execute(query)
rows = cursor.fetchall()

# === PARSE RAW SQL DATA INTO LISTS ===
dates, btc, sp, gold, oil, cpi, gold_changes = [], [], [], [], [], [], []

for row in rows:
    dates.append(datetime.strptime(row[0], "%Y-%m"))
    btc.append(row[1])
    sp.append(row[2])
    gold.append(row[3])
    oil.append(row[4])
    cpi.append(row[5])


# === CALCULATE PRICE-TO-CPI RATIOS ===
btc_to_cpi = [b / c for b, c in zip(btc, cpi)]
sp_to_cpi = [s / c for s, c in zip(sp, cpi)]
gold_to_cpi = [g / c for g, c in zip(gold, cpi)]
oil_to_cpi = [o / c for o, c in zip(oil, cpi)]

# === WRITE FIRST 20 PRICE/CPI RATIO ROWS ===
with open("calculations_output.txt", "a") as f:
    f.write("Price-to-CPI Ratios (First 20 Rows):\n")
    f.write("Date       BTC/CPI  SP500/CPI  Gold/CPI  Oil/CPI\n")
    for i in range(min(20, len(dates))):
        f.write(f"{dates[i].strftime('%Y-%m')}  "
                f"{btc_to_cpi[i]:8.2f}  "
                f"{sp_to_cpi[i]:10.2f}  "
                f"{gold_to_cpi[i]:9.2f}  "
                f"{oil_to_cpi[i]:8.2f}\n")

# === CORRELATION MATRIX ===
data_matrix = np.array([btc, sp, gold, oil, cpi])
corr_matrix = np.corrcoef(data_matrix)
labels = ['btc', 'sp500', 'gold', 'oil', 'cpi']

with open("calculations_output.txt", "a") as f:
    f.write("\nCorrelation Matrix:\n")
    f.write("{:<8}".format("") + "".join(f"{label:<10}" for label in labels) + "\n")
    for i, row in enumerate(corr_matrix):
        f.write(f"{labels[i]:<8}" + "".join(f"{val:<10.2f}" for val in row) + "\n")
# === GRAPH: Normalized Price-to-CPI (Log) ===
plt.figure(figsize=(12, 6))
plt.plot(dates, [x / btc_to_cpi[0] * 100 for x in btc_to_cpi], label='Bitcoin / CPI')
plt.plot(dates, [x / sp_to_cpi[0] * 100 for x in sp_to_cpi], label='S&P500 / CPI')
plt.plot(dates, [x / gold_to_cpi[0] * 100 for x in gold_to_cpi], label='Gold / CPI')
plt.plot(dates, [x / oil_to_cpi[0] * 100 for x in oil_to_cpi], label='Oil / CPI')
plt.xlabel("Date")
plt.ylabel("Log Normalized Price-to-CPI Ratio (Base = 100)")
plt.yscale("log")
plt.title("Normalized Price-to-CPI Ratios Over Time (Log Scale)")
plt.legend()
plt.grid(True, which='both', linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.savefig("price_to_cpi_ratio_log.png")
plt.close()

# === GRAPH: Bitcoin vs Gold ===
plt.figure(figsize=(8, 6))
sns.scatterplot(x=gold, y=btc)
plt.xlabel("Gold Price (USD)")
plt.ylabel("Bitcoin Price (USD)")
plt.title("Bitcoin vs Gold Prices")
plt.grid(True)
plt.tight_layout()
plt.savefig("btc_vs_gold_scatter.png")
plt.close()

# === RETURNS & VOLATILITY ===
returns = {
    "btc": [],
    "sp": [],
    "gold": [],
    "oil": []
}

for i in range(1, len(dates)):
    returns["btc"].append((btc[i] - btc[i - 1]) / btc[i - 1])
    returns["sp"].append((sp[i] - sp[i - 1]) / sp[i - 1])
    returns["gold"].append((gold[i] - gold[i - 1]) / gold[i - 1])
    returns["oil"].append((oil[i] - oil[i - 1]) / oil[i - 1])

# Average Returns
average_returns = {k: np.mean(v) * 100 for k, v in returns.items()}
with open("calculations_output.txt", "a") as f:
    f.write("\nAverage Monthly Returns (%):\n")
    for asset, avg in average_returns.items():
        f.write(f"{asset}: {avg:.2f}%\n")

plt.figure(figsize=(8, 6))
plt.bar(average_returns.keys(), average_returns.values())
plt.title("Average Monthly Returns (%)")
plt.ylabel("Average % Return")
plt.grid(True)
plt.tight_layout()
plt.savefig("avg_monthly_returns.png")
plt.close()

# Correlation Heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(corr_matrix, xticklabels=labels, yticklabels=labels, annot=True, cmap='coolwarm')
plt.title("Correlation Heatmap Between Assets")
plt.tight_layout()
plt.savefig("correlation_heatmap.png")
plt.close()

# Volatility
volatility = {k: np.std(v) * 100 for k, v in returns.items()}
with open("calculations_output.txt", "a") as f:
    f.write("\nVolatility of Monthly Returns (%):\n")
    for asset, vol in volatility.items():
        f.write(f"{asset}: {vol:.2f}%\n")

plt.figure(figsize=(8, 6))
plt.bar(volatility.keys(), volatility.values(), color='orange')
plt.title("Volatility of Monthly Returns (%)")
plt.ylabel("Standard Deviation (%)")
plt.grid(True)
plt.tight_layout()
plt.savefig("volatility_bar_chart.png")
plt.close()


# === CONNECT TO DATABASE AND FETCH GOLD CHANGE DATA ===
conn = sqlite3.connect("financial_data.db")
cursor = conn.cursor()

# Get count of up/down months using Gold_Change.label
cursor.execute("""
    SELECT gc.label, COUNT(*) 
    FROM Combined_Prices cp
    JOIN Gold_Change gc ON cp.gold_change = gc.id
    GROUP BY gc.label
""")

# Store results in a dictionary
change_counts = {"down": 0, "up": 0}
for label, count in cursor.fetchall():
    change_counts[label.lower()] = count

conn.close()

# === WRITE TO FILE ===
with open("calculations_output.txt", "a") as f:
    f.write("\nGold Price Movement Counts:\n")
    f.write(f"Months Gold Went UP:   {change_counts['up']}\n")
    f.write(f"Months Gold Went DOWN: {change_counts['down']}\n")

# === GRAPH: Gold Price Movement Bar Chart ===
# === GRAPH: Gold Price Movement Pie Chart ===
plt.figure(figsize=(6, 6))
labels = ["Down", "Up"]
sizes = [change_counts["down"], change_counts["up"]]
colors = ["red", "green"]
explode = (0.05, 0.05)  # Slightly separate both slices

plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%',
        startangle=90, explode=explode, shadow=True)
plt.title("Monthly Gold Price Movement")
plt.tight_layout()
plt.savefig("gold_up_down_chart.png")
plt.close()
