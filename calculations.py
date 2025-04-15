import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# === CONNECT AND FETCH DATA WITHOUT PANDAS ===
conn = sqlite3.connect("financial_data.db")
cursor = conn.cursor()

query = """
SELECT
    btc.date AS date,
    btc.price AS btc_price,
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
cursor.execute(query)
rows = cursor.fetchall()
conn.close()

# === PARSE DATA ===
dates, btc, sp, gold, oil, cpi = [], [], [], [], [], []
for row in rows:
    dates.append(datetime.strptime(row[0], "%Y-%m-%d"))
    btc.append(row[1])
    sp.append(row[2])
    gold.append(row[3])
    oil.append(row[4])
    cpi.append(row[5])

# === CALCULATIONS ===
btc_to_cpi = [b / c for b, c in zip(btc, cpi)]
sp_to_cpi = [s / c for s, c in zip(sp, cpi)]
gold_to_cpi = [g / c for g, c in zip(gold, cpi)]
oil_to_cpi = [o / c for o, c in zip(oil, cpi)]

# === WRITE FIRST 10 RATIO ROWS ===
with open("calculations_output.txt", "w") as f:
    f.write("Price-to-CPI Ratios (First 10 Rows):\n")
    for i in range(20):
        f.write(f"{dates[i].strftime('%Y-%m-%d')}: BTC/CPI={btc_to_cpi[i]:.2f}, SP500/CPI={sp_to_cpi[i]:.2f}, Gold/CPI={gold_to_cpi[i]:.2f}, Oil/CPI={oil_to_cpi[i]:.2f}\n")

# === CORRELATION MATRIX ===
data_matrix = np.array([btc, sp, gold, oil, cpi])
corr_matrix = np.corrcoef(data_matrix)
labels = ['btc', 'sp500', 'gold', 'oil', 'cpi']

with open("calculations_output.txt", "a") as f:
    f.write("\nCorrelation Matrix:\n")
    f.write('\t' + '\t'.join(labels) + '\n')
    for i, row in enumerate(corr_matrix):
        f.write(labels[i] + '\t' + '\t'.join(f"{val:.2f}" for val in row) + '\n')

# === GRAPH 1: Normalized Price-to-CPI Ratios ===
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

# === GRAPH 2: Bitcoin vs Gold Prices Scatterplot ===
plt.figure(figsize=(8, 6))
sns.scatterplot(x=gold, y=btc)
plt.xlabel("Gold Price (USD)")
plt.ylabel("Bitcoin Price (USD)")
plt.title("Bitcoin vs Gold Prices")
plt.grid(True)
plt.tight_layout()
plt.savefig("btc_vs_gold_scatter.png")
plt.close()

# === MANUAL MONTHLY RETURNS ===
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

# === AVERAGE RETURNS ===
average_returns = {k: np.mean(v) * 100 for k, v in returns.items()}
with open("calculations_output.txt", "a") as f:
    f.write("\nAverage Monthly Returns (%):\n")
    for asset, avg in average_returns.items():
        f.write(f"{asset}: {avg:.2f}%\n")

# === GRAPH 3: Average Monthly Returns ===
plt.figure(figsize=(8, 6))
plt.bar(average_returns.keys(), average_returns.values())
plt.title("Average Monthly Returns (%)")
plt.ylabel("Average % Return")
plt.grid(True)
plt.tight_layout()
plt.savefig("avg_monthly_returns.png")
plt.close()

# === GRAPH 4: Correlation Heatmap ===
plt.figure(figsize=(8, 6))
sns.heatmap(corr_matrix, xticklabels=labels, yticklabels=labels, annot=True, cmap='coolwarm')
plt.title("Correlation Heatmap Between Assets")
plt.tight_layout()
plt.savefig("correlation_heatmap.png")
plt.close()

# === VOLATILITY ===
volatility = {k: np.std(v) * 100 for k, v in returns.items()}
with open("calculations_output.txt", "a") as f:
    f.write("\nVolatility of Monthly Returns (%):\n")
    for asset, vol in volatility.items():
        f.write(f"{asset}: {vol:.2f}%\n")

# === GRAPH 5: Volatility Bar Chart ===
plt.figure(figsize=(8, 6))
plt.bar(volatility.keys(), volatility.values(), color='orange')
plt.title("Volatility of Monthly Returns (%)")
plt.ylabel("Standard Deviation (%)")
plt.grid(True)
plt.tight_layout()
plt.savefig("volatility_bar_chart.png")
plt.close()
