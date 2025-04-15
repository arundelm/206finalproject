# === Function: get_api_key ===
# Reads API keys from a `.env` file and returns one based on the provided index.
# Used to keep API keys secure and avoid hardcoding them in scripts.

def get_api_key(index):
    # Open the .env file in read mode
    with open('.env', 'r') as f:
        if index == 1:
            # Return the first line (e.g., AlphaVantage API key)
            return f.readline().strip()
        elif index == 2:
            # Skip the first line and return the second line (e.g., FRED API key)
            f.readline()
            return f.readline().strip()
