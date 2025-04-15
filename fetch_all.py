from fetch_bitcoin import fetch_and_store_bitcoin
from fetch_cpi_oil import fetch_and_store_cpi, fetch_and_store_oil
from fetch_sp500_gld import fetch_and_store_gold, fetch_and_store_sp500

if __name__ == '__main__':
    fetch_and_store_bitcoin()
    fetch_and_store_sp500()
    fetch_and_store_gold()
    fetch_and_store_cpi()
    fetch_and_store_oil()
    
