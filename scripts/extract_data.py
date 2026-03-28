import os
import sys
import time
import requests
import pandas as pd
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class MarketRecord:
    """Defines the schema for the data warehouse table."""
    date: datetime
    price: float
    total_volume: float
    market_cap: float
    coin_id: str

def fetch_coingecko_history(coin_id: str, target_date_str: str):
    """
    Fetches a historical snapshot for a specific coin.
    target_date_str format: 'YYYY-MM-DD'
    """
    # Convert 'YYYY-MM-DD' to 'DD-MM-YYYY' as required by the /history API
    dt_obj = datetime.strptime(target_date_str, "%Y-%m-%d")
    formatted_date = dt_obj.strftime("%d-%m-%Y")

    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    params = {
        "date": formatted_date,
        "localization": "false"
    }

    try:
        # Wait for rate limits
        time.sleep(1) 
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        market_data = data.get("market_data", {})
        if not market_data:
            print(f"No market data fetched for {coin_id} on {formatted_date}")
            return None

        # Extract USD price from nested response
        return MarketRecord(
            date=dt_obj,
            price=market_data.get("current_price", {}).get("usd", 0.0),
            total_volume=market_data.get("total_volume", {}).get("usd", 0.0),
            market_cap=market_data.get("market_cap", {}).get("usd", 0.0),
            coin_id=coin_id
        )
    except Exception as e:
        print(f"Error fetching {coin_id} on {formatted_date}: {e}")
        return None

def main():
    # Receive date variable from Kestra
    target_date = os.getenv("TRIGGER_DATE")

    if not target_date:
        print("TRIGGER_DATE environment variable is missing.")
        sys.exit(1)

    # List of coins to analyze, can be expanded to more coins in the future
    coins_to_fetch = ["bitcoin", "tether"]
    
    all_results = []
    for coin in coins_to_fetch:
        print(f"Starting data extraction for {coin}...")
        result = fetch_coingecko_history(coin, target_date)
        if result:
            all_results.append(asdict(result))
    
    if all_results:
        # Combine all coins into a single DataFrame
        df = pd.DataFrame(all_results)
        output_file = "crypto_market_data.parquet"
        
        # Save as Parquet - Kestra will capture this file as an output
        df.to_parquet(output_file, index=False, engine='pyarrow')
        print(f"Saved data of {coins_to_fetch} for {target_date}")
    else:
        print("Data collection failed: No records retrieved.")
        sys.exit(1)   # Exit the program with an error

if __name__ == "__main__":
    main()