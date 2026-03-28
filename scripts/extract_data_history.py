import os
import sys
import time
import requests
import pandas as pd
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

@dataclass
class MarketRecord:
    """Defines the schema for the data warehouse table."""
    date: datetime
    price: float
    total_volume: float
    market_cap: float
    coin_id: str

def to_unix_seconds(dt: datetime):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def fetch_coingecko_range(coin_id: str, start_date_str: str, end_date_str: str):
    """
    Fetches a time-range snapshot for a specific coin.
    start_date_str/end_date_str format: 'YYYY-MM-DD'
    """
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Convert to Unix seconds (Coingecko expects seconds, not ms)
    from_ts = to_unix_seconds(start_dt)
    to_ts = to_unix_seconds(end_dt)

    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": from_ts,
        "to": to_ts,
        "interval": "daily"
    }

    try:
        # Wait for rate limits
        time.sleep(1)
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        prices = data.get("prices", [])
        total_volumes = data.get("total_volumes", [])
        market_caps = data.get("market_caps", [])

        if not prices:
            print(f"No range market data fetched for {coin_id} from {start_date_str} to {end_date_str}")
            return []

        # Build dict of values keyed by timestamp_ms for safe merging
        chart_by_ts = {}
        for ts_ms, price in prices:
            chart_by_ts[ts_ms] = {
                "date": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                "price": float(price),
                "total_volume": 0.0,
                "market_cap": 0.0,
                "coin_id": coin_id
            }

        for ts_ms, volume in total_volumes:
            if ts_ms in chart_by_ts:
                chart_by_ts[ts_ms]["total_volume"] = float(volume)

        for ts_ms, cap in market_caps:
            if ts_ms in chart_by_ts:
                chart_by_ts[ts_ms]["market_cap"] = float(cap)

        return [MarketRecord(**record) for record in chart_by_ts.values()]

    except Exception as e:
        print(f"Error fetching {coin_id} from {start_date_str} to {end_date_str}: {e}")
        return []

def main():
    # Receive date variable from Kestra
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if not start_date or not end_date:
        print("START_DATE or END_DATE environment variable is missing.")
        sys.exit(1)

    # List of coins to analyze, can be expanded to more coins in the future
    coins_to_fetch = ["bitcoin", "tether"]
    
    all_results = []
    for coin in coins_to_fetch:
        print(f"Starting range extraction for {coin} from {start_date} to {end_date}...")
        results = fetch_coingecko_range(coin, start_date, end_date)
        if results:
            for record in results:
                all_results.append(asdict(record))

    if all_results:
        # Combine all coins into a single DataFrame
        df = pd.DataFrame(all_results)
        output_file = "history_crypto_market_data.parquet"
        
        # Save as Parquet - Kestra will capture this file as an output
        df.to_parquet(output_file, index=False, engine='pyarrow')
        print(f"Saved data of {coins_to_fetch} for {start_date} to {end_date}")
    else:
        print("Data collection failed: No records retrieved.")
        sys.exit(1)   # Exit the program with an error

if __name__ == "__main__":
    main()