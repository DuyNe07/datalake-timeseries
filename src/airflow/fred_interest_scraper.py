import requests
import os
import pandas as pd
from datetime import datetime, timedelta

FRED_API_KEY = "9995f798cc4f1b0fd8464bffc9e6c0e4"
INTEREST_SERIES_ID = "REAINTRATREARAT10Y"

SAVE_DIR = "/src/data/raw"
INTEREST_DIR = os.path.join(SAVE_DIR, "Interest")
os.makedirs(INTEREST_DIR, exist_ok=True)

def get_fred_series(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "frequency": "m",
        "sort_order": "desc"
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json().get("observations", [])

def spread_to_daily(year, month, value):
    start_date = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    num_days = (next_month - start_date).days
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    return pd.DataFrame({
        "date": dates,
        "interest_rate": value
    })

def main():
    data = get_fred_series(INTEREST_SERIES_ID)
    recent_data = [obs for obs in data if obs["value"] != "."]
    recent_data = recent_data[:2]  # latest 2 months only

    for obs in recent_data:
        try:
            val = float(obs["value"])
        except ValueError:
            continue
        df = spread_to_daily(y, m, val)
        df.reset_index(inplace=True)
        df.rename(columns={"index": "Id"}, inplace=True)
        file_path = os.path.join(INTEREST_DIR, f"interest_{datetime.date.today().strftime('%d_%m_%Y')}.csv")
        df.to_csv(file_path, index=False, columns=["Id", "date", "interest_rate"])
        print(f"Saved: {file_path}")

if __name__ == "__main__":
    main()