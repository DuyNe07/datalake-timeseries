import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

FRED_API_KEY = "9995f798cc4f1b0fd8464bffc9e6c0e4"
CPI_SERIES_ID = "CPIAUCSL"

SAVE_DIR = "/src/data/raw"
INFLATION_DIR = os.path.join(SAVE_DIR, "Inflation")
os.makedirs(INFLATION_DIR, exist_ok=True)

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

def spread_monthly_to_daily(year, month, cpi_value, inflation_value):
    start_date = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    num_days = (next_month - start_date).days
    dates = [start_date + timedelta(days=i) for i in range(num_days)]
    return pd.DataFrame({
        "date": dates,
        "CPI": cpi_value,
        "inflation_rate": inflation_value
    })

def main():
    cpi_data = get_fred_series(CPI_SERIES_ID)
    recent_data = [obs for obs in cpi_data if obs["value"] != "."]
    recent_data = recent_data[:2]  # latest two available months

    if len(recent_data) < 1:
        print("No CPI data available.")
        return

    # First: use real data for last month
    current_date = datetime.today()
    y1, m1, _ = map(int, recent_data[0]["date"].split("-"))
    val1 = float(recent_data[0]["value"])

    y2, m2 = None, None
    val2 = None
    if len(recent_data) >= 2:
        y2, m2, _ = map(int, recent_data[1]["date"].split("-"))
        val2 = float(recent_data[1]["value"])
        inflation = (val1 - val2) / val2 if val2 != 0 else 0
    else:
        inflation = 0

    df1 = spread_monthly_to_daily(y1, m1, val1, inflation)
    df1.reset_index(inplace=True)
    df1.rename(columns={"index": "id"}, inplace=True)
    file_path1 = os.path.join(INFLATION_DIR, f"inflation_{m1:02d}_{y1}.csv")
    df1.to_csv(file_path1, index=False, columns=["id", "date", "CPI", "inflation_rate"])
    print(f"Saved: {file_path1}")

    # Now: copy to current month if data is not yet available
    today = datetime.today()
    current_year = today.year
    current_month = today.month

    # If latest CPI is not for the current month, create placeholder from last available
    if not (y1 == current_year and m1 == current_month):
        print(f"No CPI data yet for {current_year}-{current_month:02d}, copying {y1}-{m1:02d}")
        df2 = spread_monthly_to_daily(current_year, current_month, val1, inflation)
        df2.reset_index(inplace=True)
        df2.rename(columns={"index": "id"}, inplace=True)
        file_path2 = os.path.join(INFLATION_DIR, f"inflation_{datetime.date.today().strftime('%d_%m_%Y')}.csv")
        df2.to_csv(file_path2, index=False, columns=["id", "date", "CPI", "inflation_rate"])
        print(f"Saved placeholder: {file_path2}")
    else:
        print(f"CPI data for {current_year}-{current_month:02d} already available.")