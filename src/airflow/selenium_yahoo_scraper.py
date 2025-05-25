import yfinance as yf
import pandas as pd
import datetime
import os

def parse_date(date_str: str) -> datetime.date:
    return datetime.datetime.strptime(date_str, '%d-%m-%Y').date()

def fetch_data(symbol: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    df = yf.download(symbol, start=start_date, end=end_date, progress=False)
    return df.reset_index()

def save_to_csv(df: pd.DataFrame, name: str, output_dir: str):
    output_dir = os.path.join(output_dir, name)
    name = name.lower().replace(' ', '_')
    filename = f"{name}_{datetime.date.today().strftime('%d_%m_%Y')}.csv"
    path = os.path.join(output_dir, filename)
    df.to_csv(path, index=False)
    print(f"Saved: {path}")

def main(output_dir: str, start_date_str: str):
    dataset = {
        'Gold': 'GC=F',
        'Oil': 'CL=F',
        'US 2 Year Bond': '2YY=F',
        'US 5 Year Bond': '^FVX',
        'US 10 Year Bond': 'ZN=F',
        'US 3 Month Bond': '^IRX',
        'US Dollar': 'DX-Y.NYB',
        'USD_VND': 'VND=X',
        'S_P500': '^GSPC',
        'Dow Jones': '^DJI',
        'NASDAQ100': '^IXIC',
        'Russell2000': '^RUT',
        'MSCI World': '^990100-USD-STRD'
    }

    os.makedirs(output_dir, exist_ok=True)

    start_date = parse_date(start_date_str)
    #end_date = datetime.date.today() + datetime.timedelta(days=1)
    end_date = datetime.date.today()

    for name, symbol in dataset.items():
        print(f"Fetching {name} ({symbol})...")
        try:
            df = fetch_data(symbol, start_date, end_date)
            if not df.empty:
                #df['id'] = df.index + 1
                save_to_csv(df, name, output_dir)
            else:
                print(f"No data for {name}")
        except Exception as e:
            print(f"Failed to fetch {name} ({symbol}): {e}")