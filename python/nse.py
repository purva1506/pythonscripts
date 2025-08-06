import requests
import pandas as pd
from io import StringIO

def fetch_all_nse_listed_companies():
    url = "https://www1.nseindia.com/content/equities/EQUITY_L.csv"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com/"
    }

    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'

        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}")

        df = pd.read_csv(
            StringIO(response.text),
            skipinitialspace=True,
            on_bad_lines='skip',
            encoding='utf-8'
        )

        # Use corrected column names
        df = df[['Symbol', 'Company Name', 'ISIN No', 'Face Value']]
        df.columns = ['symbol', 'company_name', 'isin', 'face_value']

        print(f"✅ Loaded {len(df)} companies from NSE CSV")
        return df

    except Exception as e:
        print("❌ Failed to load NSE CSV:", str(e))
        return pd.DataFrame()

# Run and preview first 5 companies
df = fetch_all_nse_listed_companies()
print(df.head())
