
# import requests
# import time
# import datetime
# import yfinance as yf

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import json
import time

def get_nifty_50_list():
    # Step 1: Use Selenium to get session cookies
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        print("🌐 Opening NSE homepage to set cookies...")
        driver.get("https://www.nseindia.com")
        time.sleep(3)

        # Extract cookies
        cookies = driver.get_cookies()
        driver.quit()

        # Convert cookies into a dict for requests
        cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}

        # Step 2: Make the request using requests and cookies
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive"
        }

        session = requests.Session()
        session.headers.update(headers)
        session.cookies.update(cookie_dict)

        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        print("📊 Fetching NIFTY 50 data...")
        response = session.get(url, timeout=10)

        if response.status_code != 200:
            raise Exception(f"NSE API returned status code {response.status_code}")

        data = response.json()

        stocks = [
            {
                'symbol': stock['symbol'],
                'name': stock.get('meta', {}).get('companyName', stock['symbol']),
                'lastPrice': stock['lastPrice'],
                'change': stock['change'],
                'pChange': stock['pChange']
            }
            for stock in data['data']
        ]

        print(f"✅ Extracted {len(stocks)} stocks")
        return stocks

    except Exception as e:
        print("❌ Error:", e)
        return []






# Step 2: Yahoo Finance (v8) historical data
def fetch_yahoo_chart(symbol="RELIANCE.NS", range_period="1mo", interval="1d"):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval={interval}&range={range_period}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://finance.yahoo.com/"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        json_data = response.json()

        if json_data.get("chart", {}).get("error"):
            print("❌ Yahoo Finance error:", json_data["chart"]["error"])
            return []

        chart = json_data["chart"]["result"][0]
        timestamps = chart["timestamp"]
        quote = chart["indicators"]["quote"][0]

        data = [
            {
                "date": datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d"),
                "open": quote["open"][i],
                "high": quote["high"][i],
                "low": quote["low"][i],
                "close": quote["close"][i],
                "volume": quote["volume"][i]
            }
            for i, t in enumerate(timestamps)
        ]

        return data

    except Exception as e:
        print("❌ Error fetching Yahoo Finance data:", str(e))
        return []

# Step 3: Historical data using yfinance (alternative to yahoo-finance2)
def fetch_yfinance_data(symbol="RELIANCE.NS", start="2024-07-01", end="2025-07-30"):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, end=end, interval="1d")
        if df.empty:
            print("⚠️ No data found.")
            return []
        return df.reset_index().to_dict(orient="records")
    except Exception as e:
        print("❌ yfinance error:", str(e))
        return []


# ---------- RUN EXAMPLES ----------

# Test
if __name__ == "__main__":
    stocks = get_nifty_50_list()
    print(stocks[:3])  # Show sample  # show first 3

# # Example: Get Yahoo Finance historical chart data
# yahoo_data = fetch_yahoo_chart("RELIANCE.NS", "1mo", "1d")
# print(yahoo_data[:3])  # show first 3

# # Example: Get historical data with yfinance
# yf_data = fetch_yfinance_data("RELIANCE.NS", "2024-07-01", "2025-07-30")
# print(yf_data[:3])  # show first 3
