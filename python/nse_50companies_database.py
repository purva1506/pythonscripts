import yfinance as yf
import pandas as pd
import os
import pymysql

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
}

# ✅ Ensure DB and Table Exists
def ensure_database_and_table():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_market1")
    conn.select_db("stock_market1")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adj_close FLOAT,
            volume BIGINT,
            UNIQUE(symbol, date)
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

# ✅ Initialize
ensure_database_and_table()
db_config['database'] = 'stock_market1'

# ✅ Fetch ticker symbols from listed_companies table
def get_company_symbols():
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("USE stock_market1")
        cursor.execute("SELECT symbol FROM listed_companies")
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
        print(f"❌ Failed to fetch symbols from DB: {e}")
        return []

# ✅ Get dynamic tickers list
nifty50_tickers = get_company_symbols()

if not nifty50_tickers:
    print("❌ No company symbols fetched from database.")
    exit()

print(f"🚀 Starting for {len(nifty50_tickers)} companies...\n")

# ✅ Main Loop
for idx, ticker in enumerate(nifty50_tickers, start=1):
    symbol = ticker.replace(".NS", "")
    print(f"[{idx}/{len(nifty50_tickers)}] 📈 Fetching {ticker} ...")

    try:
        # Fetch data for the last 1 month
        df = yf.download(ticker, period="1mo", auto_adjust=False)
        
        # Check if the DataFrame is empty before proceeding
        if df.empty:
            print(f"⚠️ No data for {ticker}. Skipping.\n")
            continue

        # Reset index to turn 'Date' from an index into a column
        df.reset_index(inplace=True)
        
        # Add the 'Symbol' column and rename other columns for database insertion
        df["Symbol"] = symbol
        df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adj_close',
            'Volume': 'volume',
        }, inplace=True)
        
        # Ensure 'date' column is in the correct format and drop any rows with missing dates
        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.dropna(subset=['date'])

        # ✅ Insert to MySQL
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            try:
                # Use .get() to safely access columns, with a default of 0 for missing data
                data_to_insert = {
                    'symbol': row['Symbol'],
                    'date': row['date'],
                    'open': float(row.get('open', 0)),
                    'high': float(row.get('high', 0)),
                    'low': float(row.get('low', 0)),
                    'close': float(row.get('close', 0)),
                    'adj_close': float(row.get('adj_close', 0)),
                    'volume': int(row.get('volume', 0)),
                }

                cursor.execute("""
                    INSERT IGNORE INTO companies_data
                    (symbol, date, open, high, low, close, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data_to_insert['symbol'],
                    data_to_insert['date'],
                    data_to_insert['open'],
                    data_to_insert['high'],
                    data_to_insert['low'],
                    data_to_insert['close'],
                    data_to_insert['adj_close'],
                    data_to_insert['volume'],
                ))

            except Exception as row_err:
                print(f"   ❌ Row error for {symbol}: {row_err}")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Data inserted for {symbol}\n")
        
    except KeyboardInterrupt:
        # Gracefully exit on user interruption
        print("\n\nUser interrupted the process. Exiting...")
        exit()
    except Exception as e:
        # Catch any other unexpected errors
        print(f"❌ Failed to process {ticker}: {e}\n")

print("🎯 All companies processed.")