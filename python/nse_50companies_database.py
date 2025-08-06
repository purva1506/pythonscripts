import yfinance as yf
import pandas as pd
import os
import pymysql

# ✅ MySQL Configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Patil@2000',
}

# ✅ Ensure DB and Table Exists
def ensure_database_and_table():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_market")
    conn.select_db("stock_market")
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
db_config['database'] = 'stock_market'
output_folder = "Nifty50_Companies_Data"
os.makedirs(output_folder, exist_ok=True)

# ✅ Fetch ticker symbols from listed_companies table
def get_company_symbols():
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("USE stock_market")
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
        df = yf.download(ticker, period="5y", auto_adjust=False)
        if df.empty:
            print(f"⚠️ No data for {ticker}. Skipping.\n")
            continue

        file_path = os.path.join(output_folder, f"{symbol}.xlsx")
        df.to_excel(file_path)

        # ✅ Read from Excel and clean
        df_excel = pd.read_excel(file_path)
        df_excel = df_excel.drop([1, 2], errors='ignore')  # Drop first 2 rows if present
        df_excel.reset_index(drop=True, inplace=True)

        first_col = df_excel.columns[0]
        df_excel.rename(columns={first_col: "Date"}, inplace=True)
        df_excel["Symbol"] = ticker

        # ✅ Normalize columns
        colmap = {c.lower(): c for c in df_excel.columns}
        needed = {
            'date': colmap.get('date'),
            'adj_close': colmap.get('adj close'),
            'close': colmap.get('close'),
            'high': colmap.get('high'),
            'low': colmap.get('low'),
            'open': colmap.get('open'),
            'volume': colmap.get('volume')
        }

        for key in needed:
            if needed[key] not in df_excel.columns:
                df_excel[key] = 0
            else:
                df_excel.rename(columns={needed[key]: key}, inplace=True)

        # ✅ Insert to MySQL
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        for _, row in df_excel.iterrows():
            try:
                date_val = pd.to_datetime(row['date'], errors='coerce')
                if pd.isna(date_val):
                    continue  # Skip bad rows

                cursor.execute("""
                    INSERT IGNORE INTO companies_data
                    (symbol, date, open, high, low, close, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    symbol,
                    date_val.date(),
                    float(row.get('open', 0)),
                    float(row.get('high', 0)),
                    float(row.get('low', 0)),
                    float(row.get('close', 0)),
                    float(row.get('adj_close', 0)),
                    int(row.get('volume', 0)),
                ))

            except Exception as row_err:
                print(f"   ❌ Row error: {row_err}")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Data inserted for {symbol}")
        os.remove(file_path)
        print(f"🧹 Deleted file: {file_path}\n")

    except Exception as e:
        print(f"❌ Failed to process {ticker}: {e}\n")

print("🎯 All companies processed.")
