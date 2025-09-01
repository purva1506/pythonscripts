import os
import pandas as pd
import yfinance as yf
import pymysql
from datetime import datetime

# 🔹 MySQL Database Config
db_config = {
    "host": "localhost",
    "user": "root",       # change if needed
    "password": "Patil@2000",
    "database": "stock_market"
}

# 🔹 Create folder for temporary Excel files
save_folder = "NSE_Companies_Historical_Data"
os.makedirs(save_folder, exist_ok=True)

# 🔹 Connect to DB and fetch listed companies
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

cursor.execute("SELECT symbol FROM listed_companies")
symbols = [row[0].upper() + ".NS" for row in cursor.fetchall()]

cursor.close()
conn.close()

print(f"🚀 Starting for {len(symbols)} companies from DB...\n")

for idx, symbol in enumerate(symbols, 1):
    clean_symbol = symbol.replace(".NS", "")
    print(f"[{idx}/{len(symbols)}] 📈 Fetching {symbol} ...")

    try:
        # Download 1 month daily data (change to period="5y" if needed)
        df = yf.download(symbol, period="1mo", interval="1d", auto_adjust=False, actions=True)

        if df.empty:
            print(f"⚠️ No data for {symbol}. Skipping.\n")
            continue

        # Reset index to make Date available
        df.reset_index(inplace=True)

        # ✅ Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join([str(c).strip() for c in tup if c]) for tup in df.columns.values]

        # Rename columns to match DB schema
        rename_map = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits"
        }
        df.rename(columns={c: rename_map.get(c.split("_")[0], c) for c in df.columns}, inplace=True)

        # Add Symbol column
        df["symbol"] = clean_symbol

        # ✅ Save backup Excel file temporarily
        file_name = f"{clean_symbol}.xlsx"
        file_path = os.path.join(save_folder, file_name)
        df.to_excel(file_path, index=False)
        print(f"   💾 Saved Excel backup: {file_path}")

        # ✅ Insert to MySQL (table: all_companies_data)
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Ensure all_companies_data table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS all_companies_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20),
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                dividends FLOAT,
                stock_splits FLOAT,
                UNIQUE KEY unique_record (symbol, date)
            )
        """)

        # Ensure failed_symbols table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_symbols (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20),
                error_message TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        inserted_rows = 0
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT IGNORE INTO all_companies_data
                    (symbol, date, open, high, low, close, volume, dividends, stock_splits)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["symbol"],
                    pd.to_datetime(row["date"]).date(),
                    float(row.get("open", 0) or 0),
                    float(row.get("high", 0) or 0),
                    float(row.get("low", 0) or 0),
                    float(row.get("close", 0) or 0),
                    int(row.get("volume", 0) or 0),
                    float(row.get("dividends", 0) or 0),
                    float(row.get("stock_splits", 0) or 0)
                ))
                inserted_rows += 1
            except Exception as row_err:
                print(f"   ❌ Row insert error for {symbol}: {row_err}")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Inserted {inserted_rows} rows for {symbol} into all_companies_data")

        # ✅ Delete backup Excel file after successful DB push
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"   🗑️ Deleted temporary file: {file_path}\n")

    except Exception as e:
        print(f"❌ Failed for {symbol}: {e}\n")

        # ✅ Log failure into failed_symbols table
        try:
            conn = pymysql.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS failed_symbols (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(20),
                    error_message TEXT,
                    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                INSERT INTO failed_symbols (symbol, error_message)
                VALUES (%s, %s)
            """, (clean_symbol, str(e)))
            conn.commit()
            cursor.close()
            
            conn.close()
            print(f"   ⚠️ Logged {symbol} into failed_symbols table\n")
        except Exception as log_err:
            print(f"   ❌ Failed logging {symbol}: {log_err}\n")
