import os
import pandas as pd
import yfinance as yf

# Folder to save files
save_folder = "NSE_Companies_Historical_Data"
os.makedirs(save_folder, exist_ok=True)

# 🔹 Sample symbols (later replace with your DB list)
symbols = ['3MINDIA', '21STCENMGM', '360ONE', '3IINFOLTD', 'HDFCBANK']  
symbols = [sym.strip().upper() + ".NS" for sym in symbols]  # Clean + append .NS

print(f"📦 Total symbols to process: {len(symbols)}\n")

for idx, symbol in enumerate(symbols, 1):
    print(f"[{idx}/{len(symbols)}] 📈 Fetching {symbol} ...")

    try:
        df = yf.download(symbol, period="1mo", interval="1d", auto_adjust=False)

        if df.empty:
            print(f"⚠️ No data for {symbol}. Skipping.\n")
            continue

        # Reset index (move Date from index to column)
        df.reset_index(inplace=True)
        df["Symbol"] = symbol.replace(".NS", "")

        # ✅ Flatten MultiIndex columns if any
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join([str(c) for c in col if c]) for col in df.columns.values]

        # ✅ Format Date column
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.date

        # ✅ Save to Excel
        file_name = f"{symbol.replace('.NS', '')}.xlsx"
        file_path = os.path.join(save_folder, file_name)
        df.to_excel(file_path, index=False)
        print(f"✅ Saved data to {file_path}\n")

    except Exception as e:
        print(f"❌ Failed for {symbol}: {e}\n")
