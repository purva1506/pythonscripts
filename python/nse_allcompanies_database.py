import os
import pandas as pd
import yfinance as yf

# Folder to save files
save_folder = "NSE_Companies_Historical_Data"
os.makedirs(save_folder, exist_ok=True)

# 🔹 Sample symbols (you can later use your full symbol list from DB)
symbols = ['3MINDIA', '21STCENMGM ', '360ONE', '3IINFOLTD ', 'HDFCBANK']  # Replace with your DB list
symbols = [sym.upper() + ".NS" for sym in symbols]  # append .NS suffix

print(f"📦 Total symbols to process: {len(symbols)}\n")

for idx, symbol in enumerate(symbols, 1):
    print(f"[{idx}/{len(symbols)}] 📈 Fetching {symbol} ...")

    try:
        df = yf.download(symbol, period="5y", interval="1d", auto_adjust=False)

        if df.empty:
            print(f"⚠️ No data for {symbol}. Skipping.\n")
            continue

        df.reset_index(inplace=True)
        df["Symbol"] = symbol.replace(".NS", "")

        # ✅ Format Date column for export
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        # ✅ Save to Excel
        file_name = f"{symbol.replace('.NS', '')}.xlsx"
        file_path = os.path.join(save_folder, file_name)
        df.to_excel(file_path, index=False)
        print(f"✅ Saved data to {file_path}\n")

    except Exception as e:
        print(f"❌ Failed for {symbol}: {e}")
