import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import pymysql
from sqlalchemy import create_engine, text

# --- Config ---
start_date = datetime.strptime("2025-08-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-08-30", "%Y-%m-%d")
URL_TEMPLATE = "https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{date}.zip"
OUTPUT_DIR = "nse_bhavcopy_output"

# ✅ MySQL connection configuration
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "Patil@2000"     # ✅ Correct password
DB_NAME = "bhavcopy_db"
DB_PORT = 3306

# ✅ Step 1: Connect to MySQL Server (without DB)
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
conn.commit()
cursor.close()
conn.close()

# ✅ Step 2: Connect to the specific database using SQLAlchemy
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

# --- Output folder ---
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Expected tables ---
EXPECTED_TABLES = ["bc", "bh", "corpbond", "etf", "ffix", "gl", "hl", "ix", "mcap", "pd", "pr", "sme", "tt"]

# --- Ensure all expected tables exist ---
with engine.begin() as conn:
    for tbl in EXPECTED_TABLES:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {tbl} (id INT AUTO_INCREMENT PRIMARY KEY)"))

# --- Process one date's ZIP ---
def process_zip_for_date(date_obj):
    date_str = date_obj.strftime("%d%m%y")  # PRDDMMYY
    url = URL_TEMPLATE.format(date=date_str)
    print(f"📥 Downloading: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code == 200:
            zip_bytes = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_bytes) as z:
                for file_name in z.namelist():
                    if file_name.endswith('.csv'):
                        # Extract table name (drop date+extension)
                        base = re.sub(r"\d+\.csv$", "", file_name).lower()

                        if base in EXPECTED_TABLES:
                            try:
                                with z.open(file_name) as csv_file:
                                    df = pd.read_csv(csv_file, on_bad_lines='skip', encoding_errors="ignore")
                                    df['source_date'] = date_obj.date()
                                    df['status'] = "OK"

                                    # ✅ Insert into SQL
                                    df.to_sql(name=base, con=engine, if_exists='append', index=False)

                                    print(f"✅ Inserted {len(df)} rows → `{base}` ({date_obj.date()})")
                            except Exception as e:
                                print(f"⚠ Error reading {file_name} on {date_str}: {e}")
        else:
            print(f"❌ No data for {date_obj.date()} (status: {response.status_code})")
    except Exception as e:
        print(f"⚠ Exception for {date_str}: {e} — skipped")

# --- Generate date list ---
date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# --- Multithreaded run ---
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_zip_for_date, date) for date in date_list]
    for future in as_completed(futures):
        future.result()

print(f"\n✅ All available files processed and saved into MySQL `{DB_NAME}`.")
print(f"   Tables: {', '.join(EXPECTED_TABLES)}")