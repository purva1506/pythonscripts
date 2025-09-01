import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import pymysql
from sqlalchemy import create_engine

# --- Config ---
start_date = datetime.strptime("2025-08-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-08-31", "%Y-%m-%d")
URL_TEMPLATE = "https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{date}.zip"
OUTPUT_DIR = "nse_bhavcopy_output"

# ✅ MySQL connection configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_NAME = 'bhavcopy1'
DB_PORT = 3306

# ✅ Step 1: Connect to MySQL Server (without DB)
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
conn.commit()
cursor.close()
conn.close()

# ✅ Step 3: Connect to the specific database using SQLAlchemy
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

# --- Output folder ---
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- Process one date's ZIP ---
def process_zip_for_date(date_obj):
    date_str = date_obj.strftime("%d%m%y")  # PRddmmyy
    url = URL_TEMPLATE.format(date=date_str)
    print(f"📥 Downloading: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            zip_bytes = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_bytes) as z:
                for file_name in z.namelist():
                    if file_name.endswith('.csv'):
                        # ✅ Extract base name (without extension & digits)
                        base = os.path.splitext(os.path.basename(file_name))[0]
                        base = re.sub(r"\d+", "", base).lower()  # remove numbers

                        try:
                            with z.open(file_name) as csv_file:
                                df = pd.read_csv(csv_file, on_bad_lines='skip')
                                df['source_date'] = date_obj.date()
                                df['status'] = "OK"

                                # ✅ Push data to MySQL
                                df.to_sql(name=base, con=engine, if_exists='append', index=False)

                                print(f"✅ Inserted into table `{base}` ({len(df)} rows)")

                        except Exception as e:
                            print(f"⚠ Error reading {file_name} on {date_str}: {e}")
        else:
            print(f"❌ No data for {date_obj.date()} (status: {response.status_code})")
    except Exception as e:
        print(f"⚠ Exception for {date_str}: {e} — skipped")


# --- Date list ---
date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# --- Multithreaded run ---
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_zip_for_date, date) for date in date_list]
    for future in as_completed(futures):
        future.result()

print(f"\n✅ All available files processed and saved in: {OUTPUT_DIR}")
