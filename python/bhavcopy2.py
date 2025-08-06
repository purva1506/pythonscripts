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
start_date = datetime.strptime("2009-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-08-03", "%Y-%m-%d")
URL_TEMPLATE = "https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{date}.zip"
OUTPUT_DIR = "nse_bhavcopy_output"

# ✅ MySQL configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_NAME = 'bhavcopy_db'
DB_PORT = 3306

# ✅ Create database if not exists
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
conn.commit()
cursor.close()
conn.close()

# ✅ Connect to DB using SQLAlchemy
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

HEADERS = {
    'accept': '/',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ Column cleaner to remove spaces, parentheses, slashes, etc.
def clean_column_name(col):
    return (
        col.strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
    )

# ✅ Process ZIP for a single date
def process_zip_for_date(date_obj):
    date_str = date_obj.strftime("%d%m%y")
    zip_url = URL_TEMPLATE.format(date=date_str)
    print(f"\n📥 Downloading: {zip_url}")

    try:
        response = requests.get(zip_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed to fetch {zip_url} (status: {response.status_code})")
            return

        zip_bytes = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_bytes) as z:
            file_names = z.namelist()
            print(f"📦 Files found in ZIP for {date_str}: {file_names}")

            for file_name in file_names:
                if "MCAP" in file_name.upper() and file_name.endswith('.csv'):
                    print(f"✅ Found MCAP file: {file_name}")
                    try:
                        with z.open(file_name) as csv_file:
                            df = pd.read_csv(csv_file, on_bad_lines='skip')
                            df.columns = [clean_column_name(c) for c in df.columns]
                            df['source_date'] = date_obj.date()
                            df['status'] = 'OK'

                            df.to_sql(name='mcap', con=engine, if_exists='append', index=False)
                            print(f"✅ Inserted MCAP data for {date_str} into `mcap` table.")
                    except Exception as e:
                        print(f"❌ Error reading {file_name}: {e}")
    except Exception as e:
        print(f"⚠ Exception fetching {date_str}: {e}")

# --- Date Range ---
date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# --- Multithreaded processing ---
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_zip_for_date, date) for date in date_list]
    for future in as_completed(futures):
        future.result()

print("\n✅ All MCAP files processed and inserted into MySQL.")
