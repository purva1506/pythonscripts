import pymysql
import pandas as pd
from datetime import datetime

def convert_date(date_str):
    """Convert '06-OCT-2008' to '2008-10-06'."""
    try:
        return datetime.strptime(date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception:
        return None

try:
    print("📥 Downloading latest NSE company data...")
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    df = pd.read_csv(url)

    # Rename and clean columns
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Save locally
    df.to_csv("NSE_All_Listed_Companies.csv", index=False)
    print("✅ Saved as 'NSE_All_Listed_Companies.csv'")

    # Convert date
    if "date_of_listing" in df.columns:
        df["date_of_listing"] = df["date_of_listing"].apply(convert_date)
    else:
        raise KeyError("date_of_listing column not found!")

    print("🚀 Connecting to MySQL...")
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        port=3306,
        connect_timeout=5
    )
    print("✅ Connected!")

    cursor = connection.cursor()

    # Create database if not exists
    print("🛠️ Checking/Creating database...")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_market1")
    cursor.execute("SHOW DATABASES")
    dbs = [row[0] for row in cursor.fetchall()]
    if "stock_market1" in dbs:
        print("ℹ️ Database 'stock_market1' already exists.")
    else:
        print("✅ Database created.")

    # Use the database
    cursor.execute("USE stock_market1")

    # Create table if not exists
    print("🛠️ Checking/Creating table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listed_companies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50) UNIQUE,
            company_name VARCHAR(255),
            series VARCHAR(20),
            date_of_listing DATE,
            paid_up_value INT,
            market_lot INT,
            isin VARCHAR(50),
            face_value INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ Table 'listed_companies' is ready.")

    # Insert data
    print("📝 Syncing companies with database...")
    inserted, skipped = 0, 0
    for _, row in df.iterrows():
        symbol = row["symbol"]
        cursor.execute("SELECT COUNT(*) FROM listed_companies WHERE symbol = %s", (symbol,))
        exists = cursor.fetchone()[0]
        if exists == 0:
            try:
                cursor.execute("""
                    INSERT INTO listed_companies
                    (symbol, company_name, series, date_of_listing, paid_up_value, market_lot, isin, face_value)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    row["symbol"],
                    row["name_of_company"],
                    row["series"],
                    row["date_of_listing"],
                    int(row["paid_up_value"]),
                    int(row["market_lot"]),
                    row["isin_number"],
                    int(row["face_value"])
                ))
                inserted += 1
            except Exception as e:
                print(f"❌ Insert failed for {symbol}: {e}")
        else:
            skipped += 1

    print(f"✅ Inserted: {inserted}, Skipped (already exists): {skipped}")
    print("💾 Committing changes...")
    connection.commit()

    # Verify
    cursor.execute("SELECT COUNT(*) FROM listed_companies")
    total = cursor.fetchone()[0]
    print(f"📊 Total companies in table: {total}")

except pymysql.MySQLError as e:
    print("❌ pymysql Error:", e)

except Exception as ex:
    print("❗ Unexpected Error:", ex)

finally:
    if 'connection' in locals() and connection:
        cursor.close()
        connection.close()
        print("✅ Connection closed.")

print("✅ Script completed.")
