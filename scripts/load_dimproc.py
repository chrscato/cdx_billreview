import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

# Paths
PROC_DB_PATH = os.getenv("PROC_DB_PATH", r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review\filemaker.db")
CSV_PATH = os.getenv("PROC_CSV_PATH", r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review\scripts\dim_proc.csv")  # Optional .env override

def load_dim_proc():
    df = pd.read_csv(CSV_PATH)

    # Optional cleaning
    df.columns = [c.strip().lower() for c in df.columns]

    conn = sqlite3.connect(PROC_DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS dim_proc")

    cursor.execute("""
        CREATE TABLE dim_proc (
            id INTEGER PRIMARY KEY,
            proc_cd TEXT,
            modifier TEXT,
            proc_desc TEXT,
            category TEXT,
            subcategory TEXT
        )
    """)

    df.to_sql("dim_proc", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    print(f"✅ dim_proc table loaded with {len(df)} records.")

if __name__ == "__main__":
    load_dim_proc()
