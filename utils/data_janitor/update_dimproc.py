import pandas as pd
import sqlite3
import os

# === CONFIGURATION ===
CSV_PATH = 'C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review\filemaker.db'              # <-- Replace with your actual CSV filename
DB_PATH = r'C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review\filemaker.db'               # <-- Adjust if needed
TABLE_NAME = 'dim_proc'

# === STEP 1: Load CSV ===
df = pd.read_csv(CSV_PATH)

# Clean column names (optional but helpful)
df.columns = [col.strip().lower() for col in df.columns]

# Expected columns: proc_cd, proc_desc, category, subcategory, allow_soft_match (optional: proc_category)
required_cols = {'proc_cd', 'proc_desc', 'category', 'subcategory'}
if not required_cols.issubset(set(df.columns)):
    raise ValueError(f"CSV is missing required columns: {required_cols - set(df.columns)}")

# Fill optional fields
if 'allow_soft_match' not in df.columns:
    df['allow_soft_match'] = 1
if 'proc_category' not in df.columns:
    df['proc_category'] = None

# === STEP 2: Connect to SQLite and create/replace table ===
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Drop table if exists
cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

# Create table
cursor.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proc_cd TEXT NOT NULL,
        proc_desc TEXT,
        category TEXT,
        subcategory TEXT,
        allow_soft_match INTEGER DEFAULT 1,
        proc_category TEXT
    )
""")

# Insert data
df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)

conn.commit()
conn.close()

print(f"✅ Loaded {len(df)} rows into '{TABLE_NAME}' table in {DB_PATH}")
