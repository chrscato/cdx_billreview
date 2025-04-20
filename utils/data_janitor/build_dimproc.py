import pandas as pd
import sqlite3

# === SETTINGS ===
EXCEL_PATH = r"C:\Users\ChristopherCato\Downloads\rvu25a (1)\PPRRVU25_JAN.xlsx"  # Update if needed
SHEET_NAME = 'PPRRVU25_V1223'
OUTPUT_CSV = 'categorized_cpt_codes.csv'
DB_PATH = 'filemaker.db'
TABLE_NAME = 'dim_procedure'

# === STEP 1: Load Excel ===
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
df = df[['HCPCS', 'DESCRIPTION']].copy()

# === STEP 2: Normalize and categorize ===
df['DESCRIPTION_LOWER'] = df['DESCRIPTION'].fillna("").str.lower()

def detect_modality(desc):
    if "mri" in desc:
        return "MRI"
    elif "x-ray" in desc or "xray" in desc:
        return "X-ray"
    elif "ct" in desc:
        return "CT"
    elif "emg" in desc:
        return "EMG"
    elif "ultrasound" in desc:
        return "Ultrasound"
    return "Other"

def detect_contrast(desc):
    if "without contrast" in desc or "wo contrast" in desc or "w/o contrast" in desc:
        return "No Contrast"
    elif "with contrast" in desc or "w/ contrast" in desc or "wo & w contrast" in desc:
        return "With Contrast"
    return "Unspecified"

df['category'] = df['DESCRIPTION_LOWER'].apply(detect_modality)
df['contrast'] = df['DESCRIPTION_LOWER'].apply(detect_contrast)

# Optional cleanup
df = df[['HCPCS', 'DESCRIPTION', 'category', 'contrast']]
df = df.rename(columns={'HCPCS': 'cpt_code', 'DESCRIPTION': 'description'})

# === STEP 3: Export to CSV ===
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Saved categorized CPT data to {OUTPUT_CSV}")

# # === STEP 4: (Optional) Load into SQLite DB ===
# load_to_db = True
# if load_to_db:
#     conn = sqlite3.connect(DB_PATH)
#     df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
#     conn.close()
#     print(f"✅ Loaded data into SQLite table: {TABLE_NAME}")
