#!/usr/bin/env python3
"""
map_to_fm.py

Maps HCFA JSON files to FileMaker records using fuzzy matching.
Sources from S3 valid folder and moves files to mapped or unmapped folders.
Uses local SQLite database instead of Parquet/S3.
"""

import os
import sys
import json
import tempfile
import sqlite3
from datetime import datetime
from pathlib import Path
import pandas as pd
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

# Add the project root to Python path
project_root = str(Path(__file__).resolve().parents[2])
sys.path.append(project_root)

# Import S3 helper functions
from utils.s3_utils import list_objects, download, upload, move

# Load environment variables
load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')

# S3 paths
VALID_PREFIX = 'data/hcfa_json/valid/'
MAPPED_PREFIX = 'data/hcfa_json/valid/mapped/'
UNMAPPED_PREFIX = 'data/hcfa_json/valid/unmapped/'

DB_PATH = Path(__file__).resolve().parents[2] / "filemaker.db"

def normalize_text(text):
    if not text:
        return ""
    return "".join(c for c in text.upper().strip() if c.isalnum())

def safe_get_dos_list(row):
    val = row.get("DOS_List", [])
    if isinstance(val, list):
        return val
    if pd.isna(val):
        return []
    if isinstance(val, pd.Series):
        return val.tolist()
    return []



def parse_date(date_str):
    if not date_str or pd.isna(date_str):
        return None

    if isinstance(date_str, datetime):
        return date_str

    # Convert common None-like values to real None
    if str(date_str).strip().lower() in ['none', '', 'nan']:
        return None

    # Handle ranges like "03/31/25 - 03/31/25"
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0]

    date_str = date_str.strip()

    # Try a variety of common formats
    date_formats = [
        "%Y-%m-%d",     # 2025-03-06
        "%m/%d/%Y",     # 03/06/2025
        "%m/%d/%y",     # 03/06/25
        "%Y/%m/%d",     # 2025/03/06
        "%Y%m%d",       # 20250306
        "%m-%d-%Y",     # 03-06-2025
        "%Y.%m.%d",     # 2025.03.06
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Final fallback (won't crash)
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except:
        return None


def date_diff_days(d1, d2):
    if not d1 or not d2:
        return float('inf')
    if hasattr(d1, 'to_pydatetime'):
        d1 = d1.to_pydatetime()
    if hasattr(d2, 'to_pydatetime'):
        d2 = d2.to_pydatetime()
    return abs((d1 - d2).days)

def load_orders_to_dataframe():
    print("🔄 Loading orders from SQLite...")
    conn = sqlite3.connect(DB_PATH)

    orders_df = pd.read_sql_query("""
        SELECT Order_ID, FileMaker_Record_Number, Patient_First_Name, Patient_Last_Name
        FROM orders
    """, conn)

    line_items_df = pd.read_sql_query("""
        SELECT Order_ID, DOS, CPT
        FROM line_items
    """, conn)

    # 🔧 Normalize Order_IDs (strip + uppercase for safety)
    orders_df['Order_ID'] = orders_df['Order_ID'].str.strip().str.upper()
    line_items_df['Order_ID'] = line_items_df['Order_ID'].str.strip().str.upper()

    # Parse DOS
    line_items_df['DOS'] = line_items_df['DOS'].apply(parse_date)

    # Group DOS by Order_ID
    dos_grouped = (
        line_items_df[line_items_df['DOS'].notna()]
        .groupby('Order_ID')['DOS']
        .agg(list)
        .reset_index()
    )
    dos_grouped.columns = ['Order_ID', 'DOS_List']

    # Merge and clean
    df = pd.merge(orders_df, dos_grouped, on='Order_ID', how='left')
    df['DOS_List'] = df['DOS_List'].apply(lambda x: [i for i in x if isinstance(i, datetime)] if isinstance(x, list) else [])

    # Normalize patient name
    df['NormalizedPatientName'] = df.apply(
        lambda row: normalize_text(f"{row['Patient_Last_Name']} {row['Patient_First_Name']}"), axis=1
    )


    print(f"✅ Loaded {len(df)} records.")

    # 🔍 Optional: Show missing DOS to confirm fix
    missing_dos = df[df['DOS_List'].apply(len) == 0]
    print(f"⚠️ Orders missing DOS: {len(missing_dos)} (should drop after fix)")
    return df, line_items_df


def get_cpts_for_order(order_id, df_line_items):
    try:
        return set(
            str(cpt).strip()
            for cpt in df_line_items[df_line_items['Order_ID'] == order_id]['CPT'].dropna().unique()
        )
    except:
        return set()

def process_mapping_s3():
    df_orders, df_line_items = load_orders_to_dataframe()
    json_keys = [k for k in list_objects(VALID_PREFIX)
                 if k.lower().endswith('.json') and k.count('/') == 3
                 and not any(x in k for x in ['mapped', 'unmapped', 'staging'])]

    print(f"📂 Found {len(json_keys)} JSON files to process.")
    processed = 0

    for key in json_keys:
        filename = os.path.basename(key)
        print(f"\n📄 Processing: {filename}")
        try:
            local_json = os.path.join(tempfile.gettempdir(), filename)
            download(key, local_json)

            with open(local_json, 'r') as f:
                json_data = json.load(f)

            # Get and normalize JSON name
            json_name_raw = json_data.get("patient_info", {}).get("patient_name", "")

            json_name = normalize_text(json_name_raw)
            dos_list = [parse_date(entry.get("date_of_service")) for entry in json_data.get("service_lines", [])]
            dos_list = [d for d in dos_list if isinstance(d, datetime)]

            json_cpts = {line.get("cpt_code", "").strip() for line in json_data.get("service_lines", []) if line.get("cpt_code")}

            # ✅ NEW: Print raw and normalized values for visibility
            print(f"🔎 Raw Patient Name: {json_name_raw}")
            print(f"🔎 Normalized Patient Name: {json_name}")
            print(f"📅 Parsed DOS List: {dos_list}")
            print(f"💉 CPTs from JSON: {json_cpts}")

            # ✅ NEW: Show sample DB records for visual cross-check
            print(f"\n🔬 Top 5 name matches for: {json_name}")
            scored = []
            for _, row in df_orders.iterrows():
                db_name = row.get("NormalizedPatientName", "")
                if not isinstance(db_name, str):
                    continue
                score = fuzz.token_sort_ratio(json_name, db_name)
                if score >= 70:  # lower threshold just for visibility
                    scored.append((score, db_name, row))

            # Sort by highest score
            scored = sorted(scored, key=lambda x: x[0], reverse=True)[:5]
            for score, name, row in scored:
                print(f"   🔍 {name} (score={score}) | DOS: {safe_get_dos_list(row)[:3]}")



            # Safely parse DOS list
            raw_service_lines = json_data.get("service_lines", [])
            dos_list = [parse_date(line.get("date_of_service")) for line in raw_service_lines]
            dos_list = [d for d in dos_list if isinstance(d, datetime)]
            print(f"📅 JSON DOS list: {dos_list[:3]}")  # Safe now

            # Show sample DB rows for comparison
            print("🧾 Sample normalized DB names and DOS:")
            for i in range(5):
                row = df_orders.iloc[i]
                print(f"  → {row['NormalizedPatientName']} | DOS: {row.get('DOS_List', [])[:3]}")


            candidates = []

            for _, row in df_orders.iterrows():
                db_name = row.get("NormalizedPatientName", "")
                if not isinstance(db_name, str):
                    continue

                score = fuzz.token_sort_ratio(json_name, db_name)
                if score < 90:
                    continue

                for jd in dos_list:
                    for dd in safe_get_dos_list(row):
                        if date_diff_days(jd, dd) <= 14:
                            candidates.append((score, row, date_diff_days(jd, dd)))
                            break
                    else:
                        continue
                    break

            best = None
            if len(candidates) == 1:
                best = candidates[0][1]
            elif len(candidates) > 1:
                primary_cpt = None
                max_charge = 0
                for line in json_data.get("service_lines", []):
                    try:
                        charge = float(line.get("charge_amount", "0").replace("$", "").replace(",", ""))
                        if charge > max_charge:
                            max_charge = charge
                            primary_cpt = line.get("cpt_code", "").strip()
                    except:
                        continue

                def rank(row, proximity):
                    order_id = row['Order_ID']
                    db_cpts = get_cpts_for_order(order_id, df_line_items)
                    match_count = len(json_cpts & db_cpts)
                    return (2 if primary_cpt in db_cpts else 0) + match_count - proximity

                ranked = sorted(candidates, key=lambda c: rank(c[1], c[2]), reverse=True)
                best = ranked[0][1] if ranked else None

            if best is not None:
                json_data["mapping_info"] = {
                    "order_id": str(best['Order_ID']),
                    "filemaker_number": str(best['FileMaker_Record_Number']),
                    "mapping_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                with open(local_json, 'w') as f:
                    json.dump(json_data, f, indent=4)

                upload(local_json, MAPPED_PREFIX + filename)
                # Delete original so we don't accidentally move over the updated file
                from utils.s3_utils import delete  # you may need to add this
                delete(key)
                print(f"✅ Mapped: {filename} → Order {best['Order_ID']}")
            else:
                move(key, UNMAPPED_PREFIX + filename)
                print("❌ No match found.")

            os.remove(local_json)
            processed += 1

        except Exception as e:
            print(f"💥 Error processing {filename}: {str(e)}")
            move(key, UNMAPPED_PREFIX + filename)
            continue

    print(f"\n✅ Done. Processed {processed} files.")
    print(f"🔍 Normalized JSON name: {json_name}")
    print(f"🔍 Sample DB name: {df_orders['NormalizedPatientName'].iloc[0]}")


if __name__ == "__main__":
    process_mapping_s3()

