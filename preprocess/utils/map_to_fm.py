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
STAGING_PREFIX = 'data/hcfa_json/valid/mapped/staging/'

DEBUG = False  # Set True for detailed row-by-row matching logs

def normalize_text(text):
    if not text:
        return ""
    text = text.strip().upper()
    if "," in text:
        last, first_middle = text.split(",", 1)
        parts = [last.strip()] + first_middle.strip().split()
    else:
        parts = text.split()
        if len(parts) > 1:
            parts = [parts[-1]] + parts[:-1]
    return "".join([char for char in " ".join(parts) if char.isalnum()])


def parse_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    if isinstance(date_str, datetime):
        return date_str
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0]
    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    if str(date_str).strip().lower() not in ["none", ""]:
        print(f"⚠️ Could not parse date: {date_str}")
    return None



def date_diff_days(date1, date2):
    if date1 is None or date2 is None:
        return float('inf')
    return abs((date1 - date2).days)

DB_PATH = Path(__file__).resolve().parents[2] / "filemaker.db"

def load_orders_to_dataframe():
    print("Loading data from SQLite...")
    conn = sqlite3.connect(DB_PATH)

    orders_df = pd.read_sql_query("""
        SELECT Order_ID, FileMaker_Record_Number, Patient_Last_Name,
               Patient_First_Name, PatientName
        FROM orders
    """, conn)

    line_items_df = pd.read_sql_query("""
        SELECT Order_ID, DOS, CPT
        FROM line_items
    """, conn)

    # Safer DOS parsing
    def safe_parse(x):
        if pd.isna(x) or str(x).strip().lower() in ["", "none"]:
            return None
        return parse_date(str(x))


    line_items_df['DOS'] = line_items_df['DOS'].apply(safe_parse)


    # Group all DOS values by Order_ID
    dos_groups = (
        line_items_df[line_items_df['DOS'].notna()]
        .groupby('Order_ID')['DOS']
        .agg(list)
        .reset_index()
    )
    dos_groups.columns = ['Order_ID', 'DOS_List']

    # Merge into orders
    df = pd.merge(orders_df, dos_groups, on='Order_ID', how='left')
    df['DOS_List'] = df['DOS_List'].apply(lambda x: x if isinstance(x, list) else [])

    for col in ['Patient_Last_Name', 'Patient_First_Name', 'PatientName']:
        df[col] = df[col].apply(normalize_text)

    print(f"Loaded {len(df)} records from filemaker.db")
    return df, line_items_df

def get_cpts_for_order(order_id, line_items_df):
    cpts = line_items_df[line_items_df['Order_ID'] == order_id]['CPT'].dropna().unique()
    return {str(cpt).strip() for cpt in cpts}

def process_mapping_s3():
    df_orders, df_line_items = load_orders_to_dataframe()

    print("Listing files in S3...")
    all_keys = list_objects(VALID_PREFIX)
    json_keys = [k for k in all_keys if k.lower().endswith('.json')
                 and k.count('/') == 3
                 and not any(x in k for x in ['mapped', 'unmapped', 'staging'])]

    print(f"Found {len(json_keys)} files to process")
    processed_files = 0

    for key in json_keys:
        filename = os.path.basename(key)
        print(f"\nProcessing: {filename}")

        try:
            local_json = os.path.join(tempfile.gettempdir(), filename)
            download(key, local_json)

            with open(local_json, 'r') as f:
                json_data = json.load(f)

            original_name = json_data.get("patient_info", {}).get("patient_name", "")
            print(f"🧾 Original name from JSON: '{original_name}'")
            json_name = normalize_text(original_name)
            print(f"🔍 JSON normalized name: {json_name}")

            dos_list = []
            for entry in json_data.get("service_lines", []):
                dos = parse_date(entry.get("date_of_service", ""))
                if dos:
                    dos_list.append(dos)

            if not json_name or not dos_list:
                print(f"❌ Missing name or DOS: {filename}")
                move(key, f"{UNMAPPED_PREFIX}{filename}")
                continue

            print(f"🔍 DOS from JSON: {dos_list}")
            json_cpts = {line.get("cpt_code", "").strip()
                         for line in json_data.get("service_lines", [])
                         if line.get("cpt_code")}

            candidate_matches = []
            for _, row in df_orders.iterrows():
                db_name = row['PatientName']
                db_dos_list = row.get('DOS_List', [])

                token_sort_score = fuzz.token_sort_ratio(json_name, db_name)
                token_set_score = fuzz.token_set_ratio(json_name, db_name)
                composite_score = (token_sort_score + token_set_score) / 2

                if DEBUG:
                    print("----------")
                    print(f"🧾 Comparing to DB name: {db_name}")
                    print(f"📅 DB DOS List: {db_dos_list}")
                    print(f"🎯 JSON normalized name: {json_name}")
                    print(f"📅 JSON DOS List: {dos_list}")
                    print(f"🤝 Name Score: {composite_score}")

                if composite_score >= 90 and len(db_dos_list) > 0:
                    for json_dos in dos_list:
                        for db_dos in db_dos_list:
                            if date_diff_days(json_dos, db_dos) <= 14:
                                candidate_matches.append({
                                    'composite_score': composite_score,
                                    'token_sort_score': token_sort_score,
                                    'token_set_score': token_set_score,
                                    'row': row,
                                    'dos_diff': date_diff_days(json_dos, db_dos)
                                })
                                break
                        else:
                            continue
                        break

            best_match = None
            if len(candidate_matches) == 1:
                best_match = candidate_matches[0]
            elif len(candidate_matches) > 1:
                primary_cpt = None
                max_charge = 0
                for line in json_data.get("service_lines", []):
                    cpt = line.get("cpt_code", "").strip()
                    try:
                        charge = float(line.get("charge_amount", "0").replace("$", "").replace(",", ""))
                        if charge > max_charge:
                            max_charge = charge
                            primary_cpt = cpt
                    except:
                        continue

                ranked_matches = []
                for match in candidate_matches:
                    order_id = match['row']['Order_ID']
                    db_cpts = get_cpts_for_order(order_id, df_line_items)

                    cpt_match_count = len(json_cpts & db_cpts)
                    primary_cpt_match = 2 if primary_cpt in db_cpts else 0
                    match_percentage = cpt_match_count / max(len(json_cpts), 1) * 100
                    cpt_score = primary_cpt_match + cpt_match_count + (match_percentage / 100)
                    proximity_score = 14 - match['dos_diff']

                    ranked_matches.append((
                        cpt_score,
                        proximity_score,
                        match['composite_score'],
                        match
                    ))

                ranked_matches.sort(reverse=True)
                best_match = ranked_matches[0][3]

                print(f"Multiple matches found - selected best match using CPT priority:")
                print(f"  Selected: Order {best_match['row']['Order_ID']}")
                print(f"  CPT Score: {ranked_matches[0][0]:.2f}, DOS Proximity: {ranked_matches[0][1]}")
                print(f"  Name Score: {best_match['composite_score']:.2f}")

            if best_match:
                json_data["mapping_info"] = {
                    "order_id": str(best_match['row']['Order_ID']),
                    "filemaker_number": str(best_match['row']['FileMaker_Record_Number']),
                    "mapping_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                with open(local_json, 'w') as f:
                    json.dump(json_data, f, indent=4)

                new_key = f"{MAPPED_PREFIX}{filename}"
                upload(local_json, new_key)
                move(key, new_key)
                print(f"✔ Mapped: {filename} -> Order {best_match['row']['Order_ID']}")
            else:
                print(f"❌ No match found: {filename}")
                move(key, f"{UNMAPPED_PREFIX}{filename}")

            os.remove(local_json)
            processed_files += 1

        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            continue

    print(f"\nProcessed {processed_files} files")

if __name__ == "__main__":
    process_mapping_s3()
