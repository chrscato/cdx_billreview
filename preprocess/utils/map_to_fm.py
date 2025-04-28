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
    # Convert pandas Timestamp to datetime if needed
    if hasattr(date1, 'to_pydatetime'):
        date1 = date1.to_pydatetime()
    if hasattr(date2, 'to_pydatetime'):
        date2 = date2.to_pydatetime()
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
    try:
        print(f"\nDEBUG - Getting CPTs for order {order_id}")
        print(f"  line_items_df['Order_ID'] type: {type(line_items_df['Order_ID'])}")
        print(f"  order_id type: {type(order_id)}")
        mask = line_items_df['Order_ID'] == order_id
        print(f"  mask type: {type(mask)}")
        print(f"  mask shape: {mask.shape}")
        print(f"  mask sum: {mask.sum()}")
        cpts = line_items_df[mask]['CPT'].dropna().unique()
        print(f"  cpts type: {type(cpts)}")
        print(f"  cpts shape: {cpts.shape if hasattr(cpts, 'shape') else 'N/A'}")
        return {str(cpt).strip() for cpt in cpts}
    except Exception as e:
        print(f"Error in get_cpts_for_order for order_id {order_id}: {str(e)}")
        print(f"line_items_df type: {type(line_items_df)}")
        print(f"line_items_df columns: {line_items_df.columns}")
        print(f"line_items_df shape: {line_items_df.shape}")
        return set()

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

                # More explicit type checking and conversion
                if isinstance(db_dos_list, pd.Series):
                    db_dos_list = db_dos_list.tolist()
                
                if composite_score >= 90 and len(db_dos_list) > 0:
                    for json_dos in dos_list:
                        for db_dos in db_dos_list:
                            # Convert pandas Timestamp to datetime if needed
                            if hasattr(db_dos, 'to_pydatetime'):
                                db_dos = db_dos.to_pydatetime()
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
                    try:
                        print(f"\nDEBUG - Ranking match:")
                        print(f"  match type: {type(match)}")
                        print(f"  match['row'] type: {type(match['row'])}")
                        print(f"  match['row']['Order_ID'] type: {type(match['row']['Order_ID'])}")
                        
                        order_id = match['row']['Order_ID']
                        db_cpts = get_cpts_for_order(order_id, df_line_items)
                        
                        print(f"  json_cpts type: {type(json_cpts)}")
                        print(f"  db_cpts type: {type(db_cpts)}")
                        print(f"  primary_cpt type: {type(primary_cpt)}")
                        
                        # Debug each step of the ranking calculation
                        print("\nDEBUG - Calculating scores:")
                        cpt_match_count = len(json_cpts & db_cpts)
                        print(f"  cpt_match_count: {cpt_match_count}")
                        
                        primary_cpt_match = 2 if primary_cpt in db_cpts else 0
                        print(f"  primary_cpt_match: {primary_cpt_match}")
                        
                        match_percentage = cpt_match_count / max(len(json_cpts), 1) * 100
                        print(f"  match_percentage: {match_percentage}")
                        
                        cpt_score = primary_cpt_match + cpt_match_count + (match_percentage / 100)
                        print(f"  cpt_score: {cpt_score}")
                        
                        proximity_score = 14 - match['dos_diff']
                        print(f"  proximity_score: {proximity_score}")
                        
                        print(f"  composite_score: {match['composite_score']}")

                        ranked_matches.append((
                            cpt_score,
                            proximity_score,
                            match['composite_score'],
                            match
                        ))
                    except Exception as e:
                        print(f"  Error in ranking match: {str(e)}")
                        print(f"  match: {match}")
                        raise

                print("\nDEBUG - Sorting ranked matches")
                ranked_matches.sort(reverse=True)
                print(f"  Number of ranked matches: {len(ranked_matches)}")
                
                if ranked_matches:
                    best_match = ranked_matches[0][3]
                    print(f"  Best match Order_ID: {best_match['row']['Order_ID']}")
                else:
                    print("  No ranked matches found")
                    best_match = None

            if best_match:
                try:
                    print(f"\nDEBUG - Processing best match for {filename}")
                    print(f"  best_match type: {type(best_match)}")
                    print(f"  best_match['row'] type: {type(best_match['row'])}")
                    print(f"  best_match['row']['Order_ID'] type: {type(best_match['row']['Order_ID'])}")
                    
                    order_id = best_match['row']['Order_ID']
                    db_cpts = get_cpts_for_order(order_id, df_line_items)
                    
                    # Add mapping info to JSON
                    mapping_info = {
                        "order_id": str(order_id),
                        "filemaker_number": str(best_match['row']['FileMaker_Record_Number']),
                        "mapping_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    json_data["mapping_info"] = mapping_info
                    
                    # Write the updated JSON to local file
                    with open(local_json, 'w') as f:
                        json.dump(json_data, f, indent=4)
                    
                    # Print exactly what we're about to upload
                    print("\nDEBUG - File contents being uploaded to S3:")
                    print(json.dumps(json_data, indent=4))
                    
                    # Upload to mapped folder
                    new_key = f"{MAPPED_PREFIX}{filename}"
                    upload(local_json, new_key)
                    move(key, new_key)
                    print(f"✔ Mapped: {filename} -> Order {order_id}")
                except Exception as e:
                    print(f"\n❌ Error in best match processing for {filename}:")
                    print(f"  Error: {str(e)}")
                    print(f"  best_match: {best_match}")
                    raise
            else:
                # Print debug info only for failed mappings
                print(f"\n❌ Failed to map: {filename}")
                print(f"🧾 Original name from JSON: '{original_name}'")
                print(f"🔍 JSON normalized name: {json_name}")
                print(f"📅 JSON DOS List: {dos_list}")
                print("\nDEBUG - Top 5 closest matches:")
                try:
                    top_matches = df_orders.nlargest(5, 'PatientName', key=lambda x: fuzz.token_sort_ratio(json_name, x))
                    for _, row in top_matches.iterrows():
                        print(f"  Order_ID: {row['Order_ID']}")
                        print(f"  PatientName: {row['PatientName']}")
                        print(f"  DOS_List: {row.get('DOS_List', [])}")
                        print(f"  Score: {fuzz.token_sort_ratio(json_name, row['PatientName'])}")
                        print("  ---")
                except Exception as e:
                    print(f"  Error getting top matches: {str(e)}")
                    print(f"  df_orders type: {type(df_orders)}")
                    print(f"  df_orders columns: {df_orders.columns}")
                    print(f"  df_orders shape: {df_orders.shape}")
                move(key, f"{UNMAPPED_PREFIX}{filename}")

            os.remove(local_json)
            processed_files += 1

        except Exception as e:
            # Print debug info for errors
            print(f"\n❌ Error processing {filename}:")
            print(f"  Error: {str(e)}")
            print(f"  Original name: '{original_name}'")
            print(f"  Normalized name: {json_name}")
            print(f"  DOS List: {dos_list}")
            print("\nDEBUG - Data Types:")
            print(f"  df_orders type: {type(df_orders)}")
            print(f"  df_orders columns: {df_orders.columns}")
            print(f"  df_orders shape: {df_orders.shape}")
            print(f"  df_line_items type: {type(df_line_items)}")
            print(f"  df_line_items columns: {df_line_items.columns}")
            print(f"  df_line_items shape: {df_line_items.shape}")
            continue

    print(f"\nProcessed {processed_files} files")

if __name__ == "__main__":
    process_mapping_s3()
