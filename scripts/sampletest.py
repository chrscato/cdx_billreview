import os
import sys
import json
import sqlite3
import tempfile
import pandas as pd
from datetime import datetime
from pathlib import Path
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

# Set root directory for importing utils
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from utils.s3_utils import list_objects, download

# Setup .env and constants
load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")

VALID_PREFIX = 'data/hcfa_json/valid/'
DB_PATH = ROOT_DIR / "filemaker.db"


def normalize_text(text):
    if not text:
        return ""
    return "".join(c for c in text.upper().strip() if c.isalnum())

def parse_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    if isinstance(date_str, datetime):
        return date_str
    # Handle ranges like "03/31/25 - 03/31/25"
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0]
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def load_db_data():
    conn = sqlite3.connect(DB_PATH)
    orders_df = pd.read_sql("SELECT * FROM orders", conn)
    lines_df = pd.read_sql("SELECT * FROM line_items", conn)
    conn.close()
    return orders_df, lines_df

def main(sample_size=10, output_csv="json_vs_db_sample.csv"):
    print("🔄 Loading DB...")
    orders_df, line_items_df = load_db_data()
    orders_df["NormalizedPatientName"] = orders_df.apply(
        lambda row: normalize_text(f"{row['Patient_First_Name']} {row['Patient_Last_Name']}"), axis=1
    )

    print("📂 Listing JSON files...")
    keys = [k for k in list_objects(VALID_PREFIX)
            if k.endswith(".json") and "mapped" not in k and "unmapped" not in k]

    print(f"🔍 Sampling {min(sample_size, len(keys))} files")
    samples = []

    for key in keys[:sample_size]:
        filename = os.path.basename(key)
        local_path = os.path.join(tempfile.gettempdir(), filename)
        download(key, local_path)

        try:
            with open(local_path, "r") as f:
                data = json.load(f)

            raw_name = data.get("patient_info", {}).get("patient_name", "")
            json_name = normalize_text(raw_name)
            service_lines = data.get("service_lines", [])

            json_dos = []
            for line in service_lines:
                raw_dos = line.get("date_of_service")
                parsed_dos = parse_date(raw_dos)
                if isinstance(parsed_dos, datetime):
                    json_dos.append(parsed_dos)

            json_cpts = [
                l.get("cpt_code") for l in service_lines if l.get("cpt_code")
            ]

            last_name_guess = raw_name.strip().split()[-1].upper()
            db_candidates = orders_df[orders_df["Patient_Last_Name"].str.upper() == last_name_guess]

            for _, row in db_candidates.iterrows():
                name_score = fuzz.token_sort_ratio(json_name, row["NormalizedPatientName"])
                order_id = row["Order_ID"]
                dos_list = line_items_df[line_items_df["Order_ID"] == order_id]["DOS"].dropna().tolist()
                dos_list = [parse_date(d) for d in dos_list if parse_date(d)]

                samples.append({
                    "json_file": filename,
                    "json_name": json_name,
                    "json_dos": json_dos[:3],
                    "json_cpts": json_cpts[:3],
                    "db_order_id": order_id,
                    "db_name": row["NormalizedPatientName"],
                    "db_dos": dos_list[:3],
                    "name_score": name_score
                })

        except Exception as e:
            print(f"⚠️ Failed to process {filename}: {e}")
            continue

    df = pd.DataFrame(samples)
    df.to_csv(output_csv, index=False)
    print(f"✅ Sample comparison saved to: {output_csv}")

if __name__ == "__main__":
    main()
