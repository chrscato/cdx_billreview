import os
import json
import tempfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

import sys
project_root = str(Path(__file__).resolve().parents[1])
sys.path.append(project_root)

from utils.s3_utils import list_objects, download, upload, move

# Load environment
load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')

# S3 prefixes
UNMAPPED_PREFIX = 'data/hcfa_json/valid/unmapped/'
MAPPED_PREFIX = 'data/hcfa_json/valid/mapped/'

# Local staging folder path (update if needed)
LOCAL_STAGING_FOLDER = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\Documents\Bill_Review_INTERNAL\scripts\VAILIDATION\data\extracts\valid\mapped\staging"

def extract_order_id_and_fm_number(json_data):
    order_id = None
    fm_number = None

    # Check common locations for order ID
    if "Order_ID" in json_data:
        order_id = json_data["Order_ID"]
    elif "order_id" in json_data:
        order_id = json_data["order_id"]
    elif "mapping_info" in json_data:
        order_id = json_data["mapping_info"].get("order_id")

    # Check for FM number
    fm_number = json_data.get("filemaker_number") or \
                json_data.get("filemaker_record_number") or \
                json_data.get("mapping_info", {}).get("filemaker_number")

    return order_id, fm_number


def patch_from_staging():
    print("🔍 Scanning S3 unmapped files...")
    all_keys = list_objects(UNMAPPED_PREFIX)
    json_keys = [k for k in all_keys if k.endswith('.json')]

    print(f"Found {len(json_keys)} files in unmapped S3 folder.")
    patched_count = 0

    for s3_key in json_keys:
        filename = os.path.basename(s3_key)
        staging_path = os.path.join(LOCAL_STAGING_FOLDER, filename)

        if not os.path.exists(staging_path):
            continue  # No match in staging, skip

        with open(staging_path, 'r') as f:
            staging_data = json.load(f)

        order_id, fm_number = extract_order_id_and_fm_number(staging_data)

        if not order_id:
            print(f"⚠️ Skipping {filename} — missing order_id in staging file.")
            continue

        # Download the S3 unmapped file locally
        local_unmapped = os.path.join(tempfile.gettempdir(), filename)
        download(s3_key, local_unmapped)

        with open(local_unmapped, 'r') as f:
            json_data = json.load(f)

        # Inject mapping_info properly
        json_data['mapping_info'] = {
            "order_id": order_id,
            "filemaker_number": fm_number or "",
            "mapping_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Save updated JSON
        with open(local_unmapped, 'w') as f:
            json.dump(json_data, f, indent=4)

        # Upload to mapped
        mapped_key = f"{MAPPED_PREFIX}{filename}"
        upload(local_unmapped, mapped_key)

        # Move original from unmapped to mapped (overwrite allowed)
        move(s3_key, mapped_key)

        print(f"✔ Patched and moved: {filename} -> order_id {order_id}")
        patched_count += 1

    print(f"\n✅ Finished patching. Total files updated: {patched_count}")

if __name__ == "__main__":
    patch_from_staging()
