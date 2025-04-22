import os
import json
import boto3
import tempfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load env vars
load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')
REGION = os.getenv('AWS_DEFAULT_REGION')
MAPPED_PREFIX = "data/hcfa_json/valid/mapped/"
STAGING_FOLDER = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\Documents\Bill_Review_INTERNAL\scripts\VAILIDATION\data\extracts\valid\mapped\staging"

# Setup S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION
)

def extract_mapping_from_staging(filename):
    local_path = os.path.join(STAGING_FOLDER, filename)
    if not os.path.exists(local_path):
        return None, None

    with open(local_path, 'r') as f:
        data = json.load(f)

    order_id = (
        data.get("Order_ID")
        or data.get("order_id")
        or data.get("mapping_info", {}).get("order_id")
    )

    filemaker_number = (
        data.get("filemaker_number")
        or data.get("filemaker_record_number")
        or data.get("mapping_info", {}).get("filemaker_number")
    )

    return order_id, filemaker_number

def reinject_mapping_info(key):
    filename = os.path.basename(key)

    # Step 1: Check for local staging match
    order_id, fm_number = extract_mapping_from_staging(filename)
    if not order_id:
        print(f"⚠️ Skipping {filename} - no order_id found in staging")
        return

    # Step 2: Pull S3 JSON
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    json_data = json.loads(response['Body'].read().decode('utf-8'))

    # Step 3: Inject mapping_info
    json_data['mapping_info'] = {
        "order_id": order_id,
        "filemaker_number": fm_number or "",
        "mapping_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Step 4: Save back to S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(json_data, indent=2).encode("utf-8")
    )

    print(f"🛠️ Injected mapping_info into: {filename}")

def main():
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=MAPPED_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                reinject_mapping_info(key)

if __name__ == "__main__":
    main()
