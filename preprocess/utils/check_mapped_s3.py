import boto3
import json
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv('S3_BUCKET')
MAPPED_PREFIX = 'data/hcfa_json/valid/mapped/'
DEST_PREFIX = 'data/hcfa_json/valid/'

def check_json_format(json_data):
    if 'mapping_info' not in json_data:
        return False, "Missing 'mapping_info'"
    if 'order_id' not in json_data['mapping_info']:
        return False, "Missing 'order_id' in 'mapping_info'"
    return True, "Valid"

def move_file(s3, key, reason):
    filename = key.replace(MAPPED_PREFIX, "")
    dest_key = f"{DEST_PREFIX}{filename}"
    
    print(f"⏪ Moving: {key} ➜ {dest_key} (Reason: {reason})")

    # Copy then delete
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={'Bucket': BUCKET, 'Key': key},
        Key=dest_key
    )
    s3.delete_object(Bucket=BUCKET, Key=key)

def main():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )

    paginator = s3.get_paginator('list_objects_v2')
    total_checked = 0
    total_valid = 0
    total_invalid = 0

    for page in paginator.paginate(Bucket=BUCKET, Prefix=MAPPED_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']

            # Skip subfolders and non-json
            if not key.endswith('.json'):
                continue
            if '/' in key.replace(MAPPED_PREFIX, ''):
                continue

            total_checked += 1

            try:
                response = s3.get_object(Bucket=BUCKET, Key=key)
                content = response['Body'].read().decode('utf-8')
                data = json.loads(content)

                is_valid, reason = check_json_format(data)
                if is_valid:
                    total_valid += 1
                else:
                    move_file(s3, key, reason)
                    total_invalid += 1

            except ClientError as e:
                move_file(s3, key, f"S3 error: {e.response['Error']['Message']}")
                total_invalid += 1
            except json.JSONDecodeError:
                move_file(s3, key, "Invalid JSON structure")
                total_invalid += 1

    print(f"\nSummary: Checked {total_checked} files | ✅ {total_valid} valid | ❌ {total_invalid} moved")

if __name__ == "__main__":
    main()
