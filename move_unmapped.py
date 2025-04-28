#!/usr/bin/env python3
"""
Move files without mapping_info from mapped directory back to valid directory.
"""

import os
import json
import boto3
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
S3_BUCKET = os.getenv('S3_BUCKET')
REGION = os.getenv('AWS_DEFAULT_REGION')

# Setup S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION
)

def move_files_without_mapping():
    """Move files without mapping_info back to valid directory."""
    mapped_prefix = 'data/hcfa_json/valid/mapped/'
    valid_prefix = 'data/hcfa_json/valid/'
    moved_count = 0
    
    # List all objects in mapped directory
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=mapped_prefix):
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            
            # Skip if not a JSON file
            if not key.endswith('.json'):
                continue
                
            try:
                # Download and check JSON content
                response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                content = response['Body'].read().decode('utf-8')
                data = json.loads(content)
                
                # If no mapping_info, move the file
                if 'mapping_info' not in data:
                    # Get the filename without the mapped/ prefix
                    filename = os.path.basename(key)
                    new_key = f"{valid_prefix}{filename}"
                    
                    # Copy to new location
                    s3.copy_object(
                        Bucket=S3_BUCKET,
                        CopySource={'Bucket': S3_BUCKET, 'Key': key},
                        Key=new_key
                    )
                    
                    # Delete from old location
                    s3.delete_object(
                        Bucket=S3_BUCKET,
                        Key=key
                    )
                    
                    print(f"Moved {filename} back to valid directory")
                    moved_count += 1
                    
            except Exception as e:
                print(f"Error processing {key}: {str(e)}")
    
    print(f"\nTotal files moved: {moved_count}")

if __name__ == '__main__':
    move_files_without_mapping() 