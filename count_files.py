#!/usr/bin/env python3
"""
Count files in S3 bucket folder and subfolders.
Prints total number of files and JSON files found at each depth.
"""

import os
import json
import boto3
from pathlib import Path
from dotenv import load_dotenv
import argparse

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

def count_files_at_depth(prefix, target_depth):
    """Count files at specific depth and check for mapping_info."""
    paginator = s3.get_paginator('list_objects_v2')
    depth_counts = {}
    mapping_info_present = 0
    mapping_info_missing = 0
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            parts = key.split('/')
            depth = len(parts) - 1  # Subtract 1 because parts[0] is empty
            
            if depth == target_depth:
                if key.endswith('.json'):
                    try:
                        # Download and check JSON content
                        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                        content = response['Body'].read().decode('utf-8')
                        data = json.loads(content)
                        
                        if 'mapping_info' in data:
                            mapping_info_present += 1
                        else:
                            mapping_info_missing += 1
                            print(f"Missing mapping_info: {key}")
                    except Exception as e:
                        print(f"Error processing {key}: {str(e)}")
                
                if depth not in depth_counts:
                    depth_counts[depth] = {'total': 0, 'json': 0}
                depth_counts[depth]['total'] += 1
                if key.endswith('.json'):
                    depth_counts[depth]['json'] += 1
    
    return depth_counts, mapping_info_present, mapping_info_missing

def main():
    parser = argparse.ArgumentParser(description='Count files in S3 bucket at specific depth')
    parser.add_argument('--depth', type=int, help='Target depth to count files at')
    args = parser.parse_args()
    
    prefix = 'data/hcfa_json/valid/mapped/'
    
    if args.depth is not None:
        depth_counts, mapping_present, mapping_missing = count_files_at_depth(prefix, args.depth)
        print(f"\nDepth {args.depth}:")
        print(f"Total files: {depth_counts.get(args.depth, {}).get('total', 0)}")
        print(f"JSON files: {depth_counts.get(args.depth, {}).get('json', 0)}")
        print(f"Files with mapping_info: {mapping_present}")
        print(f"Files missing mapping_info: {mapping_missing}")
    else:
        # Count all depths
        depth_counts, _, _ = count_files_at_depth(prefix, None)
        for depth in sorted(depth_counts.keys()):
            print(f"\nDepth {depth}:")
            print(f"Total files: {depth_counts[depth]['total']}")
            print(f"JSON files: {depth_counts[depth]['json']}")

if __name__ == '__main__':
    main() 