#!/usr/bin/env python3
"""
find_missing_cpts.py

Scans all the fails JSON files in S3, extracts their CPT codes,
and checks if they exist in the dim_proc table of the filemaker.db.
Reports all CPT codes that aren't found in the dim_proc table.
"""
import os
import sys
import json
import boto3
import sqlite3
import tempfile
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to Python path
project_root = Path(__file__).resolve().parents[0]  # Adjust this if needed
sys.path.append(str(project_root))

# Load environment variables
load_dotenv()

# S3 configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'bill-review-prod')
FAILS_PREFIX = 'data/hcfa_json/valid/mapped/staging/fails/'

# Database configuration
DB_PATH = os.path.join(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review\filemaker.db")

def get_db_connection():
    """Create a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def get_all_dim_proc_cpts():
    """Get all CPT codes from dim_proc table."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT proc_cd FROM dim_proc")
        return {row['proc_cd'] for row in cursor.fetchall()}
    except sqlite3.Error as e:
        print(f"Error querying dim_proc table: {e}")
        return set()
    finally:
        conn.close()

def list_s3_files():
    """List all JSON files in the fails directory."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        # Get list of all JSON files in the fails folder
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=FAILS_PREFIX
        )
        
        files = []
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
        
        return s3_client, files
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        sys.exit(1)

def extract_cpt_codes(s3_client, file_key):
    """Extract CPT codes from a JSON file in S3."""
    try:
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=file_key
        )
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Extract CPT codes from service_lines (reference the stagingjsonfile.json format)
        cpt_codes = []
        if 'service_lines' in data:
            for line in data['service_lines']:
                if 'cpt_code' in line:
                    cpt_codes.append(line['cpt_code'])
        
        return cpt_codes, data
    except Exception as e:
        print(f"Error processing {file_key}: {e}")
        return [], None

def find_missing_cpts():
    """Main function to find CPT codes that aren't in dim_proc table."""
    # Get all CPT codes from dim_proc
    print("Loading CPT codes from dim_proc table...")
    db_cpt_codes = get_all_dim_proc_cpts()
    print(f"Found {len(db_cpt_codes)} CPT codes in dim_proc table")
    
    # List all JSON files in the fails directory
    print(f"Listing files in S3 bucket: {S3_BUCKET}/{FAILS_PREFIX}")
    s3_client, files = list_s3_files()
    print(f"Found {len(files)} files to process")
    
    # Process each file
    all_cpt_codes = set()
    missing_cpt_codes = set()
    file_cpt_map = {}  # Map of filename to its missing CPT codes
    
    for i, file_key in enumerate(files, 1):
        filename = os.path.basename(file_key)
        print(f"Processing file {i}/{len(files)}: {filename}")
        
        # Extract CPT codes from file
        cpt_codes, data = extract_cpt_codes(s3_client, file_key)
        
        if not cpt_codes:
            print(f"  No CPT codes found in {filename}")
            continue
        
        # Add to all CPT codes
        all_cpt_codes.update(cpt_codes)
        
        # Check which CPT codes are missing from dim_proc
        missing_in_file = [cpt for cpt in cpt_codes if cpt not in db_cpt_codes]
        
        if missing_in_file:
            missing_cpt_codes.update(missing_in_file)
            file_cpt_map[filename] = {
                'missing_cpts': missing_in_file
            }
            print(f"  Found {len(missing_in_file)} missing CPT codes: {', '.join(missing_in_file)}")
        else:
            print(f"  All CPT codes found in dim_proc table")
    
    # Print summary
    print("\n=== Missing CPT Codes Summary ===")
    print(f"Total unique CPT codes found in fails: {len(all_cpt_codes)}")
    print(f"Total CPT codes missing from dim_proc: {len(missing_cpt_codes)}")
    
    if missing_cpt_codes:
        print("\nList of missing CPT codes:")
        for cpt in sorted(missing_cpt_codes):
            print(f"  {cpt}")
        
        print("\nFiles with missing CPT codes:")
        for filename, data in file_cpt_map.items():
            print(f"  ): {', '.join(data['missing_cpts'])}")
        
        # Save results to a CSV file
        with open("missing_cpts.csv", "w") as f:
            f.write("cpt_code,files_count\n")
            
            # Count how many files each missing CPT appears in
            cpt_file_count = {}
            for filename, data in file_cpt_map.items():
                for cpt in data['missing_cpts']:
                    cpt_file_count[cpt] = cpt_file_count.get(cpt, 0) + 1
            
            # Write to CSV
            for cpt, count in sorted(cpt_file_count.items(), key=lambda x: x[1], reverse=True):
                f.write(f"{cpt},{count}\n")
        
        print("\nResults saved to missing_cpts.csv")
    else:
        print("\nAll CPT codes from fails files exist in dim_proc table!")

if __name__ == "__main__":
    find_missing_cpts()