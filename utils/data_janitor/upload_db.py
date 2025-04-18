#!/usr/bin/env python3
"""
upload_db.py

Handles uploading and downloading of the SQLite database to/from S3.
"""
import os
import sys
import tempfile
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to Python path
project_root = str(Path(__file__).resolve().parents[2])
sys.path.append(project_root)

# Import S3 helper functions
from utils.s3_utils import upload, download, list_objects

# Load environment variables
load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')
DB_S3_KEY = 'reference_tables/orders2.db'

def upload_db(local_db_path):
    """Upload SQLite database to S3."""
    print(f"Uploading database from: {local_db_path}")
    print(f"To: s3://{S3_BUCKET}/{DB_S3_KEY}")
    
    try:
        upload(local_db_path, DB_S3_KEY)
        print("✔ Database uploaded successfully!")
    except Exception as e:
        print(f"❌ Failed to upload database: {str(e)}")

def download_db(output_path=None):
    """Download SQLite database from S3."""
    if output_path is None:
        output_path = os.path.join(project_root, 'reference_tables', 'orders2.db')
    
    print(f"Downloading database to: {output_path}")
    
    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Download the database
        download(DB_S3_KEY, output_path)
        print("✔ Database downloaded successfully!")
        return output_path
    except Exception as e:
        print(f"❌ Failed to download database: {str(e)}")
        return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Upload/Download SQLite database to/from S3")
    parser.add_argument('action', choices=['upload', 'download'], help='Action to perform')
    parser.add_argument('--path', help='Local path for database (required for upload, optional for download)')
    
    args = parser.parse_args()
    
    if args.action == 'upload':
        if not args.path:
            print("❌ Please specify the local database path with --path")
            sys.exit(1)
        upload_db(args.path)
    else:  # download
        download_db(args.path) 