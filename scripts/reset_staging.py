#!/usr/bin/env python3
"""
reset_staging.py

Resets validation workflow by moving files from success/fails directories back to staging,
while removing validation metadata.

Uses the project's existing s3_utils.py for S3 operations.
"""
import os
import sys
import json
import argparse
import tempfile
import logging
from pathlib import Path
from dotenv import load_dotenv

# Get the project root directory and add to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

# Load environment variables
load_dotenv(project_root / '.env')

# Import the project's existing S3 utils
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, move, delete

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Reset Staging")

# S3 path prefixes
STAGING_PREFIX = 'data/hcfa_json/valid/mapped/staging/'
SUCCESS_PREFIX = f'{STAGING_PREFIX}success/'
FAILS_PREFIX = f'{STAGING_PREFIX}fails/'

def clean_json(data):
    """
    Remove validation and processing metadata from JSON data.
    """
    # Fields to remove
    fields_to_remove = [
        'validation_info',
        'processing_info',
        'override',
        'rate_assignment',
        'escalation',
        'denial'
    ]
    
    # Remove these fields if they exist
    for field in fields_to_remove:
        if field in data:
            del data[field]
            
    return data

def process_file(file_key, dry_run=False):
    """
    Process a single file:
    1. Get the JSON data from S3
    2. Clean the metadata
    3. Upload to staging
    4. Delete the original
    """
    try:
        # Get file name for destination
        filename = os.path.basename(file_key)
        dest_key = f"{STAGING_PREFIX}{filename}"
        
        # Get JSON data
        json_data = get_s3_json(file_key)
        
        # Clean the JSON
        cleaned_data = clean_json(json_data)
        
        if not dry_run:
            # Upload to staging
            upload_json_to_s3(cleaned_data, dest_key)
            
            # Delete the original
            delete(file_key)
            
            logger.info(f"Moved and cleaned: {file_key} -> {dest_key}")
        else:
            logger.info(f"[DRY RUN] Would move: {file_key} -> {dest_key}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {file_key}: {str(e)}")
        return False

def main():
    """Main function to process files."""
    parser = argparse.ArgumentParser(description='Reset validation workflow by moving files back to staging.')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making changes')
    parser.add_argument('--source', choices=['success', 'fails', 'both'], default='both', 
                       help='Source location to move files from (default: both)')
    parser.add_argument('--limit', type=int, help='Limit the number of files to process')
    
    args = parser.parse_args()
    
    # Get list of files to process
    files = []
    
    if args.source in ['success', 'both']:
        success_files = [k for k in list_objects(SUCCESS_PREFIX) if k.endswith('.json')]
        logger.info(f"Found {len(success_files)} files in success folder")
        files.extend(success_files)
    
    if args.source in ['fails', 'both']:
        fails_files = [k for k in list_objects(FAILS_PREFIX) if k.endswith('.json')]
        logger.info(f"Found {len(fails_files)} files in fails folder")
        files.extend(fails_files)
    
    # Apply limit if specified
    if args.limit and args.limit > 0:
        files = files[:args.limit]
        logger.info(f"Limited to {args.limit} files")
    
    # Display mode
    if args.dry_run:
        logger.info("Running in DRY RUN mode - no changes will be made")
    
    # Process files
    success_count = 0
    for file_key in files:
        if process_file(file_key, args.dry_run):
            success_count += 1
    
    # Summary
    logger.info(f"Processing complete: {success_count}/{len(files)} files processed")
    
    if args.dry_run:
        logger.info("This was a dry run - no changes were made")

if __name__ == "__main__":
    main()