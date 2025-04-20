#!/usr/bin/env python3
"""
check_arthrogram.py

Checks if FileMaker orders have a bundle_type of "arthrogram" (case insensitive).
Moves matching files to a dedicated arthrograms folder for specialized processing.
Uses existing .env configuration and s3_utils.py for S3 operations.
"""
import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Get the project root directory
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

# Load environment variables from root .env file
load_dotenv(project_root / '.env')

# Import S3 helper functions from root utils directory
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3

# S3 prefixes - use environment variables if available, otherwise use defaults
S3_BUCKET = os.getenv('S3_BUCKET')
STAGING_PREFIX = os.getenv('STAGING_PREFIX', 'data/hcfa_json/valid/mapped/staging/')
ARTHROGRAM_PREFIX = os.getenv('ARTHROGRAM_PREFIX', 'data/hcfa_json/valid/mapped/staging/arthrograms/')

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("Check Arthrogram")

def is_arthrogram(json_data):
    """
    Check if the order's bundle_type is "arthrogram" (case insensitive).
    
    Args:
        json_data (dict): The JSON data containing FileMaker order information
        
    Returns:
        bool: True if bundle_type is arthrogram, False otherwise
    """
    try:
        # Access bundle_type from the FileMaker order data
        bundle_type = json_data.get('filemaker', {}).get('order', {}).get('bundle_type')
        
        # Check if bundle_type is "arthrogram" (case insensitive)
        if bundle_type and isinstance(bundle_type, str) and bundle_type.lower() == 'arthrogram':
            return True
        return False
    except Exception as e:
        # Log error but don't fail processing
        logger = logging.getLogger("Check Arthrogram")
        logger.error(f"Error checking arthrogram status: {str(e)}")
        return False

def add_arthrogram_status(json_data, is_arthrogram_order):
    """
    Add arthrogram status to the JSON data.
    
    Args:
        json_data (dict): The JSON data to update
        is_arthrogram_order (bool): Whether this is an arthrogram order
        
    Returns:
        dict: Updated JSON data with arthrogram status
    """
    # Create processing_info section if it doesn't exist
    if 'processing_info' not in json_data:
        json_data['processing_info'] = {}
    
    # Add arthrogram check info
    json_data['processing_info']['arthrogram_check'] = {
        'is_arthrogram': is_arthrogram_order,
        'check_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return json_data

def process_file(key):
    """
    Process a single JSON file.
    
    Args:
        key (str): S3 key of the file to process
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    logger = setup_logging()
    filename = os.path.basename(key)
    
    logger.info(f"Processing {filename}")
    
    try:
        # Get the JSON data
        json_data = get_s3_json(key)
        
        # Check if this is an arthrogram order
        arthrogram_order = is_arthrogram(json_data)
        
        # Add arthrogram status to JSON
        json_data = add_arthrogram_status(json_data, arthrogram_order)
        
        # Determine destination based on arthrogram status
        if arthrogram_order:
            dest_key = f"{ARTHROGRAM_PREFIX}{filename}"
            logger.info(f"✅ Arthrogram order identified - moving to arthrogram folder: {dest_key}")
        else:
            # Keep in original staging location, just update the JSON
            dest_key = key
            logger.info(f"Regular order (not arthrogram) - updating in place")
        
        # Upload updated JSON to destination
        upload_json_to_s3(json_data, dest_key)
        
        # If moved to arthrogram folder, delete from original location
        if arthrogram_order and dest_key != key:
            from boto3 import client
            s3_client = client('s3')
            s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {filename}: {str(e)}", exc_info=True)
        return False

def process_staging_files(file_key=None, limit=None):
    """
    Process files in the staging directory.
    
    Args:
        file_key (str, optional): Process a specific file only
        limit (int, optional): Maximum number of files to process
    """
    logger = setup_logging()
    
    if file_key:
        # Process a specific file
        full_key = f"{STAGING_PREFIX}{file_key}"
        logger.info(f"Processing specific file: {full_key}")
        process_file(full_key)
    else:
        # Get all JSON files in staging directory
        json_keys = [k for k in list_objects(STAGING_PREFIX) if k.lower().endswith('.json')]
        
        if not json_keys:
            logger.info("No JSON files found in staging directory")
            return
        
        # Apply limit if specified
        if limit:
            json_keys = json_keys[:int(limit)]
        
        logger.info(f"Found {len(json_keys)} JSON files to process")
        
        success_count = 0
        arthrogram_count = 0
        for key in json_keys:
            if process_file(key):
                success_count += 1
                # Get the JSON data to count arthrograms (after processing)
                try:
                    json_data = get_s3_json(key)
                    if json_data.get('processing_info', {}).get('arthrogram_check', {}).get('is_arthrogram', False):
                        arthrogram_count += 1
                except:
                    pass  # Ignore errors in counting
        
        logger.info(f"Processing complete. {success_count}/{len(json_keys)} files processed successfully.")
        logger.info(f"Found {arthrogram_count} arthrogram orders.")

def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Process HCFA JSON files to identify arthrogram orders.')
    parser.add_argument('--file', help='Process a specific file only')
    parser.add_argument('--limit', type=int, help='Maximum number of files to process')
    
    args = parser.parse_args()
    process_staging_files(file_key=args.file, limit=args.limit)

if __name__ == "__main__":
    main()