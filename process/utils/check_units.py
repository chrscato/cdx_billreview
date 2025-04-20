#!/usr/bin/env python3
"""
check_units.py

Validates that non-ancillary codes only have 1 unit.
Uses ancillaries.json to determine which codes can have multiple units.
"""
import os
import sys
import json
import logging
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

# S3 bucket from environment
S3_BUCKET = os.getenv('S3_BUCKET')
STAGING_PREFIX = os.getenv('STAGING_PREFIX', 'data/hcfa_json/valid/mapped/staging/')

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("Check Units")

def clean_cpt_code(cpt):
    """Clean and normalize a CPT code."""
    if not cpt:
        return ""
    
    # Convert to string and strip whitespace
    return str(cpt).strip()

def check_units(json_data):
    """
    Check units in service lines.
    Rule: Non-ancillary codes must have exactly 1 unit.
    """
    # Load ancillary codes
    try:
        ancillaries_path = Path(__file__).parent.parent / 'data' / 'ancillaries.json'
        with open(ancillaries_path, 'r') as f:
            ancillary_codes = set(json.load(f).get('ignored_cpt_codes', []))
    except Exception as e:
        logging.error(f"Error loading ancillaries.json: {str(e)}")
        ancillary_codes = set()
    
    service_lines = json_data.get('service_lines', [])
    if not service_lines:
        return False, ["No service lines to validate"]
    
    violations = []
    
    for line in service_lines:
        cpt = clean_cpt_code(line.get('cpt_code', ''))
        if not cpt:
            continue
            
        try:
            units = int(line.get('units', 1))
        except (ValueError, TypeError):
            units = 1
        
        # Only check non-ancillary codes with units > 1
        if units > 1 and cpt not in ancillary_codes:
            violations.append({
                "cpt": cpt,
                "units": units
            })
    
    if violations:
        messages = [f"Found {len(violations)} CPT code(s) with multiple units"]
        for i, v in enumerate(violations[:3], 1):
            messages.append(f"  {i}. CPT {v['cpt']} has {v['units']} units")
        
        if len(violations) > 3:
            messages.append(f"  ... and {len(violations) - 3} more violations")
        
        return False, messages
    
    return True, ["All units are valid"]

def process_file(key):
    """Process a single JSON file."""
    logger = setup_logging()
    filename = os.path.basename(key)
    
    logger.info(f"Processing {filename}")
    
    try:
        # Get the JSON data
        json_data = get_s3_json(key)
        
        # Check units
        units_valid, messages = check_units(json_data)
        
        # Create validation_info if it doesn't exist
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        
        # Add units validation results
        json_data['validation_info']['units_validation'] = {
            'status': 'PASS' if units_valid else 'FAIL',
            'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'messages': messages
        }
        
        # Add simple validation message for failures
        if not units_valid:
            if 'processing_info' not in json_data:
                json_data['processing_info'] = {}
                
            if 'validation_messages' not in json_data['processing_info']:
                json_data['processing_info']['validation_messages'] = []
            
            json_data['processing_info']['validation_messages'].append("FAIL - units")
        
        # Log validation result
        if units_valid:
            logger.info(f"✓ Units validation passed: {filename}")
        else:
            logger.info(f"✗ Units validation failed: {filename}")
            for message in messages:
                logger.info(f"  {message}")
        
        # Upload updated JSON back to the same location
        upload_json_to_s3(json_data, key)
        
        return True
    
    except Exception as e:
        logger.error(f"Error processing {filename}: {str(e)}")
        return False

def main():
    """Process all JSON files in the staging directory."""
    logger = setup_logging()
    
    # Get command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Validate CPT code units in HCFA files')
    parser.add_argument('--file', help='Process a specific file only')
    parser.add_argument('--limit', type=int, help='Maximum number of files to process')
    args = parser.parse_args()
    
    if args.file:
        # Process a specific file
        key = f"{STAGING_PREFIX}{args.file}"
        logger.info(f"Processing specific file: {key}")
        process_file(key)
        return
    
    # Get all JSON files in staging directory
    json_keys = [k for k in list_objects(STAGING_PREFIX) if k.lower().endswith('.json')]
    
    if not json_keys:
        logger.info("No JSON files found in staging directory")
        return
    
    # Apply limit if specified
    if args.limit:
        json_keys = json_keys[:args.limit]
    
    logger.info(f"Found {len(json_keys)} files to process")
    
    # Process each file
    success_count = 0
    pass_count = 0
    fail_count = 0
    
    for key in json_keys:
        if process_file(key):
            success_count += 1
            
            # Count passes and fails
            try:
                json_data = get_s3_json(key)
                status = json_data.get('validation_info', {}).get('units_validation', {}).get('status')
                if status == 'PASS':
                    pass_count += 1
                elif status == 'FAIL':
                    fail_count += 1
            except:
                pass  # Ignore counting errors
    
    logger.info(f"Processing complete: {success_count}/{len(json_keys)} files processed")
    logger.info(f"Results: {pass_count} passed, {fail_count} failed")

if __name__ == "__main__":
    main()