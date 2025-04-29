#!/usr/bin/env python3
"""
refresh_summary.py

Refreshes the failed_summary.json file by:
1. Listing all files in the S3 fails directory
2. Extracting necessary information from each file
3. Recreating the failed_summary.json file with current data

This script helps keep the dashboard data in sync with the actual files in the 
fails directory, especially if files are added or removed outside of the normal workflow.
"""
import os
import sys
import json
import boto3
import argparse
import logging
from pathlib import Path
from datetime import datetime, date
from dateutil.parser import parse as parse_date
from typing import Dict, List, Any, Optional

# Get the project root directory and add to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

# Import the project's existing utilities
from utils.s3_utils import list_objects, get_s3_json
from utils.summary_manager import ensure_summary_file, DEFAULT_SUMMARY_PATH

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Summary Refresh")

# S3 path prefixes
FAILS_PREFIX = 'data/hcfa_json/valid/mapped/staging/fails/'


def extract_failure_types(data: Dict[str, Any]) -> List[str]:
    """
    Extract the failure types from the validation_info in the JSON data.
    
    Args:
        data (Dict[str, Any]): The JSON data from the file
        
    Returns:
        List[str]: List of failure types
    """
    try:
        reasons = data.get("validation_info", {}).get("failure_reasons", [])
        return [r.split(":", 1)[0].strip() for r in reasons if isinstance(r, str)]
    except Exception as e:
        logger.warning(f"Error extracting failure types: {str(e)}")
        return ["UNKNOWN"]


def extract_provider(data: Dict[str, Any]) -> str:
    """
    Extract the provider name from the JSON data.
    
    Args:
        data (Dict[str, Any]): The JSON data from the file
        
    Returns:
        str: Provider name
    """
    try:
        # Try different possible paths for provider info
        provider = data.get("filemaker", {}).get("provider", {}).get("Billing Name")
        if not provider:
            provider = data.get("filemaker", {}).get("provider", {}).get("DBA Name Billing Name")
        if not provider:
            provider = data.get("billing_info", {}).get("billing_provider_name")
        
        return provider or "Unknown Provider"
    except Exception as e:
        logger.warning(f"Error extracting provider: {str(e)}")
        return "Unknown Provider"


def extract_dos(data: Dict[str, Any]) -> str:
    """
    Extract the date of service from the JSON data.
    
    Args:
        data (Dict[str, Any]): The JSON data from the file
        
    Returns:
        str: Date of service in YYYY-MM-DD format
    """
    try:
        # Try to get DOS from filemaker data first
        dos = None
        if 'filemaker' in data and 'line_items' in data['filemaker'] and data['filemaker']['line_items']:
            dos = data['filemaker']['line_items'][0].get('DOS')
        
        # If not found, try service_lines
        if not dos and 'service_lines' in data and data['service_lines']:
            date_str = data['service_lines'][0].get('date_of_service', '')
            if date_str:
                # Handle format like "MM/DD/YY - MM/DD/YY" by taking the first date
                if ' - ' in date_str:
                    date_str = date_str.split(' - ')[0]
                
                # Try to parse the date
                formats = ['%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d']
                for fmt in formats:
                    try:
                        date_obj = datetime.strptime(date_str.strip(), fmt)
                        dos = date_obj.strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue
        
        return dos or date.today().strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Error extracting DOS: {str(e)}")
        return date.today().strftime('%Y-%m-%d')


def calculate_age_days(dos_str: str) -> int:
    """
    Calculate the age in days based on the DOS.
    
    Args:
        dos_str (str): Date of service in YYYY-MM-DD format
        
    Returns:
        int: Age in days
    """
    try:
        date_obj = datetime.strptime(dos_str, '%Y-%m-%d').date()
        return (date.today() - date_obj).days
    except Exception as e:
        logger.warning(f"Error calculating age for DOS {dos_str}: {str(e)}")
        return 0


def validate_provider_info(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that required provider info fields are present and not null.
    
    Args:
        data (Dict[str, Any]): The JSON data from the file
        
    Returns:
        Dict[str, Any]: Dictionary containing validation results
    """
    required_fields = [
        "Billing Address 1",
        "Billing Address City",
        "Billing Address Postal Code",
        "Billing Address State",
        "Billing Name"
    ]
    
    validation_result = {
        "is_valid": True,
        "missing_fields": [],
        "null_fields": []
    }
    
    try:
        # Look for provider info under filemaker.provider
        provider_info = data.get("filemaker", {}).get("provider", {})
        
        # Check for missing fields
        for field in required_fields:
            if field not in provider_info:
                validation_result["missing_fields"].append(field)
                validation_result["is_valid"] = False
        
        # Check for null values in existing fields
        for field in required_fields:
            if field in provider_info and provider_info[field] is None:
                validation_result["null_fields"].append(field)
                validation_result["is_valid"] = False
                
        return validation_result
        
    except Exception as e:
        logger.warning(f"Error validating provider info: {str(e)}")
        validation_result["is_valid"] = False
        validation_result["error"] = str(e)
        return validation_result


def process_file(file_key: str, verbose: bool = False) -> Optional[Dict[str, Any]]:
    """
    Process a single file to extract summary information.
    
    Args:
        file_key (str): The S3 key of the file
        verbose (bool): Whether to log verbose details
        
    Returns:
        Optional[Dict[str, Any]]: Summary entry or None if error
    """
    filename = os.path.basename(file_key)
    
    try:
        # Get the JSON data
        json_data = get_s3_json(file_key)
        
        # Extract data
        failure_types = extract_failure_types(json_data)
        provider = extract_provider(json_data)
        dos = extract_dos(json_data)
        age_days = calculate_age_days(dos)
        
        # Validate provider info
        provider_validation = validate_provider_info(json_data)
        
        entry = {
            "filename": filename,
            "failure_types": failure_types,
            "provider": provider,
            "dos": dos,
            "age_days": age_days,
            "provider_validation": provider_validation
        }
        
        if verbose:
            logger.info(f"Processed {filename}: {len(failure_types)} failure types")
            if not provider_validation["is_valid"]:
                logger.warning(f"Provider info validation failed for {filename}:")
                if provider_validation["missing_fields"]:
                    logger.warning(f"Missing fields: {', '.join(provider_validation['missing_fields'])}")
                if provider_validation["null_fields"]:
                    logger.warning(f"Null fields: {', '.join(provider_validation['null_fields'])}")
        
        return entry
    
    except Exception as e:
        logger.error(f"Error processing file {filename}: {str(e)}")
        return {
            "filename": filename,
            "failure_types": ["READ_ERROR"],
            "provider": "Unknown Provider",
            "dos": date.today().strftime('%Y-%m-%d'),
            "age_days": 0,
            "provider_validation": {
                "is_valid": False,
                "error": str(e)
            }
        }


def main():
    """
    Main function to refresh the summary file.
    """
    parser = argparse.ArgumentParser(description='Refresh the failed summary JSON file from S3 fails directory.')
    parser.add_argument('--output', type=str, default=DEFAULT_SUMMARY_PATH, 
                        help=f'Output path for summary file (default: {DEFAULT_SUMMARY_PATH})')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without writing the file')
    
    args = parser.parse_args()
    
    # Ensure the output directory exists
    output_dir = os.path.dirname(args.output)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            logger.info(f"Created directory: {output_dir}")
        except Exception as e:
            logger.error(f"Error creating directory {output_dir}: {str(e)}")
            return 1
    
    # Get list of files to process
    try:
        file_keys = [k for k in list_objects(FAILS_PREFIX) if k.endswith('.json')]
        logger.info(f"Found {len(file_keys)} files in fails directory")
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return 1
    
    if not file_keys:
        logger.warning("No files found to process")
        if args.dry_run:
            logger.info("Dry run - would create empty summary file")
            return 0
        
        # Create empty summary
        with open(args.output, 'w') as f:
            json.dump([], f, indent=2)
        logger.info(f"Created empty summary file at {args.output}")
        return 0
    
    # Process files and build summary
    summary = []
    
    for file_key in file_keys:
        entry = process_file(file_key, args.verbose)
        if entry:
            summary.append(entry)
    
    # Sort by age (most recent first)
    summary.sort(key=lambda x: x.get('age_days', 0) or 0, reverse=True)
    
    if args.dry_run:
        logger.info(f"Dry run - summary would contain {len(summary)} entries")
        logger.info(f"First entry in summary would be: {summary[0] if summary else None}")
        return 0
    
    # Write the summary file
    try:
        with open(args.output, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Successfully wrote {len(summary)} entries to {args.output}")
    except Exception as e:
        logger.error(f"Error writing summary file: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 