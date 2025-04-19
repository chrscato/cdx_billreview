#!/usr/bin/env python3
"""
compare_service_lines.py

Compares service lines from HCFA JSON files with FileMaker line items.
Checks for exact matches of CPT codes and adds validation status to the JSON.
Handles duplicate codes and ignores ancillary codes.
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

# Load environment variables from .env
load_dotenv(project_root / '.env')

# S3 prefixes
STAGING_PREFIX = os.getenv('STAGING_PREFIX', 'data/hcfa_json/valid/mapped/staging/')
S3_BUCKET = os.getenv('S3_BUCKET')

def compare_cpt_codes(json_data):
    """
    Compare CPT codes from service_lines with those in filemaker.line_items.
    Considers quantity of each code and handles ancillary codes that can be ignored.
    
    Args:
        json_data (dict): The JSON data containing both service_lines and filemaker.line_items
        
    Returns:
        tuple: (match_status, discrepancies)
            - match_status (bool): True if all CPT codes match, False otherwise
            - discrepancies (dict): Dictionary describing any discrepancies found
    """
    # Load list of ancillary codes to ignore
    try:
        with open(Path(__file__).parent.parent / 'data' / 'ancillaries.json', 'r') as f:
            ancillaries = json.load(f)
            ignored_codes = set(ancillaries.get('ignored_cpt_codes', []))
    except (FileNotFoundError, json.JSONDecodeError):
        ignored_codes = set()
    
    # Extract CPT codes from service_lines with counts
    hcfa_cpt_counts = {}
    for line in json_data.get('service_lines', []):
        cpt = line.get('cpt_code', '').strip()
        if cpt and cpt not in ignored_codes:
            if cpt in hcfa_cpt_counts:
                hcfa_cpt_counts[cpt] += 1
            else:
                hcfa_cpt_counts[cpt] = 1
    
    # Extract CPT codes from filemaker.line_items with counts
    fm_cpt_counts = {}
    for line in json_data.get('filemaker', {}).get('line_items', []):
        cpt = line.get('CPT', '').strip()
        if cpt and cpt.lower() != 'none' and cpt not in ignored_codes:
            if cpt in fm_cpt_counts:
                fm_cpt_counts[cpt] += 1
            else:
                fm_cpt_counts[cpt] = 1
    
    # Get all unique CPT codes
    all_cpt_codes = set(hcfa_cpt_counts.keys()) | set(fm_cpt_counts.keys())
    
    # Identify missing and mismatched counts
    missing_from_hcfa = []
    missing_from_fm = []
    count_mismatches = []
    
    for cpt in all_cpt_codes:
        hcfa_count = hcfa_cpt_counts.get(cpt, 0)
        fm_count = fm_cpt_counts.get(cpt, 0)
        
        if hcfa_count == 0:
            missing_from_hcfa.append(cpt)
        elif fm_count == 0:
            missing_from_fm.append(cpt)
        elif hcfa_count != fm_count:
            count_mismatches.append({
                'cpt': cpt,
                'hcfa_count': hcfa_count,
                'filemaker_count': fm_count
            })
    
    # Prepare discrepancies report
    discrepancies = {
        'hcfa_cpt_codes': [{'cpt': cpt, 'count': count} for cpt, count in hcfa_cpt_counts.items()],
        'filemaker_cpt_codes': [{'cpt': cpt, 'count': count} for cpt, count in fm_cpt_counts.items()],
        'cpt_in_filemaker_not_in_hcfa': missing_from_hcfa,
        'cpt_in_hcfa_not_in_filemaker': missing_from_fm,
        'cpt_count_mismatches': count_mismatches,
        'ignored_ancillary_codes': list(ignored_codes)
    }
    
    # Determine match status
    match_status = (len(missing_from_hcfa) == 0 and 
                   len(missing_from_fm) == 0 and 
                   len(count_mismatches) == 0)
    
    return match_status, discrepancies

def add_validation_status(json_data, match_status, discrepancies):
    """
    Add validation status to the JSON data.
    
    Args:
        json_data (dict): The JSON data to update
        match_status (bool): True if all CPT codes match, False otherwise
        discrepancies (dict): Dictionary describing any discrepancies found
    
    Returns:
        dict: Updated JSON data with validation status
    """
    # Create validation_info section if it doesn't exist
    if 'validation_info' not in json_data:
        json_data['validation_info'] = {}
    
    # Add CPT validation info
    json_data['validation_info']['cpt_validation'] = {
        'status': 'matched' if match_status else 'discrepancy',
        'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'discrepancies': discrepancies if not match_status else {}
    }
    
    return json_data