#!/usr/bin/env python3
"""
validatejson.py

Validates JSON files in S3 that were extracted from HCFA forms.
Moves valid files to a validated prefix and invalid ones to a review prefix.
"""
import os
import json
import re
import sys
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to Python path
project_root = str(Path(__file__).resolve().parents[2])
sys.path.append(project_root)

# Load environment variables
load_dotenv()

# Import S3 helper functions
from utils.s3_utils import list_objects, download, upload, move

# S3 prefixes (override in .env if needed)
INPUT_PREFIX = os.getenv('PREPROCESS_INPUT_PREFIX', 'data/hcfa_json/')
VALID_PREFIX = os.getenv('VALIDATE_VALID_PREFIX', 'data/hcfa_json/valid/')
INVALID_PREFIX = os.getenv('VALIDATE_INVALID_PREFIX', 'data/hcfa_json/invalid/')
LOG_PREFIX = os.getenv('VALIDATE_LOG_PREFIX', 'logs/validation_errors.log')
S3_BUCKET = os.getenv('S3_BUCKET')

# Precompiled regex patterns for critical validations
ZIP_PATTERN = re.compile(r"^\d{5}$")
CURRENCY_PATTERN = re.compile(r"^\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})?$")

def clean_text(text):
    """
    Clean text by removing special characters and normalizing spaces.
    """
    if not text:
        return text
    
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', text)
    
    # Replace any non-ASCII characters with closest ASCII equivalent
    text = text.encode('ascii', 'ignore').decode('ascii')
    
    # Fix multiple spaces and trim
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def clean_name(name):
    """
    Clean a patient name by removing special characters and normalizing spaces.
    """
    return clean_text(name)

def parse_date(date_str):
    """
    Try to parse a date string using formats MM/DD/YY and MM/DD/YYYY.
    Return the date in YYYY-MM-DD format if successful, otherwise None.
    """
    if not date_str:
        return None
        
    date_str = clean_text(date_str)
    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def clean_currency(amount):
    """
    Clean and standardize currency amounts.
    """
    if not amount:
        return None
    
    # Remove any non-numeric characters except decimal point
    amount = re.sub(r'[^\d.]', '', str(amount))
    try:
        value = float(amount)
        return f"${value:.2f}"
    except ValueError:
        return None

def validate_json(data):
    # Clean all text fields to handle encoding issues
    if "patient_info" in data:
        if "patient_name" in data["patient_info"]:
            data["patient_info"]["patient_name"] = clean_name(data["patient_info"]["patient_name"])
        if "patient_zip" in data["patient_info"]:
            data["patient_info"]["patient_zip"] = clean_text(data["patient_info"]["patient_zip"])

    if "billing_info" in data:
        if "billing_provider_name" in data["billing_info"]:
            data["billing_info"]["billing_provider_name"] = clean_text(data["billing_info"]["billing_provider_name"])
        if "billing_provider_address" in data["billing_info"]:
            data["billing_info"]["billing_provider_address"] = clean_text(data["billing_info"]["billing_provider_address"])

    # Define minimum required fields
    required_fields = {
        "patient_info": ["patient_name"],  # ZIP removed from required fields
        "service_lines": ["date_of_service", "cpt_code", "charge_amount"]  # Reduced to essential fields
        #"billing_info": ["billing_provider_name", "total_charge"]  # Reduced to essential fields
    }
    
    # Check required sections and fields
    for section, fields in required_fields.items():
        if section not in data:
            return False, f"Missing required section: {section}"
        
        if section == "service_lines":
            if not isinstance(data[section], list) or not data[section]:
                return False, f"{section} should be a non-empty list"
            for service in data[section]:
                for field in fields:
                    if field not in service:
                        return False, f"Missing required field: {field} in service_lines"
        else:
            for field in fields:
                if field not in data[section]:
                    return False, f"Missing required field: {field} in {section}"
    
    # Optional ZIP validation - only if present
    if "patient_info" in data and "patient_zip" in data["patient_info"] and data["patient_info"]["patient_zip"]:
        if not ZIP_PATTERN.match(data["patient_info"]["patient_zip"]):
            data["patient_info"]["patient_zip"] = re.sub(r'\D', '', data["patient_info"]["patient_zip"])[:5]
            if not ZIP_PATTERN.match(data["patient_info"]["patient_zip"]):
                print(f"Warning: Invalid ZIP format for {data['patient_info'].get('patient_name', 'Unknown')}")
    
    # Clean and validate currency amounts
    if "billing_info" in data and "total_charge" in data["billing_info"]:
        total_charge = clean_currency(data["billing_info"]["total_charge"])
        if not total_charge:
            return False, "Invalid total charge format"
        data["billing_info"]["total_charge"] = total_charge
    
    # Clean and validate service line charges
    valid_charges = []
    for service in data["service_lines"]:
        if "charge_amount" in service:
            charge = clean_currency(service["charge_amount"])
            if charge:
                service["charge_amount"] = charge
                valid_charges.append(float(charge.replace('$', '')))
            else:
                return False, f"Invalid charge amount format in service line"
        
        # Clean and validate dates
        if "date_of_service" in service:
            dates = service["date_of_service"].split(" - ")
            parsed_dates = []
            for date in dates:
                parsed = parse_date(date)
                if parsed:
                    parsed_dates.append(parsed)
            if parsed_dates:
                service["date_of_service"] = " - ".join(parsed_dates)
            else:
                return False, "Invalid Date of Service format"
    
    # Validate total matches sum of line items (if we have both)
    if valid_charges and "billing_info" in data and "total_charge" in data["billing_info"]:
        total = float(data["billing_info"]["total_charge"].replace('$', ''))
        sum_charges = sum(valid_charges)
        if abs(total - sum_charges) > 5:  # Allow $5 tolerance
            print(f"Warning: Sum of charges (${sum_charges:.2f}) differs from total (${total:.2f})")
    
    return True, "Valid JSON"

def process_validation_s3(limit=None):
    """Process JSON files from S3, validate them, and move to appropriate locations."""
    print(f"Starting validation run against bucket: {S3_BUCKET} (prefix: {INPUT_PREFIX})")
    
    # Get list of JSON files to process - only from root hcfa_json directory
    all_keys = list_objects(INPUT_PREFIX)
    json_keys = [k for k in all_keys if k.lower().endswith('.json') 
                and k.count('/') == 2  # Only process files in root hcfa_json directory
                and not any(x in k for x in ['valid', 'invalid', 'garbage', 'processed'])]  # Skip already processed files
    
    if limit:
        json_keys = json_keys[:int(limit)]

    for key in json_keys:
        print(f"→ Processing s3://{S3_BUCKET}/{key}")
        local_json = download(key, os.path.join(tempfile.gettempdir(), os.path.basename(key)))
        
        try:
            # Load and validate JSON
            with open(local_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Store original name for comparison
            original_name = data.get("patient_info", {}).get("patient_name", "N/A")
            
            is_valid, message = validate_json(data)
            
            # Write back cleaned/standardized data
            with open(local_json, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            
            if is_valid:
                # Move to valid directory
                dest_key = f"data/hcfa_json/valid/{os.path.basename(key)}"
                upload(local_json, dest_key)
                print(f"✔ Valid JSON moved to s3://{S3_BUCKET}/{dest_key}")
                
                # Delete original after successful upload
                move(key, dest_key)
                
                # Log name changes if any
                new_name = data.get("patient_info", {}).get("patient_name", "N/A")
                if new_name != original_name:
                    log_msg = f"Name standardized in {key}: {original_name} -> {new_name}"
                    log_local = tempfile.mktemp(suffix='.log')
                    with open(log_local, 'w', encoding='utf-8') as logf:
                        logf.write(log_msg + '\n')
                    upload(log_local, LOG_PREFIX)
                    os.remove(log_local)
            else:
                # Move invalid files to invalid directory
                dest_key = f"data/hcfa_json/invalid/{os.path.basename(key)}"
                move(key, dest_key)
                print(f"❌ Invalid JSON moved to s3://{S3_BUCKET}/{dest_key}")
                
                # Log validation error
                log_msg = f"Validation failed for {key}: {message}"
                log_local = tempfile.mktemp(suffix='.log')
                with open(log_local, 'w', encoding='utf-8') as logf:
                    logf.write(log_msg + '\n')
                upload(log_local, LOG_PREFIX)
                os.remove(log_local)
                
        except Exception as e:
            err = f"❌ Error processing {key}: {str(e)}"
            print(err)
            log_local = tempfile.mktemp(suffix='.log')
            with open(log_local, 'w', encoding='utf-8') as logf:
                logf.write(err + '\n')
            upload(log_local, LOG_PREFIX)
            os.remove(log_local)
            
        finally:
            # Clean up temporary files
            if os.path.exists(local_json):
                os.remove(local_json)

    print("Validation run complete.")

if __name__ == '__main__':
    process_validation_s3()