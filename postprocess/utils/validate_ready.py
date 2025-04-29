import os
import sys
import json
import logging
import sqlite3
import argparse
import copy
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
import random

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, delete
from process.utils.filter_ancillaries import load_ancillary_cpts

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Directories
READY_DIR = 'data/hcfa_json/readyforprocess/'
EOBR_READY_DIR = 'data/hcfa_json/EOBR_ready/'

# Database configuration
PROC_DB_PATH = os.getenv("PROC_DB_PATH", "filemaker.db")

# Load ancillary CPTs once
ANCILLARY_CPTS: Set[str] = set(load_ancillary_cpts())

def extract_modifier(modifiers: List[str]) -> Optional[str]:
    """Extract 26 or TC modifier if present, otherwise return None."""
    if not modifiers:
        return None
    for mod in modifiers:
        if mod in ['26', 'TC']:
            return mod
    return None

def lookup_ppo_rate(cpt: str, tin: str, modifier: Optional[str]) -> Optional[float]:
    """Look up rate in PPO table for in-network providers."""
    if cpt in ANCILLARY_CPTS:
        return 0.0
        
    conn = sqlite3.connect(PROC_DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT rate FROM ppo
        WHERE proc_cd = ? AND TIN = ? AND (modifier = ? OR (? IS NULL AND modifier IS NULL))
        LIMIT 1
    """
    cursor.execute(query, (cpt, tin, modifier, modifier))
    result = cursor.fetchone()
    conn.close()
    
    return float(result[0]) if result else None

def lookup_ota_rate(order_id: str, cpt: str, modifier: Optional[str]) -> Optional[float]:
    """Look up rate in current_otas table for out-of-network providers."""
    if cpt in ANCILLARY_CPTS:
        return 0.0
        
    conn = sqlite3.connect(PROC_DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT rate FROM current_otas
        WHERE ID_Order_PrimaryKey = ? AND CPT = ? AND (modifier = ? OR (? IS NULL AND modifier IS NULL))
        LIMIT 1
    """
    cursor.execute(query, (order_id, cpt, modifier, modifier))
    result = cursor.fetchone()
    conn.close()
    
    return float(result[0]) if result and result[0] else None

def validate_rates(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate rates for all service lines in the claim."""
    results = {
        'rate_check_passed': True,
        'missing_rates': [],
        'found_rates': {},
        'errors': [],
        'ancillary_cpts': [],
        'updated_service_lines': []
    }
    
    try:
        # Get provider network status and order ID
        provider_network = data.get('filemaker', {}).get('provider', {}).get('Provider Network')
        order_id = data.get('filemaker', {}).get('order', {}).get('Order_ID')
        provider_tin = data.get('filemaker', {}).get('provider', {}).get('TIN', '').replace('-', '')
        
        if not provider_network:
            results['errors'].append("Missing Provider Network status")
            results['rate_check_passed'] = False
            return results
            
        if not order_id and provider_network != "In Network":
            results['errors'].append("Missing Order ID for out-of-network rate check")
            results['rate_check_passed'] = False
            return results
        
        # Process each service line
        service_lines = data.get('service_lines', [])
        updated_lines = []
        
        for line in service_lines:
            updated_line = line.copy()  # Create a copy to modify
            cpt_code = line.get('cpt_code')
            if not cpt_code:
                updated_lines.append(updated_line)
                continue
                
            # Check if CPT is ancillary
            if cpt_code in ANCILLARY_CPTS:
                results['ancillary_cpts'].append(cpt_code)
                results['found_rates'][cpt_code] = {
                    'rate': 0.0,
                    'modifier': None,
                    'source': 'Ancillary'
                }
                updated_line['assigned_rate'] = 0.0
                updated_lines.append(updated_line)
                continue
                
            modifiers = line.get('modifiers', [])
            modifier = extract_modifier(modifiers)
            
            rate = None
            rate_source = None
            if provider_network == "In Network":
                if not provider_tin:
                    results['errors'].append(f"Missing provider TIN for in-network CPT {cpt_code}")
                    updated_lines.append(updated_line)
                    continue
                rate = lookup_ppo_rate(cpt_code, provider_tin, modifier)
                rate_source = 'PPO'
            else:
                rate = lookup_ota_rate(order_id, cpt_code, modifier)
                rate_source = 'OTA'
            
            if rate is not None:
                results['found_rates'][cpt_code] = {
                    'rate': rate,
                    'modifier': modifier,
                    'source': rate_source
                }
                # Add the assigned rate directly to the service line
                updated_line['assigned_rate'] = float(rate)
            else:
                results['missing_rates'].append({
                    'cpt': cpt_code,
                    'modifier': modifier,
                    'network': provider_network
                })
            
            updated_lines.append(updated_line)
        
        # Update the service lines in the data
        results['updated_service_lines'] = updated_lines
        
        results['rate_check_passed'] = len(results['missing_rates']) == 0
        
    except Exception as e:
        results['errors'].append(f"Rate validation error: {str(e)}")
        results['rate_check_passed'] = False
    
    return results


def validate_field_formats(data: Dict[str, Any]) -> List[str]:
    """
    Validate formatting of fields inside the JSON file.
    Return a list of formatting errors. If empty, formatting is valid.
    """
    errors = []

    # 1. Validate filemaker.provider TIN
    provider = data.get('filemaker', {}).get('provider', {})
    tin = provider.get('TIN', '').replace('-', '')
    if not tin.isdigit() or len(tin) != 9:
        errors.append("Invalid TIN format in filemaker.provider (must be 9 digits)")

    # 2. Validate provider billing fields
    billing_fields = [
        'Billing Address 1', 'Billing Address City', 'Billing Address State', 
        'Billing Address Postal Code', 'Billing Name'
    ]
    for field in billing_fields:
        if not provider.get(field):
            errors.append(f"Missing required billing field in provider: {field}")

    # 3. Validate patient DOB from filemaker.order
    patient_dob = data.get('filemaker', {}).get('order', {}).get('Patient_DOB')
    if patient_dob:
        dob_formats = ['%m/%d/%Y', '%m-%d-%Y', '%m/%d/%y', '%m-%d-%y', '%m %d %Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
        parsed = False
        for fmt in dob_formats:
            try:
                dt = datetime.strptime(patient_dob.strip(), fmt)
                # Normalize to YYYY-MM-DD
                data['filemaker']['order']['Patient_DOB'] = dt.strftime('%Y-%m-%d')
                parsed = True
                break
            except Exception:
                continue
        if not parsed:
            errors.append("Invalid patient DOB format in filemaker.order (cannot parse date)")

    # 4. Correct patient_account_no if 'uncertain'
    acct_no = data.get('billing_info', {}).get('patient_account_no')
    if acct_no and acct_no.strip().lower() == 'uncertain':
        data['billing_info']['patient_account_no'] = 'N/A'

    # 5. Validate each service line
    for idx, line in enumerate(data.get('service_lines', [])):
        # 5.1 Validate date_of_service
        dos = line.get('date_of_service')
        if dos:
            dos_clean = dos.split(' - ')[0].strip()  # Take first date if date range
            dos_formats = ['%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d']
            parsed = False
            for fmt in dos_formats:
                try:
                    dt = datetime.strptime(dos_clean, fmt)
                    parsed = True
                    break
                except Exception:
                    continue
            if not parsed:
                errors.append(f"Service line {idx+1}: Invalid date_of_service format ({dos})")

        # 5.2 Validate modifiers
        modifiers = line.get('modifiers', [])
        if modifiers:
            allowed_mods = {'LT', 'RT', '26', 'TC'}
            if not all(mod in allowed_mods for mod in modifiers):
                errors.append(f"Service line {idx+1}: Invalid modifier(s) {modifiers}")

        # 5.3 Validate charge_amount
        charge = line.get('charge_amount')
        if charge:
            charge_clean = str(charge).replace('$', '').replace(',', '').strip()
            try:
                value = float(charge_clean)
                if value <= 0:
                    errors.append(f"Service line {idx+1}: Charge amount not positive ({charge})")
            except Exception:
                errors.append(f"Service line {idx+1}: Invalid charge amount ({charge})")

        # 5.4 Validate units
        units = line.get('units')
        if not isinstance(units, int) or units <= 0:
            errors.append(f"Service line {idx+1}: Units must be a positive integer")

    return errors



def validate_json_structure(data: Dict[str, Any]) -> List[str]:
    """
    Validate the structure of a JSON file.
    
    Args:
        data: The JSON data to validate
        
    Returns:
        List of validation errors, empty if valid
    """
    errors = []
    
    # Check for required top-level fields
    required_fields = ['patient_info', 'service_lines', 'billing_info', 'mapping_info', 'filemaker']
    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")
    
    # Validate patient_info
    if 'patient_info' in data:
        patient_required = ['patient_name', 'patient_dob']
        for field in patient_required:
            if not data['patient_info'].get(field):
                errors.append(f"Missing or empty required patient field: {field}")
    
    # Validate service_lines
    if 'service_lines' in data:
        if not data['service_lines']:
            errors.append("Empty service_lines array")
        else:
            for i, line in enumerate(data['service_lines']):
                required = ['date_of_service', 'cpt_code', 'charge_amount', 'units']
                for field in required:
                    if not line.get(field):
                        errors.append(f"Service line {i+1} missing required field: {field}")
    
    # Validate billing_info
    if 'billing_info' in data:
        billing_required = ['billing_provider_tin', 'total_charge']
        for field in billing_required:
            if not data['billing_info'].get(field):
                errors.append(f"Missing or empty required billing field: {field}")
    
    # Validate mapping_info
    if 'mapping_info' in data:
        if not data['mapping_info'].get('order_id'):
            errors.append("Missing or empty order_id in mapping_info")
    
    return errors

def validate_ready_files(test_files: List[str] = None) -> Dict[str, Any]:
    """
    Validate files in the readyforprocess directory.
    
    Args:
        test_files: Optional list of specific files to test. If None, tests all files.
    """
    results = {
        'total_files': 0,
        'valid_files': 0,
        'invalid_files': 0,
        'file_details': {},
        'summary_errors': [],
        'moved_files': []
    }
    
    try:
        # List all files in the ready directory
        files = test_files if test_files else list_objects(READY_DIR)
        if not files:
            logger.info(f"No files found to validate")
            return results
            
        results['total_files'] = len(files)
        logger.info(f"Found {len(files)} files to validate")
        
        # Validate each file
        for file_key in files:
            # Skip if it's a directory marker
            if file_key.endswith('/'):
                continue
                
            file_results = {
                'valid': True,
                'errors': [],
                'warnings': [],
                'rate_check': None,
                'moved': False
            }
            
            try:
                # Get the file contents
                data = get_s3_json(file_key)
                original_data = copy.deepcopy(data)  # Use deep copy to ensure nested changes persist
                
                # Validate JSON structure
                validation_errors = validate_json_structure(data)
                if not validation_errors:
                    format_errors = validate_field_formats(data)
                    if format_errors:
                        validation_errors.extend(format_errors)

                if validation_errors:
                    file_results['valid'] = False
                    file_results['errors'].extend(validation_errors)
                
                # Validate rates if structure is valid
                if not validation_errors:
                    rate_results = validate_rates(data)
                    file_results['rate_check'] = rate_results
                    
                    if rate_results['rate_check_passed']:
                        # Update service lines with rate information in original data
                        original_data['service_lines'] = rate_results['updated_service_lines']
                        
                        # Add rate check timestamp
                        original_data['rate_check_info'] = {
                            'timestamp': datetime.now().isoformat(),
                            'status': 'PASS'
                        }

                        # Print the updated JSON before uploading/moving
                        logger.info(f"\nUpdated JSON for {file_key} (to be moved):\n" + json.dumps(original_data, indent=2))

                        # Move to EOBR_ready
                        target_key = file_key.replace(READY_DIR, EOBR_READY_DIR)
                        logger.info(f"Destination S3 key: {target_key}")

                        # Upload updated JSON to new location
                        upload_json_to_s3(original_data, target_key)

                        # Delete from original location after successful upload
                        delete(file_key)
                        
                        file_results['moved'] = True
                        results['moved_files'].append(file_key)
                        logger.info(f"Moved {file_key} to {target_key}")
                    else:
                        file_results['valid'] = False
                        if rate_results['missing_rates']:
                            file_results['errors'].append(
                                f"Missing rates for CPTs: {', '.join(item['cpt'] for item in rate_results['missing_rates'])}"
                            )
                        file_results['errors'].extend(rate_results['errors'])
                
                # Update counts
                if file_results['valid']:
                    results['valid_files'] += 1
                else:
                    results['invalid_files'] += 1
                
            except json.JSONDecodeError as e:
                file_results['valid'] = False
                file_results['errors'].append(f"Invalid JSON format: {str(e)}")
                results['invalid_files'] += 1
            except Exception as e:
                file_results['valid'] = False
                file_results['errors'].append(f"Error processing file: {str(e)}")
                results['invalid_files'] += 1
            
            # Store file results
            results['file_details'][file_key] = file_results
            
    except Exception as e:
        results['summary_errors'].append(f"Error during validation: {str(e)}")
        logger.error(f"Error during validation: {str(e)}")
    
    return results

def print_validation_report(results: Dict[str, Any]):
    """Print a detailed validation report."""
    logger.info("\nValidation Report")
    logger.info("=" * 50)
    logger.info(f"Total Files: {results['total_files']}")
    logger.info(f"Valid Files: {results['valid_files']}")
    logger.info(f"Invalid Files: {results['invalid_files']}")
    logger.info(f"Moved to EOBR_ready: {len(results.get('moved_files', []))}")
    
    if results['moved_files']:
        logger.info("\nMoved Files:")
        for file_key in results['moved_files']:
            logger.info(f"  - {file_key}")
    
    if results['summary_errors']:
        logger.info("\nSummary Errors:")
        for error in results['summary_errors']:
            logger.error(f"  - {error}")
    
    if results['invalid_files'] > 0:
        logger.info("\nInvalid Files Details:")
        for file_key, details in results['file_details'].items():
            if not details['valid']:
                logger.error(f"\nFile: {file_key}")
                for error in details['errors']:
                    logger.error(f"  - Error: {error}")
                
                # Print rate check details if available
                if details.get('rate_check'):
                    rate_check = details['rate_check']
                    if rate_check.get('ancillary_cpts'):
                        logger.info("\n  Ancillary CPTs (Rate = $0.00):")
                        for cpt in rate_check['ancillary_cpts']:
                            logger.info(f"    - {cpt}")
                            
                    if rate_check['missing_rates']:
                        logger.error("\n  Rate Check Failures:")
                        for missing in rate_check['missing_rates']:
                            logger.error(f"    - Missing rate for CPT {missing['cpt']}"
                                       f" (modifier: {missing['modifier'] or 'None'},"
                                       f" network: {missing['network']}")
                    
                    if rate_check.get('found_rates'):
                        logger.info("\n  Found Rates:")
                        for cpt, rate_info in rate_check['found_rates'].items():
                            logger.info(f"    - CPT {cpt}: ${rate_info['rate']:.2f}"
                                      f" (modifier: {rate_info['modifier'] or 'None'},"
                                      f" source: {rate_info['source']})")
    
    if results['valid_files'] > 0:
        logger.info("\nValid Files:")
        for file_key, details in results['file_details'].items():
            if details['valid']:
                logger.info(f"\nFile: {file_key}")
                if details.get('rate_check'):
                    rate_check = details['rate_check']
                    if rate_check.get('ancillary_cpts'):
                        logger.info("  Ancillary CPTs (Rate = $0.00):")
                        for cpt in rate_check['ancillary_cpts']:
                            logger.info(f"    - {cpt}")
                            
                    if rate_check.get('found_rates'):
                        logger.info("  Found Rates:")
                        for cpt, rate_info in rate_check['found_rates'].items():
                            if rate_info['source'] != 'Ancillary':
                                logger.info(f"    - CPT {cpt}: ${rate_info['rate']:.2f}"
                                          f" (modifier: {rate_info['modifier'] or 'None'},"
                                          f" source: {rate_info['source']})")

# ADDITION: print_validation_report
def print_validation_report(results: Dict[str, Any]):
    logger.info("\nValidation Report")
    logger.info("=" * 50)
    logger.info(f"Total Files: {results['total_files']}")
    logger.info(f"Valid Files: {results['valid_files']}")
    logger.info(f"Invalid Files: {results['invalid_files']}")
    logger.info(f"Moved to EOBR_ready: {len(results.get('moved_files', []))}")

    if results['moved_files']:
        logger.info("\nMoved Files:")
        for file_key in results['moved_files']:
            logger.info(f"  - {file_key}")

    if results['summary_errors']:
        logger.info("\nSummary Errors:")
        for error in results['summary_errors']:
            logger.error(f"  - {error}")

    if results['invalid_files'] > 0:
        logger.info("\nInvalid Files Details:")
        for file_key, details in results['file_details'].items():
            if not details['valid']:
                logger.error(f"\nFile: {file_key}")
                for error in details['errors']:
                    logger.error(f"  - Error: {error}")

                if details.get('rate_check'):
                    rate_check = details['rate_check']
                    if rate_check.get('ancillary_cpts'):
                        logger.info("\n  Ancillary CPTs (Rate = $0.00):")
                        for cpt in rate_check['ancillary_cpts']:
                            logger.info(f"    - {cpt}")

                    if rate_check['missing_rates']:
                        logger.error("\n  Rate Check Failures:")
                        for missing in rate_check['missing_rates']:
                            logger.error(f"    - Missing rate for CPT {missing['cpt']} (modifier: {missing['modifier'] or 'None'}, network: {missing['network']})")

                    if rate_check.get('found_rates'):
                        logger.info("\n  Found Rates:")
                        for cpt, rate_info in rate_check['found_rates'].items():
                            logger.info(f"    - CPT {cpt}: ${rate_info['rate']:.2f} (modifier: {rate_info['modifier'] or 'None'}, source: {rate_info['source']})")

    if results['valid_files'] > 0:
        logger.info("\nValid Files:")
        for file_key, details in results['file_details'].items():
            if details['valid']:
                logger.info(f"\nFile: {file_key}")
                if details.get('rate_check'):
                    rate_check = details['rate_check']
                    if rate_check.get('ancillary_cpts'):
                        logger.info("  Ancillary CPTs (Rate = $0.00):")
                        for cpt in rate_check['ancillary_cpts']:
                            logger.info(f"    - {cpt}")

                    if rate_check.get('found_rates'):
                        logger.info("  Found Rates:")
                        for cpt, rate_info in rate_check['found_rates'].items():
                            if rate_info['source'] != 'Ancillary':
                                logger.info(f"    - CPT {cpt}: ${rate_info['rate']:.2f} (modifier: {rate_info['modifier'] or 'None'}, source: {rate_info['source']})")

# ADDITION: main entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Validate files in readyforprocess directory')
    parser.add_argument('files', nargs='*', help='Specific files to test. If not provided, tests all files.')
    parser.add_argument('--random', '-r', type=int, default=None, help='Validate N random files from readyforprocess.')
    args = parser.parse_args()

    if args.random:
        all_files = list_objects(READY_DIR)
        files = random.sample(all_files, min(args.random, len(all_files)))
        logger.info(f"Randomly selected {len(files)} files for validation.")
    elif args.files:
        files = args.files
    else:
        files = None

    logger.info("Starting validation of readyforprocess files...")
    results = validate_ready_files(files)
    print_validation_report(results)
