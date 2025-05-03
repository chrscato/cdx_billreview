import boto3
import json
import os
import glob
from pathlib import Path
from datetime import datetime
import pandas as pd

# Import from modules
from config.settings import BASE_PATH, HISTORICAL_EXCEL_PATH
from utils.validators import validate_record
from data.excel_manager import initialize_excel_file, load_historical_duplicates, append_to_excel
from processors.document_processor import generate_document
from processors.eobr_processor import collect_additional_eobr_data
from data.db_manager import check_if_item_paid, update_payment_info, list_line_items, check_if_order_has_payments
from data.db_logger import db_logger

def setup_folder_structure():
    """Create folder structure for current run"""
    # This function remains unchanged
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_folder = os.path.join(BASE_PATH, current_date)
    
    folder_structure = {
        'root': run_folder,
        'docs': os.path.join(run_folder, 'docs'),
        'pdf': os.path.join(run_folder, 'pdf'),
        'excel': os.path.join(run_folder, 'excel'),
    }
    
    for path in folder_structure.values():
        Path(path).mkdir(parents=True, exist_ok=True)
        
    folder_structure['current_excel'] = os.path.join(folder_structure['excel'], f"EOBR_Data_{current_date}.xlsx")
    folder_structure['db_updates_excel'] = os.path.join(folder_structure['excel'], f"Database_Updates_{current_date}.xlsx")
    return folder_structure

def adapt_record_format(record, filename):
    """
    Adapt the S3 JSON format to match what all processors expect
    Based on the exact structure of the sample JSON provided
    """
    # Extract the order ID for debugging
    order_id = record.get("filemaker", {}).get("order", {}).get("Order_ID")
    print(f"  [DEBUG] Processing {filename} with Order ID: {order_id}")
    
    # Extract billing info
    billing_info = record.get("billing_info", {})
    
    # Get the properly formatted provider info
    fm_provider = record.get("filemaker", {}).get("provider", {})
    fm_order = record.get("filemaker", {}).get("order", {})
    
    # Service lines from top level
    service_lines = record.get("service_lines", [])
    
    # Setup provider info structured specifically as needed by validators
    provider_info = {
        # These specific fields are required by validators.py
        "Billing_Name": fm_provider.get("Billing Name", ""),
        "TIN": fm_provider.get("TIN", ""),
        "NPI": fm_provider.get("NPI", ""),
    }
    
    # Create the billing address structure
    # Split the combined address if needed
    billing_address_str = billing_info.get("billing_provider_address", "")
    address_parts = billing_address_str.split(",")
    
    provider_info["Billing_Address"] = {
        "Address": fm_provider.get("Billing Address 1", address_parts[0] if address_parts else ""),
        "City": fm_provider.get("Billing Address City", address_parts[1].strip() if len(address_parts) > 1 else ""),
        "State": fm_provider.get("Billing Address State", address_parts[2].split()[0] if len(address_parts) > 2 else ""),
        "Postal_Code": fm_provider.get("Billing Address Postal Code", address_parts[2].split()[1] if len(address_parts) > 2 and len(address_parts[2].split()) > 1 else ""),
    }
    
    # Create the adapted record in the format expected by processors
    adapted_record = {
        "file_info": {
            "file_name": filename,
            "order_id": order_id,
            "timestamp": datetime.now().isoformat(),
        },
        "validation_summary": {
            "status": "PASS",
            "total_checks": len(service_lines),
            "failed_checks": 0
        },
        "data": {
            "patient_info": {
                # Map the patient info fields required by validators
                "Order_ID": order_id,
                "FileMaker_Record_Number": record.get("mapping_info", {}).get("filemaker_number", ""),
                "PatientName": fm_order.get("PatientName", ""),
                "Patient_DOB": fm_order.get("Patient_DOB", ""),
                "Patient_Injury_Date": fm_order.get("Patient_Injury_Date", ""),
                "Claim_Number": fm_order.get("Claim_Number", ""),
                # Include all the filemaker order data
                **fm_order
            },
            "provider_info": provider_info,
            "date_of_service": next((line.get("date_of_service") for line in service_lines), None),
            "line_items": []
        },
        "order_id": order_id
    }
    
    # Add line items from service_lines
    for service_line in service_lines:
        cpt_code = service_line.get("cpt_code")
        
        # Look for matching line item in filemaker data to get the ID
        matching_line_item = None
        for line_item in record.get("filemaker", {}).get("line_items", []):
            if line_item.get("CPT") == cpt_code:
                matching_line_item = line_item
                break
        
        # Create the line item in the expected format
        new_line_item = {
            "date_of_service": service_line.get("date_of_service"),
            "cpt": cpt_code,
            "modifier": ",".join(service_line.get("modifiers", [])) if service_line.get("modifiers") else None,
            "units": service_line.get("units", 1),
            "charge": service_line.get("charge_amount"),
            "validated_rate": service_line.get("assigned_rate"),
            "description": service_line.get("proc_desc", ""),
            "pos": service_line.get("place_of_service", "11")
        }
        
        # Add line item ID if available
        if matching_line_item and matching_line_item.get("id"):
            new_line_item["payment_id"] = {"line_item_id": matching_line_item.get("id")}
        
        adapted_record["data"]["line_items"].append(new_line_item)
    
    return adapted_record

def debug_validate_record(record):
    """
    Debug version of validate_record that prints which check failed
    """
    if "data" not in record:
        print("  [DEBUG] Missing 'data' key in record.")
        return False
    data = record.get("data", {})
    line_items = data.get("line_items", [])
    if not line_items:
        print("  [DEBUG] No line items found in record['data']['line_items'].")
        return False
    for idx, line in enumerate(line_items):
        if line.get("validated_rate") is None:
            print(f"  [DEBUG] Line item {idx} missing 'validated_rate'. Line: {line}")
            return False
    if not data.get("date_of_service"):
        has_date = any(line.get("date_of_service") for line in line_items)
        if not has_date:
            print("  [DEBUG] No 'date_of_service' found in record or any line item.")
            return False
    patient_info = data.get("patient_info", {})
    if not patient_info.get("PatientName"):
        print(f"  [DEBUG] Missing 'PatientName' in patient_info: {patient_info}")
        return False
    provider_info = data.get("provider_info", {})
    if not provider_info.get("Billing_Name"):
        print(f"  [DEBUG] Missing 'Billing_Name' in provider_info: {provider_info}")
        return False
    return True

def process_s3_json_files(bucket_name, prefix):
    """Process JSON files from S3 bucket and generate EOBR reports"""
    # Setup
    folders = setup_folder_structure()
    initialize_excel_file(folders['current_excel'])
    initialize_excel_file(HISTORICAL_EXCEL_PATH)
    historical_duplicates, processed_control_numbers = load_historical_duplicates()
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # List objects in the bucket with the given prefix
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    except Exception as e:
        print(f"Error connecting to S3 bucket: {e}")
        return
    
    # Check if there are any objects
    if 'Contents' not in response:
        print(f"No objects found in bucket {bucket_name} with prefix {prefix}")
        return
    
    json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
    print(f"Found {len(json_files)} JSON files to process in S3 bucket.")
    
    processed_count = 0
    skipped_count = 0
    processed_order_ids = set()  # Track processed order IDs
    
    # Track database updates
    db_updates = []
    
    for json_key in json_files:
        filename = os.path.basename(json_key)
        try:
            # Get the object from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=json_key)
            file_content = response['Body'].read().decode('utf-8')
            
            # Parse JSON content
            record = json.loads(file_content)
            
            # Check validation status - looking for PASS in rate_check_info 
            # or moved_to_readyforprocess = true
            validation_passed = False
            if record.get("rate_check_info", {}).get("status") == "PASS":
                validation_passed = True
            elif record.get("processing_info", {}).get("moved_to_readyforprocess") == True:
                validation_passed = True
            
            if not validation_passed:
                print(f"Skipping file {filename}: Validation checks did not pass.")
                print(f"  [DEBUG] rate_check_info.status: {record.get('rate_check_info', {}).get('status')}")
                print(f"  [DEBUG] processing_info.moved_to_readyforprocess: {record.get('processing_info', {}).get('moved_to_readyforprocess')}")
                skipped_count += 1
                continue
            
            # Adapt record to expected format
            adapted_record = adapt_record_format(record, filename)
            
            # Check if order has any payments
            order_id = adapted_record.get("order_id")
            if order_id and check_if_order_has_payments(order_id):
                print(f"Skipping file {filename}: Order {order_id} has already been paid.")
                skipped_count += 1
                continue
            
            # Validate record structure - using your existing validator
            if not validate_record(adapted_record):
                print(f"Skipping file {filename}: Record validation failed. [Order ID: {order_id}]")
                debug_validate_record(adapted_record)
                skipped_count += 1
                continue
            
            # Process the record using your existing processor
            try:
                eobr_data = collect_additional_eobr_data(
                    adapted_record, {}, historical_duplicates, processed_control_numbers
                )
                
                # Save to Excel (local)
                append_to_excel(folders['current_excel'], eobr_data)
                append_to_excel(HISTORICAL_EXCEL_PATH, eobr_data)
                
                # Generate documents (local)
                docx_path, pdf_path = generate_document(adapted_record, eobr_data, folders)
                processed_count += 1
                print(f"Generated EOBR {eobr_data['EOBR Number']}")
                
                # Update database with payment information (local)
                updated_items = update_database_with_payment(adapted_record, eobr_data)
                if updated_items:
                    db_updates.extend(updated_items)
                
                # Track processed order ID
                if order_id:
                    processed_order_ids.add(order_id)
                
            except Exception as e:
                import traceback
                print(f"Error processing record for {filename}: {e}")
                print(traceback.format_exc())
                skipped_count += 1
                
        except Exception as e:
            import traceback
            print(f"Error reading or parsing file {filename}: {e}")
            print(traceback.format_exc())
            skipped_count += 1
    
    print(f"Processing complete. Processed: {processed_count}, Skipped: {skipped_count}")
    
    # Save database updates to Excel (local)
    if db_updates:
        df = pd.DataFrame(db_updates)
        df.to_excel(folders['db_updates_excel'], index=False)
        print(f"\nSaved database updates to: {folders['db_updates_excel']}")
    
    # Save database interaction log to Excel (local)
    db_log_path = os.path.join(folders['excel'], f"db_interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    db_logger.save_to_excel(db_log_path)
    print(f"Database interactions log saved to: {db_log_path}")
    
    # Verify database updates (local)
    print("\nVerifying database updates:")
    for order_id in processed_order_ids:
        list_line_items(order_id)

def update_database_with_payment(record, eobr_data):
    """Update database with payment information for each line item"""
    order_id = record.get("order_id")
    eobr_number = eobr_data.get("EOBR Number")
    processed_date = datetime.now().strftime("%Y-%m-%d")
    
    updated_items = []
    for line in record.get("data", {}).get("line_items", []):
        line_item_id = line.get("payment_id", {}).get("line_item_id")
        
        if line_item_id and order_id:
            success = update_payment_info(
                line_item_id=line_item_id,
                order_id=order_id,
                br_paid=str(line.get("validated_rate", 0)),
                br_rate=float(line.get("validated_rate", 0)),
                eobr_doc_no=eobr_number,
                hcfa_doc_no=eobr_number,
                br_date_processed=processed_date
            )
            
            if success:
                updated_items.append({
                    'Line_Item_ID': line_item_id,
                    'Order_ID': order_id,
                    'CPT': line.get('cpt'),
                    'BR_Paid': line.get("validated_rate", 0),
                    'BR_Rate': line.get("validated_rate", 0),
                    'EOBR_Doc_No': eobr_number,
                    'Date_Processed': processed_date
                })
    
    return updated_items

if __name__ == "__main__":
    # S3 bucket information - update these values for your S3 bucket
    S3_BUCKET_NAME = "bill-review-prod"
    S3_PREFIX = "data/hcfa_json/EOBR_ready/"
    
    # Process files from S3 bucket
    process_s3_json_files(S3_BUCKET_NAME, S3_PREFIX)