import os
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse

from utils.formatters import format_date_for_eob, calculate_due_date

def collect_additional_eobr_data(record, mapping, historical_duplicates, processed_control_numbers):
    """
    Collect additional data for EOBR record
    Returns a dictionary with all fields needed for Excel
    """
    # Extract base file name
    full_path = record.get("file_info", {}).get("file_name", "Unknown")
    base_filename = Path(full_path).name if full_path else "Unknown.json"
    
    # Process dates with detailed error handling
    date_of_service = record.get("data", {}).get("date_of_service")
    if not date_of_service:
        print(f"Error: date_of_service is missing or empty in record from file: {base_filename}")
        print(f"Record contents: {record}")
        raise ValueError(f"date_of_service is missing or empty in record from file: {base_filename}")
    
    try:
        formatted_date = format_date_for_eob(date_of_service)
        print(f"Original date: {date_of_service}")
        print(f"Formatted date: {formatted_date}")
        bill_date = parse(formatted_date)
        formatted_bill_date = bill_date.strftime("%m.%d.%Y")
        due_date = calculate_due_date(bill_date)
    except Exception as e:
        print(f"Error processing date '{date_of_service}' from file: {base_filename}")
        print(f"Error details: {str(e)}")
        raise ValueError(f"Failed to process date '{date_of_service}': {str(e)}")
    
    # Get control number and generate EOBR number
    control_number = record.get("data", {}).get("patient_info", {}).get("FileMaker_Record_Number", "N/A")
    # Check if last 5 digits are 00000
    if control_number.endswith("00000"):
        control_number = record.get("order_id", control_number)
    eobr_serial = processed_control_numbers.get(control_number, 0) + 1
    eobr_number = f"{control_number}-{eobr_serial}"
    processed_control_numbers[control_number] = eobr_serial
    
    provider_info = record.get('data', {}).get('provider_info', {})
    billing_address = provider_info.get('Billing_Address', {})
    mailing_address = f"{billing_address.get('Address', 'N/A')}, " \
                    f"{billing_address.get('City', 'N/A')}, " \
                    f"{billing_address.get('State', 'N/A')} " \
                    f"{billing_address.get('Postal_Code', 'N/A')}"
    # Get CPT codes and check for duplicates
    cpt_list = [line.get('cpt') for line in record.get('data', {}).get('line_items', []) if line.get('cpt')]
    duplicate_key = f"{control_number}|{','.join(cpt_list)}"
    is_duplicate = duplicate_key in historical_duplicates or duplicate_key in processed_control_numbers
    release_payment = "N" if is_duplicate else "Y"
    processed_control_numbers[duplicate_key] = True
    
    # Create description field with DOS, CPT codes, patient name, and control number
    description = f"{date_of_service} {','.join(cpt_list)} {record.get('data', {}).get('patient_info', {}).get('PatientName', 'N/A')} {control_number}"
    
    # Return data dictionary
    return {
        "EOBR Number": eobr_number, 
        "Bill Date": formatted_bill_date, 
        "Due Date": due_date.strftime("%m.%d.%Y"),
        "Vendor": provider_info.get('Billing_Name', 'N/A'), 
        "Input File": base_filename,
        "Mailing Address": mailing_address, 
        "Description": description,
        "Memo": f"{date_of_service}, {record.get('data', {}).get('patient_info', {}).get('PatientName', 'N/A')}",
        "Amount": "${:,.2f}".format(sum(float(line.get('charge', 0)) for line in record.get('data', {}).get('line_items', []))),
        "Total": "${:,.2f}".format(sum(float(line.get('validated_rate', 0)) for line in record.get('data', {}).get('line_items', []))),
        "Duplicate Check": "Duplicate" if is_duplicate else "Null", 
        "Full Duplicate Key": duplicate_key, 
        "Release Payment": release_payment,
        "Terms": "Net 45", 
        "Category": "Subcontracted Services:Provider Services",
    }