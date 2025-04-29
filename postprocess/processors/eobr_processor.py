import os
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse

from utils.formatters import format_date_for_eob, calculate_due_date

def collect_additional_eobr_data(record, mapping, historical_duplicates, processed_control_numbers):
    """
    Collect additional data for EOBR record (new JSON format)
    Returns a dictionary with all fields needed for Excel
    """
    # Extract base file name
    full_path = record.get("file_info", {}).get("file_name", "Unknown")
    base_filename = Path(full_path).name if full_path else "Unknown.json"

    # Extract new JSON structure
    filemaker = record.get("filemaker", {})
    filemaker_order = filemaker.get("order", {})
    filemaker_provider = filemaker.get("provider", {})
    billing_info = record.get("billing_info", {})
    service_lines = record.get("service_lines", [])

    # Process dates with detailed error handling
    date_of_service = None
    if service_lines and service_lines[0].get("date_of_service"):
        date_of_service = service_lines[0].get("date_of_service")
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
    control_number = filemaker_order.get("FileMaker_Record_Number", "N/A")
    if control_number.endswith("00000"):
        control_number = filemaker_order.get("Order_ID", control_number)
    eobr_serial = processed_control_numbers.get(control_number, 0) + 1
    eobr_number = f"{control_number}-{eobr_serial}"
    processed_control_numbers[control_number] = eobr_serial

    # Provider info
    billing_name = filemaker_provider.get("Billing Name", "N/A")
    billing_address1 = filemaker_provider.get("Billing Address 1", "N/A")
    billing_city = filemaker_provider.get("Billing Address City", "N/A")
    billing_state = filemaker_provider.get("Billing Address State", "N/A")
    billing_zip = filemaker_provider.get("Billing Address Postal Code", "N/A")
    mailing_address = f"{billing_address1}, {billing_city}, {billing_state} {billing_zip}"

    # CPT codes and duplicate check
    cpt_list = [line.get('cpt_code') for line in service_lines if line.get('cpt_code')]
    duplicate_key = f"{control_number}|{','.join(cpt_list)}"
    is_duplicate = duplicate_key in historical_duplicates or duplicate_key in processed_control_numbers
    release_payment = "N" if is_duplicate else "Y"
    processed_control_numbers[duplicate_key] = True

    # Patient info
    patient_name = filemaker_order.get("PatientName", "N/A")

    # Description field
    description = f"{date_of_service} {','.join(cpt_list)} {patient_name} {control_number}"

    # Memo field
    memo = f"{date_of_service}, {patient_name}"

    # Amount and Total
    def safe_float(val):
        try:
            return float(str(val).replace("$", "").replace(",", ""))
        except Exception:
            return 0.0
    amount = sum(safe_float(line.get('charge_amount', 0)) for line in service_lines)
    total = sum(safe_float(line.get('assigned_rate', 0)) for line in service_lines)

    # Return data dictionary
    return {
        "EOBR Number": eobr_number,
        "Bill Date": formatted_bill_date,
        "Due Date": due_date.strftime("%m.%d.%Y"),
        "Vendor": billing_name,
        "Input File": base_filename,
        "Mailing Address": mailing_address,
        "Description": description,
        "Memo": memo,
        "Amount": "${:,.2f}".format(amount),
        "Total": "${:,.2f}".format(total),
        "Duplicate Check": "Duplicate" if is_duplicate else "Null",
        "Full Duplicate Key": duplicate_key,
        "Release Payment": release_payment,
        "Terms": "Net 45",
        "Category": "Subcontracted Services:Provider Services",
    }