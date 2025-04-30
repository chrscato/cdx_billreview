import os
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse

from utils.formatters import format_date_for_eob, calculate_due_date

def collect_additional_eobr_data(record, mapping, historical_duplicates, processed_control_numbers):
    """
    Collect additional data for EOBR record (adapted record format)
    Returns a dictionary with all fields needed for Excel
    """
    # Extract base file name
    full_path = record.get("file_info", {}).get("file_name", "Unknown")
    base_filename = Path(full_path).name if full_path else "Unknown.json"

    data = record.get("data", {})
    patient_info = data.get("patient_info", {})
    provider_info = data.get("provider_info", {})
    billing_address = provider_info.get("Billing_Address", {})
    line_items = data.get("line_items", [])

    # Process dates with detailed error handling
    date_of_service = data.get("date_of_service")
    if not date_of_service and line_items:
        date_of_service = line_items[0].get("date_of_service")
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
    order_id = record.get("order_id", "N/A")
    control_number = patient_info.get("FileMaker_Record_Number", "N/A")
    if str(control_number).endswith("00000"):
        control_number = order_id or control_number
    eobr_serial = processed_control_numbers.get(control_number, 0) + 1
    eobr_number = f"{control_number}-{eobr_serial}"
    processed_control_numbers[control_number] = eobr_serial

    # Provider info
    billing_name = provider_info.get("Billing_Name", "N/A")
    billing_address1 = billing_address.get("Address", "N/A")
    billing_city = billing_address.get("City", "N/A")
    billing_state = billing_address.get("State", "N/A")
    billing_zip = billing_address.get("Postal_Code", "N/A")
    mailing_address = f"{billing_address1}, {billing_city}, {billing_state} {billing_zip}"

    # CPT codes and duplicate check
    cpt_list = [line.get('cpt') for line in line_items if line.get('cpt')]
    duplicate_key = f"{control_number}|{','.join(cpt_list)}"
    is_duplicate = duplicate_key in historical_duplicates or duplicate_key in processed_control_numbers
    release_payment = "N" if is_duplicate else "Y"
    processed_control_numbers[duplicate_key] = True

    # Patient info
    patient_name = patient_info.get("PatientName", "N/A")

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
    amount = sum(safe_float(line.get('charge', 0)) for line in line_items)
    total = sum(safe_float(line.get('validated_rate', 0)) for line in line_items)

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