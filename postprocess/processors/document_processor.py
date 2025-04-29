import os
from pathlib import Path
from docx import Document
from datetime import datetime
from config.settings import WORD_TEMPLATE, ACCEPTABLE_MODIFIERS, ACCEPTABLE_POS

def process_line_items(service_lines):
    """Process line items for document placeholders using service_lines from new JSON format"""
    mapping = {}
    for i, line in enumerate(service_lines[:6], start=1):
        # Get modifier, handling both string format and list format
        modifier_raw = line.get("modifiers")
        if isinstance(modifier_raw, list):
            modifier = ",".join([m for m in modifier_raw if m in ACCEPTABLE_MODIFIERS])
        elif isinstance(modifier_raw, str):
            modifier = modifier_raw if modifier_raw in ACCEPTABLE_MODIFIERS else ""
        else:
            modifier = ""
        pos = line.get("place_of_service", "11")  # Default POS
        units = line.get("units", 1)
        # Remove $ and commas for charge, fallback to 0
        try:
            charge = float(str(line.get("charge_amount", "0")).replace("$", "").replace(",", ""))
        except Exception:
            charge = 0.0
        # Use assigned_rate if present, fallback to 0
        try:
            rate = float(str(line.get("assigned_rate", "0")).replace("$", "").replace(",", ""))
        except Exception:
            rate = 0.0
        mapping.update({
            f"<dos{i}>": line.get("date_of_service", ""),
            f"<cpt{i}>": line.get("cpt_code", "N/A"),
            f"<charge{i}>": "${:,.2f}".format(charge),
            f"<units{i}>": units,
            f"<modifier{i}>": modifier,
            f"<pos{i}>": pos,
            f"<rate{i}>": f"{rate:.2f}",
            f"<alwd{i}>": "${:,.2f}".format(rate),
            f"<paid{i}>": "${:,.2f}".format(rate),
            f"<code{i}>": "85, 125"
        })
    # Fill in empty values for any remaining rows
    for i in range(len(service_lines) + 1, 7):
        mapping.update({
            f"<dos{i}>": "", f"<cpt{i}>": "", f"<charge{i}>": "", f"<units{i}>": "",
            f"<modifier{i}>": "", f"<pos{i}>": "", f"<alwd{i}>": "", f"<paid{i}>": "", f"<code{i}>": ""
        })
    return mapping

def populate_placeholders(doc, mapping):
    """Replace placeholders in a Word document with values"""
    sanitized_mapping = {k: str(v) if v is not None else "" for k, v in mapping.items()}
    # Process paragraphs
    for paragraph in doc.paragraphs:
        for placeholder, value in sanitized_mapping.items():
            if placeholder in paragraph.text:
                paragraph.text = paragraph.text.replace(placeholder, value)
    # Process tables
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for placeholder, value in sanitized_mapping.items():
                    if placeholder in cell.text:
                        cell.text = cell.text.replace(placeholder, value)

def generate_document(record, eobr_data, output_folders):
    """Generate Word document for an EOBR record using new JSON format"""
    # Extract relevant sections
    filemaker = record.get("filemaker", {})
    filemaker_order = filemaker.get("order", {})
    filemaker_provider = filemaker.get("provider", {})
    billing_info = record.get("billing_info", {})
    service_lines = record.get("service_lines", [])

    # Patient info
    patient_name = filemaker_order.get("PatientName", "N/A")
    dob = filemaker_order.get("Patient_DOB", "")
    injury_date = filemaker_order.get("Patient_Injury_Date", "")

    # Provider info
    billing_name = filemaker_provider.get("Billing Name", "N/A")
    billing_address1 = filemaker_provider.get("Billing Address 1", "N/A")
    billing_city = filemaker_provider.get("Billing Address City", "N/A")
    billing_state = filemaker_provider.get("Billing Address State", "N/A")
    billing_zip = filemaker_provider.get("Billing Address Postal Code", "N/A")
    tin = filemaker_provider.get("TIN", "N/A")
    npi = filemaker_provider.get("NPI", "N/A")

    # Provider ref
    provider_ref = billing_info.get("patient_account_no", "N/A")

    # Order number logic
    order_id = filemaker_order.get("Order_ID", "")
    filemaker_record_number = filemaker_order.get("FileMaker_Record_Number", "")
    if order_id and order_id.startswith("ORD"):
        order_no = order_id
    else:
        order_no = filemaker_record_number or order_id or "N/A"

    # Total paid: sum assigned_rate from service_lines
    total_paid = 0.0
    for item in service_lines:
        try:
            total_paid += float(str(item.get("assigned_rate", 0)).replace("$", "").replace(",", ""))
        except Exception:
            continue

    mapping = {
        "<process_date>": datetime.now().strftime("%Y-%m-%d"),
        "<PatientName>": patient_name,
        "<dob>": dob,
        "<doi>": injury_date,
        "<provider_ref>": provider_ref,
        "<order_no>": order_no,
        "<billing_name>": billing_name,
        "<billing_address1>": billing_address1,
        "<billing_address2>": "",  # Not present in new format
        "<billing_city>": billing_city,
        "<billing_state>": billing_state,
        "<billing_zip>": billing_zip,
        "<TIN>": tin,
        "<NPI>": npi,
        "<total_paid>": "${:,.2f}".format(total_paid),
    }
    # Add line item details
    mapping.update(process_line_items(service_lines))
    # Create document
    doc = Document(WORD_TEMPLATE)
    populate_placeholders(doc, mapping)
    # Save document
    eobr_file_name = f"EOBR_{eobr_data['EOBR Number']}"
    docx_output = os.path.join(output_folders['docs'], f"{eobr_file_name}.docx")
    doc.save(docx_output)
    return docx_output, None  # Return None for pdf_path since we're not generating PDFs