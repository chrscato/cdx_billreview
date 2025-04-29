import os
from pathlib import Path
from docx import Document
from datetime import datetime
from config.settings import WORD_TEMPLATE, ACCEPTABLE_MODIFIERS, ACCEPTABLE_POS

def process_line_items(line_items):
    """Process line items for document placeholders"""
    mapping = {}
    for i, line in enumerate(line_items[:6], start=1):
        # Get modifier, handling both string format and list format
        modifier_raw = line.get("modifier")
        if isinstance(modifier_raw, list):
            modifier = ",".join([m for m in modifier_raw if m in ACCEPTABLE_MODIFIERS])
        elif isinstance(modifier_raw, str):
            modifier = modifier_raw if modifier_raw in ACCEPTABLE_MODIFIERS else ""
        else:
            modifier = ""
        
        pos = line.get("pos", "11")  # Default POS
        units = line.get("units", 1)
        charge = float(line.get("charge", 0))
        rate = float(line.get("validated_rate", 0))
        
        mapping.update({
            f"<dos{i}>": line.get("date_of_service", ""),
            f"<cpt{i}>": line.get("cpt", "N/A"),
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
    for i in range(len(line_items) + 1, 7):
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
    """Generate Word document for an EOBR record"""
    # Get the data object from the record
    data = record.get("data", {})
    
    # Basic document info
    mapping = {
        "<process_date>": datetime.now().strftime("%Y-%m-%d"),
        "<PatientName>": data.get("patient_info", {}).get("PatientName", "N/A"),
        "<dob>": data.get("patient_info", {}).get("Patient_DOB", ""),
        "<doi>": data.get("patient_info", {}).get("Patient_Injury_Date", ""),
        "<provider_ref>": data.get("patient_info", {}).get("Claim_Number", "N/A"),
        "<order_no>": record.get("order_id", "N/A") if data.get("patient_info", {}).get("FileMaker_Record_Number", "").endswith("00000") else data.get("patient_info", {}).get("FileMaker_Record_Number", "N/A"),
        "<billing_name>": data.get("provider_info", {}).get("Billing_Name", "N/A"),
        "<billing_address1>": data.get("provider_info", {}).get("Billing_Address", {}).get("Address", "N/A"),
        "<billing_address2>": "",  # Not present in new format
        "<billing_city>": data.get("provider_info", {}).get("Billing_Address", {}).get("City", "N/A"),
        "<billing_state>": data.get("provider_info", {}).get("Billing_Address", {}).get("State", "N/A"),
        "<billing_zip>": data.get("provider_info", {}).get("Billing_Address", {}).get("Postal_Code", "N/A"),
        "<TIN>": data.get("provider_info", {}).get("TIN", "N/A"),
        "<NPI>": data.get("provider_info", {}).get("NPI", "N/A"),
        "<total_paid>": "${:,.2f}".format(sum(float(item.get('validated_rate', 0)) for item in data.get("line_items", []))),
    }
    
    # Add line item details
    mapping.update(process_line_items(data.get("line_items", [])))
    
    # Create document
    doc = Document(WORD_TEMPLATE)
    populate_placeholders(doc, mapping)
    
    # Save document
    eobr_file_name = f"EOBR_{eobr_data['EOBR Number']}"
    docx_output = os.path.join(output_folders['docs'], f"{eobr_file_name}.docx")
    
    doc.save(docx_output)
    return docx_output, None  # Return None for pdf_path since we're not generating PDFs