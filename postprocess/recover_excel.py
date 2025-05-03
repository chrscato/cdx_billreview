import os
from pathlib import Path
from openpyxl import Workbook, load_workbook
from config.settings import EXCEL_HEADERS, HISTORICAL_EXCEL_PATH
import pandas as pd
import shutil
from datetime import datetime
import zipfile
import tempfile
import xml.etree.ElementTree as ET

def try_recover_excel(corrupted_path):
    """Try to recover data from a corrupted Excel file"""
    print(f"Attempting to recover data from: {corrupted_path}")
    
    # Create backup of corrupted file
    backup_path = str(corrupted_path) + f".recovery_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(corrupted_path, backup_path)
    print("\n" + "="*80)
    print(f"BACKUP FILE CREATED AT:")
    print(f"Full path: {backup_path}")
    print("="*80 + "\n")
    
    # Try different methods to read the file
    recovered_data = []
    
    # Method 1: Try to extract and parse XML directly
    try:
        print("\nTrying to extract and parse XML directly...")
        with tempfile.TemporaryDirectory() as temp_dir:
            # Try to extract any readable parts
            with zipfile.ZipFile(corrupted_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Look for shared strings and sheet data
            shared_strings = {}
            sheet_data = []
            
            # First, try to read shared strings
            shared_strings_path = Path(temp_dir) / 'xl' / 'sharedStrings.xml'
            if shared_strings_path.exists():
                print(f"Found shared strings at: {shared_strings_path}")
                tree = ET.parse(shared_strings_path)
                root = tree.getroot()
                for idx, si in enumerate(root.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si')):
                    text = si.find('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t')
                    if text is not None:
                        shared_strings[idx] = text.text
            
            # Then, try to read sheet data
            sheet_path = Path(temp_dir) / 'xl' / 'worksheets' / 'sheet1.xml'
            if sheet_path.exists():
                print(f"Found sheet data at: {sheet_path}")
                tree = ET.parse(sheet_path)
                root = tree.getroot()
                
                # Get all rows
                rows = root.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row')
                if rows:
                    print(f"Found {len(rows)} rows in sheet data")
                    
                    # Process each row
                    for row in rows:
                        row_data = []
                        cells = row.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c')
                        for cell in cells:
                            value = cell.find('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v')
                            if value is not None:
                                cell_value = value.text
                                # If it's a shared string reference
                                if cell.get('t') == 's':
                                    cell_value = shared_strings.get(int(cell_value), '')
                                row_data.append(cell_value)
                            else:
                                row_data.append('')
                        
                        if row_data:
                            sheet_data.append(row_data)
            
            # If we found data, convert it to the expected format
            if sheet_data:
                print(f"Successfully extracted {len(sheet_data)} rows of data")
                headers = sheet_data[0] if sheet_data else []
                for row in sheet_data[1:]:
                    row_dict = {headers[i]: value for i, value in enumerate(row)}
                    recovered_data.append(row_dict)
    
    except Exception as e:
        print(f"XML extraction method failed: {e}")
    
    # Create new Excel file with recovered data
    if recovered_data:
        print(f"\nRecovered {len(recovered_data)} rows of data")
        new_path = str(corrupted_path) + ".recovered"
        wb = Workbook()
        ws = wb.active
        ws.title = "Recovered Data"
        ws.append(EXCEL_HEADERS)
        
        for row in recovered_data:
            ws.append([
                row.get("Release Payment"), row.get("Duplicate Check"), row.get("Full Duplicate Key"),
                row.get("Input File"), row.get("EOBR Number"), row.get("Vendor"), row.get("Mailing Address"),
                row.get("Terms"), row.get("Bill Date"), row.get("Due Date"), row.get("Category"), row.get("Description"),
                row.get("Amount"), row.get("Memo"), row.get("Total"),
            ])
        
        wb.save(new_path)
        print(f"Saved recovered data to: {new_path}")
    else:
        print("\nNo data could be recovered")

if __name__ == "__main__":
    # Get the path to the corrupted file
    corrupted_path = HISTORICAL_EXCEL_PATH
    if not Path(corrupted_path).exists():
        print(f"Error: File not found at {corrupted_path}")
    else:
        try_recover_excel(corrupted_path) 