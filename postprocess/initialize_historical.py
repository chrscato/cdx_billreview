from openpyxl import Workbook
from config.settings import EXCEL_HEADERS, HISTORICAL_EXCEL_PATH
from pathlib import Path

def initialize_historical_excel():
    """Create a new historical Excel file with headers"""
    print(f"Creating new historical Excel file at: {HISTORICAL_EXCEL_PATH}")
    
    # Create parent directory if it doesn't exist
    Path(HISTORICAL_EXCEL_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    # Create new workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Historical EOBR Data"
    
    # Add headers
    ws.append(EXCEL_HEADERS)
    
    # Save workbook
    wb.save(HISTORICAL_EXCEL_PATH)
    print("Successfully created new historical Excel file with headers")

if __name__ == "__main__":
    initialize_historical_excel() 