import os
from pathlib import Path

# Base paths
BASE_PATH = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\Documents\Bill_Review_INTERNAL\EOBR"
INPUT_JSON_PATH = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\Documents\Bill_Review_INTERNAL\validation logs\validation_passes_20250323_171406.json"
JSON_DIR_PATH = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\Documents\Bill_Review_INTERNAL\scripts\VAILIDATION\data\extracts\valid\mapped\staging\success"
DB_PATH = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\Documents\Bill_Review_INTERNAL\reference_tables\orders2.db"
WORD_TEMPLATE = os.path.join(BASE_PATH, "EOBR Template.docx")
HISTORICAL_EXCEL_PATH = os.path.join(BASE_PATH, "Historical_EOBR_Data.xlsx")

# Excel headers
EXCEL_HEADERS = [
    "Release Payment", "Duplicate Check", "Full Duplicate Key", "Input File", "EOBR Number", "Vendor",
    "Mailing Address", "Terms", "Bill Date", "Due Date", "Category", "Description", "Amount", "Memo", "Total"
]

# Acceptable values
ACCEPTABLE_MODIFIERS = {"26", "25", "TC", "RT", "LT", "59"}
ACCEPTABLE_POS = {"49", "11"}

# Business rules
DEFAULT_TERMS = "Net 45"
DEFAULT_CATEGORY = "Subcontracted Services:Provider Services"