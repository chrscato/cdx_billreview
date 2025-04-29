# EOBR Generation System

This system processes validation data and generates Explanation of Benefits Reports (EOBRs) in Word and PDF formats, along with Excel records for tracking.

## Directory Structure

```
bill_review/
├── config/
│   └── settings.py           # Constants and configuration values
├── data/
│   └── excel_manager.py      # Excel read/write operations
├── processors/
│   ├── document_processor.py # Word document generation
│   └── eobr_processor.py     # EOBR data processing
├── utils/
│   ├── formatters.py         # Date and text formatting
│   └── validators.py         # Data validation
└── main.py                   # Main script entry point
```

## Setup & Usage

1. Install required packages:
   ```
   pip install python-docx docx2pdf openpyxl python-dateutil holidays
   ```

2. Update path configurations in `config/settings.py` as needed.

3. Run the system:
   ```
   python main.py
   ```

## Main Features

- Processes JSON validation data
- Checks for duplicates against historical records
- Generates Word and PDF documents from template
- Maintains Excel records of all EOBRs processed
- Uses a modular structure for maintainability

## Module Responsibilities

- **config/settings.py**: Contains all configuration constants
- **data/excel_manager.py**: Handles Excel file operations
- **processors/document_processor.py**: Creates Word documents and PDF files
- **processors/eobr_processor.py**: Processes EOBR data and creates metadata
- **utils/formatters.py**: Handles date and currency formatting
- **utils/validators.py**: Validates input records before processing
- **main.py**: Orchestrates the entire process

## Customization

To customize the description format or other fields, modify the appropriate function in `processors/eobr_processor.py`.