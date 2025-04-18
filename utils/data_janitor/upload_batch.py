#!/usr/bin/env python3
"""
upload_batch.py

Uploads PDFs from local datadump folder to S3 bucket's data/batches folder.
Creates a new timestamped folder for each batch upload.
"""
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

# Import S3 helper functions
from utils.s3_utils import upload

# Load environment variables
load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')
DATADUMP_FOLDER = project_root / 'datadump'
S3_BATCH_PREFIX = 'data/batches'

def ensure_datadump_folder():
    """Create datadump folder if it doesn't exist."""
    DATADUMP_FOLDER.mkdir(exist_ok=True)
    logging.info(f"Ensured datadump folder exists at: {DATADUMP_FOLDER}")
    return DATADUMP_FOLDER

def upload_batch():
    """Upload all PDFs from datadump folder to a new batch folder in S3."""
    logger = logging.getLogger("Upload Batch")
    datadump_dir = ensure_datadump_folder()
    
    # Get list of PDF files
    pdf_files = list(datadump_dir.glob('*.pdf'))
    
    if not pdf_files:
        logger.warning("No PDF files found in datadump folder.")
        logger.info(f"Please place PDF files in: {datadump_dir}")
        return
    
    # Create timestamped batch folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_folder = f"{S3_BATCH_PREFIX}/batch_{timestamp}"
    
    logger.info(f"Uploading {len(pdf_files)} files to s3://{S3_BUCKET}/{batch_folder}/")
    
    # Upload each PDF
    for pdf_path in pdf_files:
        s3_key = f"{batch_folder}/{pdf_path.name}"
        
        try:
            upload(str(pdf_path), s3_key)
            logger.info(f"Uploaded: {pdf_path.name}")
            
            # Optionally, remove the local file after successful upload
            pdf_path.unlink()
            
        except Exception as e:
            logger.error(f"Failed to upload {pdf_path.name}: {str(e)}")
    
    logger.info("Upload complete!")
    logger.info(f"Batch folder: s3://{S3_BUCKET}/{batch_folder}/")

if __name__ == '__main__':
    # Setup basic logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    upload_batch() 