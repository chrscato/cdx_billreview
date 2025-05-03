import os
import csv
import sys
import logging

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from utils.s3_utils import list_objects, move_with_confirmation

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def move_processed_files():
    # Read the CSV file
    csv_path = os.path.join(project_root, 'tests', 'files_to_move.csv')
    with open(csv_path, 'r') as f:
        files_to_move = [line.strip() for line in f if line.strip()]
    
    # Source and destination paths
    source_dir = 'data/hcfa_json/EOBR_ready/'
    dest_dir = 'data/hcfa_json/processed/'
    
    # Get all files in the source directory
    source_files = list_objects(source_dir)
    
    # Filter for files that are in our CSV list
    files_to_process = [f for f in source_files if os.path.basename(f) in files_to_move]
    
    # Move each file
    moved_files = []
    failed_files = []
    
    for file_key in files_to_process:
        try:
            # Construct destination key
            dest_key = file_key.replace(source_dir, dest_dir)
            
            # Move the file with confirmation
            success, result = move_with_confirmation(file_key, dest_key)
            
            if success:
                moved_files.append(file_key)
                logger.info(f"Moved: {file_key} -> {dest_key}")
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Failed to move {file_key}: {error_msg}")
                failed_files.append((file_key, error_msg))
            
        except Exception as e:
            logger.error(f"Failed to move {file_key}: {str(e)}")
            failed_files.append((file_key, str(e)))
    
    # Print summary
    logger.info("\nSummary:")
    logger.info(f"Total files to move: {len(files_to_move)}")
    logger.info(f"Successfully moved: {len(moved_files)}")
    logger.info(f"Failed to move: {len(failed_files)}")
    
    if failed_files:
        logger.info("\nFailed files:")
        for f, error in failed_files:
            logger.error(f"  - {f}: {error}")

if __name__ == "__main__":
    move_processed_files() 