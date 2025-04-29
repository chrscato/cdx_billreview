import os
import sys
import logging
from typing import List

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.s3_utils import list_objects, move_with_confirmation

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Source directory
SOURCE_DIR = 'data/hcfa_json/valid/mapped/staging/success/'

# Target directory
TARGET_DIR = 'data/hcfa_json/readyforprocess/'

def move_files_to_ready() -> dict:
    """
    Move files from the source directory to the readyforprocess directory.
    
    Returns:
        dict: Summary of the operation including counts and any errors
    """
    results = {
        'total_files': 0,
        'successful_moves': 0,
        'failed_moves': 0,
        'errors': []
    }
    
    try:
        # List all files in the source directory
        files = list_objects(SOURCE_DIR)
        if not files:
            logger.info(f"No files found in {SOURCE_DIR}")
            return results
            
        logger.info(f"Found {len(files)} files in {SOURCE_DIR}")
        results['total_files'] = len(files)
        
        # Move each file to the target directory
        for file_key in files:
            # Skip if it's a directory marker
            if file_key.endswith('/'):
                continue
                
            # Create the target key by replacing the source directory with the target directory
            target_key = file_key.replace(SOURCE_DIR, TARGET_DIR)
            
            try:
                success, move_result = move_with_confirmation(file_key, target_key)
                if success:
                    results['successful_moves'] += 1
                    logger.info(f"Successfully moved {file_key} to {target_key}")
                else:
                    results['failed_moves'] += 1
                    results['errors'].append({
                        'file': file_key,
                        'error': move_result.get('error', 'Unknown error')
                    })
                    logger.error(f"Failed to move {file_key}: {move_result.get('error')}")
            except Exception as e:
                results['failed_moves'] += 1
                results['errors'].append({
                    'file': file_key,
                    'error': str(e)
                })
                logger.error(f"Error moving {file_key}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error processing directory {SOURCE_DIR}: {str(e)}")
        results['errors'].append({
            'directory': SOURCE_DIR,
            'error': str(e)
        })
    
    return results

if __name__ == "__main__":
    logger.info("Starting file move operation...")
    results = move_files_to_ready()
    
    logger.info("\nMove operation summary:")
    logger.info(f"Total files found: {results['total_files']}")
    logger.info(f"Successfully moved: {results['successful_moves']}")
    logger.info(f"Failed moves: {results['failed_moves']}")
    
    if results['errors']:
        logger.info("\nErrors encountered:")
        for error in results['errors']:
            if 'file' in error:
                logger.error(f"File: {error['file']} - Error: {error['error']}")
            else:
                logger.error(f"Directory: {error['directory']} - Error: {error['error']}") 