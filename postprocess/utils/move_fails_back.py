import os
import sys
import logging
from typing import List, Dict, Any
import json

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.s3_utils import list_objects, move_with_confirmation, get_s3_json, upload_json_to_s3, delete

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Directories
READY_DIR = 'data/hcfa_json/readyforprocess/'
FAILS_DIR = 'data/hcfa_json/readyforprocess/fails/'
LOCAL_DATA_DIR = os.path.join(project_root, 'data')

def move_fails_back(files: List[str] = None) -> Dict[str, Any]:
    """
    Move files from the fails directory back to readyforprocess.
    
    Args:
        files: Optional list of specific files to move. If None, moves all files.
        
    Returns:
        Dict with summary of the operation
    """
    results = {
        'total_files': 0,
        'successful_moves': 0,
        'failed_moves': 0,
        'errors': []
    }
    
    try:
        # List all files in the fails directory
        files_to_move = files if files else list_objects(FAILS_DIR)
        if not files_to_move:
            logger.info(f"No files found in {FAILS_DIR}")
            return results
            
        # Filter out the summary.json file
        files_to_move = [f for f in files_to_move if not f.endswith('summary.json')]
        
        results['total_files'] = len(files_to_move)
        logger.info(f"Found {len(files_to_move)} files to move back")
        
        # Move each file
        for file_key in files_to_move:
            # Skip if it's a directory marker
            if file_key.endswith('/'):
                continue
                
            # Create the target key by replacing the fails directory with ready directory
            target_key = file_key.replace(FAILS_DIR, READY_DIR)
            
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
        
        # If we moved all files successfully, delete the local summary.json
        if results['successful_moves'] == results['total_files']:
            try:
                summary_path = os.path.join(LOCAL_DATA_DIR, 'summary.json')
                if os.path.exists(summary_path):
                    os.remove(summary_path)
                    logger.info("Deleted local summary.json")
            except Exception as e:
                logger.warning(f"Failed to delete local summary.json: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error processing directory {FAILS_DIR}: {str(e)}")
        results['errors'].append({
            'directory': FAILS_DIR,
            'error': str(e)
        })
    
    return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Move files from fails directory back to readyforprocess')
    parser.add_argument('files', nargs='*', help='Specific files to move. If not provided, moves all files.')
    args = parser.parse_args()
    
    logger.info("Starting to move files back from fails directory...")
    results = move_fails_back(args.files)
    
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