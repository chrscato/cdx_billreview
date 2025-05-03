import os
import sys
import logging
from typing import List, Tuple, Optional, Dict, Any

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.s3_utils import list_objects, move_with_confirmation

# Set up logging
logger = logging.getLogger(__name__)

class FileMover:
    def __init__(self, source_dir: str, dest_dir: str):
        """
        Initialize the FileMover with source and destination directories.
        
        Args:
            source_dir (str): Source directory path
            dest_dir (str): Destination directory path
        """
        self.source_dir = source_dir.rstrip('/')
        self.dest_dir = dest_dir.rstrip('/')
        
    def move_files(self, file_list: List[str], verify_only_basename: bool = True) -> Dict[str, Any]:
        """
        Move files from source to destination directory.
        
        Args:
            file_list (List[str]): List of files to move (can be basenames or full paths)
            verify_only_basename (bool): If True, matches only file basenames against source files
            
        Returns:
            Dict containing:
                - moved_files: List of successfully moved files
                - failed_files: List of (file, error) tuples for failed moves
                - summary: Dict with count statistics
        """
        # Get all files in the source directory
        source_files = list_objects(self.source_dir)
        
        # Filter for files that are in our list
        if verify_only_basename:
            files_to_process = [f for f in source_files 
                              if os.path.basename(f) in [os.path.basename(x) for x in file_list]]
        else:
            files_to_process = [f for f in source_files if f in file_list]
        
        moved_files = []
        failed_files = []
        
        for file_key in files_to_process:
            try:
                # Construct destination key
                dest_key = file_key.replace(self.source_dir, self.dest_dir)
                
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
        
        # Prepare summary
        summary = {
            'total_requested': len(file_list),
            'total_found': len(files_to_process),
            'successfully_moved': len(moved_files),
            'failed_moves': len(failed_files)
        }
        
        # Log summary
        logger.info("\nMove Operation Summary:")
        logger.info(f"Total files requested: {summary['total_requested']}")
        logger.info(f"Files found in source: {summary['total_found']}")
        logger.info(f"Successfully moved: {summary['successfully_moved']}")
        logger.info(f"Failed to move: {summary['failed_moves']}")
        
        if failed_files:
            logger.info("\nFailed files:")
            for f, error in failed_files:
                logger.error(f"  - {f}: {error}")
        
        return {
            'moved_files': moved_files,
            'failed_files': failed_files,
            'summary': summary
        }
    
    def move_files_from_csv(self, csv_path: str) -> Dict[str, Any]:
        """
        Move files listed in a CSV file.
        
        Args:
            csv_path (str): Path to CSV file containing list of files to move
            
        Returns:
            Same as move_files()
        """
        try:
            with open(csv_path, 'r') as f:
                files_to_move = [line.strip() for line in f if line.strip()]
            
            return self.move_files(files_to_move)
            
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_path}: {str(e)}")
            return {
                'moved_files': [],
                'failed_files': [],
                'summary': {
                    'total_requested': 0,
                    'total_found': 0,
                    'successfully_moved': 0,
                    'failed_moves': 0,
                    'error': str(e)
                }
            } 