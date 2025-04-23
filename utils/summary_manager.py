#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Summary Manager Utility

This module provides utility functions for managing the failed_summary.json file.
The file contains information about failed processing attempts for various files.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default path to the summary file
DEFAULT_SUMMARY_PATH = 'data/dashboard/failed_summary.json'


def ensure_summary_file(summary_path: str = DEFAULT_SUMMARY_PATH) -> bool:
    """
    Ensures that the summary file exists and is a valid JSON file.
    If the file doesn't exist, creates it with an empty array.
    
    Args:
        summary_path (str): Path to the summary file
        
    Returns:
        bool: True if the file exists and is valid, False otherwise
        
    Raises:
        IOError: If the directory cannot be created
        json.JSONDecodeError: If the file exists but contains invalid JSON
    """
    try:
        # Check if the directory exists, if not create it
        directory = os.path.dirname(summary_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")
        
        # Check if the file exists
        if not os.path.exists(summary_path):
            # Create a new file with an empty array
            with open(summary_path, 'w') as f:
                json.dump([], f, indent=2)
            logger.info(f"Created new summary file at {summary_path}")
            return True
        
        # Validate the existing file
        with open(summary_path, 'r') as f:
            data = json.load(f)
            if not isinstance(data, list):
                logger.error(f"Summary file {summary_path} does not contain a JSON array")
                return False
        
        return True
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in summary file {summary_path}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error ensuring summary file: {str(e)}")
        raise


def add_to_summary(filename: str, 
                  failure_types: List[str], 
                  provider: str, 
                  dos: str, 
                  age_days: int,
                  summary_path: str = DEFAULT_SUMMARY_PATH) -> bool:
    """
    Adds a new entry to the summary file.
    
    Args:
        filename (str): Name of the file that failed
        failure_types (List[str]): List of failure type strings
        provider (str): Provider name
        dos (str): Date of service in YYYY-MM-DD format
        age_days (int): Age in days
        summary_path (str): Path to the summary file
        
    Returns:
        bool: True if the entry was added successfully, False otherwise
    """
    try:
        # Ensure the summary file exists
        if not ensure_summary_file(summary_path):
            return False
        
        # Read the current summary
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        
        # Check if the entry already exists
        for entry in summary:
            if entry.get('filename') == filename:
                logger.warning(f"Entry for {filename} already exists. Use update_summary instead.")
                return False
        
        # Add the new entry
        new_entry = {
            "filename": filename,
            "failure_types": failure_types,
            "provider": provider,
            "dos": dos,
            "age_days": age_days
        }
        
        summary.append(new_entry)
        
        # Write back the updated summary
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Added entry for {filename} to summary")
        return True
    
    except Exception as e:
        logger.error(f"Error adding to summary: {str(e)}")
        return False


def remove_from_summary(filename: str, summary_path: str = DEFAULT_SUMMARY_PATH) -> bool:
    """
    Removes an entry from the summary file based on the filename.
    
    Args:
        filename (str): Name of the file to remove
        summary_path (str): Path to the summary file
        
    Returns:
        bool: True if the entry was removed, False if not found or an error occurred
    """
    try:
        # Ensure the summary file exists
        if not ensure_summary_file(summary_path):
            return False
        
        # Read the current summary
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        
        # Find and remove the entry
        initial_length = len(summary)
        summary = [entry for entry in summary if entry.get('filename') != filename]
        
        # Check if anything was removed
        if len(summary) == initial_length:
            logger.warning(f"Entry for {filename} not found in summary")
            return False
        
        # Write back the updated summary
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Removed entry for {filename} from summary")
        return True
    
    except Exception as e:
        logger.error(f"Error removing from summary: {str(e)}")
        return False


def update_summary(filename: str, 
                  updates: Dict[str, Any], 
                  summary_path: str = DEFAULT_SUMMARY_PATH) -> bool:
    """
    Updates an existing entry in the summary file.
    
    Args:
        filename (str): Name of the file to update
        updates (Dict[str, Any]): Dictionary of fields and values to update
        summary_path (str): Path to the summary file
        
    Returns:
        bool: True if the entry was updated, False if not found or an error occurred
    """
    try:
        # Ensure the summary file exists
        if not ensure_summary_file(summary_path):
            return False
        
        # Read the current summary
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        
        # Find and update the entry
        entry_found = False
        for entry in summary:
            if entry.get('filename') == filename:
                # Update the fields
                for key, value in updates.items():
                    entry[key] = value
                entry_found = True
                break
        
        if not entry_found:
            logger.warning(f"Entry for {filename} not found in summary")
            return False
        
        # Write back the updated summary
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Updated entry for {filename} in summary")
        return True
    
    except Exception as e:
        logger.error(f"Error updating summary: {str(e)}")
        return False


def get_summary_entry(filename: str, summary_path: str = DEFAULT_SUMMARY_PATH) -> Optional[Dict[str, Any]]:
    """
    Retrieves an entry from the summary file based on filename.
    
    Args:
        filename (str): Name of the file to retrieve
        summary_path (str): Path to the summary file
        
    Returns:
        Optional[Dict[str, Any]]: The entry if found, None otherwise
    """
    try:
        # Ensure the summary file exists
        if not ensure_summary_file(summary_path):
            return None
        
        # Read the current summary
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        
        # Find the entry
        for entry in summary:
            if entry.get('filename') == filename:
                return entry
        
        logger.warning(f"Entry for {filename} not found in summary")
        return None
    
    except Exception as e:
        logger.error(f"Error getting summary entry: {str(e)}")
        return None


def get_all_entries(summary_path: str = DEFAULT_SUMMARY_PATH) -> List[Dict[str, Any]]:
    """
    Retrieves all entries from the summary file.
    
    Args:
        summary_path (str): Path to the summary file
        
    Returns:
        List[Dict[str, Any]]: All entries in the summary file, or empty list if error
    """
    try:
        # Ensure the summary file exists
        if not ensure_summary_file(summary_path):
            return []
        
        # Read the current summary
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        
        return summary
    
    except Exception as e:
        logger.error(f"Error getting all summary entries: {str(e)}")
        return [] 