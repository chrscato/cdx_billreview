#!/usr/bin/env python3
import os
import json
import sqlite3
import glob
import logging
import shutil
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rate_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_PATH = './filemaker.db'  # Path relative to running directory

# Path to the rate_updates directory
RATE_UPDATES_DIR = './data/rate_updates'  # Path relative to running directory
ARCHIVE_DIR = os.path.join(RATE_UPDATES_DIR, 'archive')

def connect_to_db():
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def insert_into_current_otas(conn, data):
    """Insert data into the current_otas table."""
    cursor = conn.cursor()
    
    # Check if rates is a list and has items
    if not isinstance(data.get('rates', []), list) or not data.get('rates'):
        logger.error(f"Invalid or missing 'rates' data for current_otas: {data}")
        return False
    
    # Check if order_id exists
    if not data.get('order_id'):
        logger.error(f"Missing order_id for current_otas: {data}")
        return False
    
    for rate_item in data['rates']:
        # Verify rate_item is a dictionary and has required fields
        if not isinstance(rate_item, dict):
            logger.error(f"Rate item is not a dictionary: {rate_item}")
            continue
            
        # Check for required fields
        if 'cpt_code' not in rate_item or 'rate' not in rate_item:
            logger.error(f"Missing required fields in rate item: {rate_item}")
            continue
        
        try:
            cursor.execute(
                """
                INSERT INTO current_otas (ID_Order_PrimaryKey, CPT, modifier, rate)
                VALUES (?, ?, ?, ?)
                """,
                (
                    data['order_id'],
                    rate_item['cpt_code'],
                    rate_item.get('modifier', ''),
                    str(rate_item['rate'])
                )
            )
            logger.info(f"Inserted into current_otas: Order ID: {data['order_id']}, CPT: {rate_item['cpt_code']}")
        except sqlite3.Error as e:
            logger.error(f"Error inserting into current_otas: {e}")
            return False
    
    return True

def get_procedure_codes_for_category(conn, category):
    """Get all procedure codes for a specific category from the dim_proc table."""
    cursor = conn.cursor()
    try:
        # Make the category comparison case-insensitive
        cursor.execute(
            """
            SELECT proc_cd, modifier, proc_desc, category FROM dim_proc 
            WHERE LOWER(category) = LOWER(?)
            """,
            (category,)
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Error querying dim_proc for category {category}: {e}")
        raise

def insert_into_ppo(conn, data):
    """Insert data into the ppo table based on rate type."""
    cursor = conn.cursor()
    
    # Check if TIN exists
    if not data.get('tin'):
        logger.error(f"Missing TIN for PPO data: {data}")
        return False
    
    # Generate a unique ID for the ppo table
    unique_id = f"PPO_{data['tin']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Initialize success flag
    success = False
    
    # Check for category_rates and handle different formats
    category_rates = data.get('category_rates')
    
    # Log the category_rates for debugging
    logger.info(f"Found category_rates: {category_rates}")
    
    # Handle different formats of category_rates
    if category_rates:
        # If category_rates is a dictionary (format: {"category_name": rate_value})
        if isinstance(category_rates, dict):
            logger.info(f"Processing dictionary-style category_rates")
            
            # Process each category in the dictionary
            for category, rate in category_rates.items():
                if not category or rate is None:
                    logger.warning(f"Skipping category rate with missing data: {category}={rate}")
                    continue
                
                logger.info(f"Processing category {category} with rate {rate}")
                
                # Get all procedure codes for this category
                try:
                    proc_codes = get_procedure_codes_for_category(conn, category)
                    
                    if not proc_codes:
                        logger.warning(f"No procedure codes found for category: {category}")
                        continue
                    
                    logger.info(f"Found {len(proc_codes)} procedure codes for category {category}")
                    
                    # Insert each procedure code with the category rate
                    for proc_code, modifier, proc_desc, proc_category in proc_codes:
                        try:
                            cursor.execute(
                                """
                                INSERT INTO ppo (id, RenderingState, TIN, provider_name, proc_cd, 
                                             modifier, proc_desc, proc_category, rate)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    unique_id,
                                    '',  # RenderingState - not provided in JSON
                                    data['tin'],
                                    data.get('ota_data', {}).get('provider_name', '') if isinstance(data.get('ota_data'), dict) else '',
                                    proc_code,
                                    modifier if modifier else '',
                                    proc_desc if proc_desc else '',
                                    proc_category if proc_category else '',
                                    str(rate)
                                )
                            )
                            logger.info(f"Inserted into ppo: TIN: {data['tin']}, Category: {category}, CPT: {proc_code}")
                            success = True
                        except sqlite3.Error as e:
                            logger.error(f"Error inserting category proc code into ppo: {e}")
                            # Continue with other codes rather than raising
                            continue
                except Exception as e:
                    logger.error(f"Error processing category {category}: {e}")
                    # Continue with other categories rather than raising
                    continue
        
        # If category_rates is a list (format: [{"category": "name", "rate": value}])
        elif isinstance(category_rates, list) and category_rates:
            logger.info(f"Processing list-style category_rates")
            # Process each category rate in the list
            for category_rate in category_rates:
                # Verify category_rate is a dictionary
                if not isinstance(category_rate, dict):
                    logger.error(f"Category rate is not a dictionary: {category_rate}")
                    continue
                    
                category = category_rate.get('category')
                rate = category_rate.get('rate')
                
                if not category or rate is None:
                    logger.warning(f"Skipping category rate with missing data: {category_rate}")
                    continue
                    
                # Get all procedure codes for this category
                try:
                    proc_codes = get_procedure_codes_for_category(conn, category)
                    
                    if not proc_codes:
                        logger.warning(f"No procedure codes found for category: {category}")
                        continue
                        
                    # Insert each procedure code with the category rate
                    for proc_code, modifier, proc_desc, proc_category in proc_codes:
                        try:
                            cursor.execute(
                                """
                                INSERT INTO ppo (id, RenderingState, TIN, provider_name, proc_cd, 
                                             modifier, proc_desc, proc_category, rate)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    unique_id,
                                    '',  # RenderingState - not provided in JSON
                                    data['tin'],
                                    data.get('ota_data', {}).get('provider_name', '') if isinstance(data.get('ota_data'), dict) else '',
                                    proc_code,
                                    modifier if modifier else '',
                                    proc_desc if proc_desc else '',
                                    proc_category if proc_category else '',
                                    str(rate)
                                )
                            )
                            logger.info(f"Inserted into ppo: TIN: {data['tin']}, Category: {category}, CPT: {proc_code}")
                            success = True
                        except sqlite3.Error as e:
                            logger.error(f"Error inserting category proc code into ppo: {e}")
                            # Continue with other codes rather than raising
                            continue
                except Exception as e:
                    logger.error(f"Error processing category {category}: {e}")
                    # Continue with other categories rather than raising
                    continue
        else:
            logger.warning(f"Unexpected category_rates format: {type(category_rates)}")
    # Regular CPT-specific rates
    elif 'rates' in data and isinstance(data['rates'], list) and data['rates']:
        for rate_item in data['rates']:
            # Verify rate_item is a dictionary and has required fields
            if not isinstance(rate_item, dict):
                logger.error(f"Rate item is not a dictionary: {rate_item}")
                continue
                
            # Check for required fields
            if 'cpt_code' not in rate_item or 'rate' not in rate_item:
                logger.error(f"Missing required fields in rate item: {rate_item}")
                continue
            
            try:
                cursor.execute(
                    """
                    INSERT INTO ppo (id, RenderingState, TIN, provider_name, proc_cd, modifier, 
                                   proc_desc, proc_category, rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        unique_id,
                        '',  # RenderingState - not provided in JSON
                        data['tin'],
                        data.get('ota_data', {}).get('provider_name', '') if isinstance(data.get('ota_data'), dict) else '',
                        rate_item['cpt_code'],
                        rate_item.get('modifier', ''),
                        '',  # proc_desc - not provided in JSON
                        '',  # proc_category - not provided in JSON
                        str(rate_item['rate'])
                    )
                )
                logger.info(f"Inserted into ppo: TIN: {data['tin']}, CPT: {rate_item['cpt_code']}")
                success = True
            except sqlite3.Error as e:
                logger.error(f"Error inserting into ppo: {e}")
                # Continue with other rates rather than raising
                continue
    else:
        logger.warning(f"No rates or category_rates found in PPO data: {data}")
        return False
    
    return success

def move_to_archive(file_path):
    """Move processed file to archive directory."""
    try:
        # Create archive directory if it doesn't exist
        if not os.path.exists(ARCHIVE_DIR):
            os.makedirs(ARCHIVE_DIR)
            logger.info(f"Created archive directory: {ARCHIVE_DIR}")
        
        # Get the filename without the path
        filename = os.path.basename(file_path)
        
        # Create destination path
        dest_path = os.path.join(ARCHIVE_DIR, filename)
        
        # If a file with the same name already exists in the archive, add a timestamp
        if os.path.exists(dest_path):
            filename_parts = os.path.splitext(filename)
            timestamped_filename = f"{filename_parts[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}{filename_parts[1]}"
            dest_path = os.path.join(ARCHIVE_DIR, timestamped_filename)
        
        # Move the file
        shutil.move(file_path, dest_path)
        logger.info(f"Moved file to archive: {file_path} -> {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Error moving file to archive: {file_path} - {e}")
        return False

def process_json_file(file_path, conn):
    """Process a single JSON file and update the appropriate table."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            # Check if content is empty
            if not content.strip():
                logger.error(f"Empty JSON file: {file_path}")
                return False
            data = json.loads(content)
        
        # Verify data is a dictionary
        if not isinstance(data, dict):
            logger.error(f"JSON data is not a dictionary: {data}")
            return False
        
        # Log the full data for debugging
        logger.info(f"Processing JSON data: {data}")
        
        # Check rate_type and insert into appropriate table
        rate_type = data.get('rate_type', '').lower()
        logger.info(f"Found rate_type: {rate_type}")
        
        if rate_type == 'create_ota':
            return insert_into_current_otas(conn, data)
        elif rate_type == 'category':
            return insert_into_ppo(conn, data)
        else:
            logger.warning(f"Unknown rate_type '{rate_type}' in file {file_path}")
            return False
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file {file_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return False

def main():
    """Main function to process all JSON files in the rate_updates directory."""
    logger.info("Starting rate updater process")
    
    try:
        conn = connect_to_db()
        
        # Get list of all JSON files in the rate_updates directory (excluding the archive subdirectory)
        json_files = [f for f in glob.glob(os.path.join(RATE_UPDATES_DIR, '*.json*')) 
                     if not os.path.join('archive') in f]
        
        if not json_files:
            logger.warning(f"No JSON files found in {RATE_UPDATES_DIR}")
            return
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        # Process each JSON file
        successful = 0
        failed = 0
        archived = 0
        
        for file_path in json_files:
            try:
                logger.info(f"Processing file: {file_path}")
                result = process_json_file(file_path, conn)
                
                if result:
                    successful += 1
                    # Move processed file to archive
                    if move_to_archive(file_path):
                        archived += 1
                else:
                    failed += 1
                    # Optionally move failed files to a "failed" directory
                    # This can be added if desired
            except Exception as e:
                logger.error(f"Exception processing file {file_path}: {e}")
                failed += 1
        
        # Commit all changes
        conn.commit()
        logger.info(f"Completed processing. Successful: {successful}, Failed: {failed}, Archived: {archived}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 