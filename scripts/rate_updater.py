#!/usr/bin/env python3
import os
import json
import sqlite3
import glob
import logging
import shutil
import paramiko
from datetime import datetime
from io import StringIO

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

# Remote VM configuration
REMOTE_HOST = '159.223.104.254'
REMOTE_USER = 'ubuntu'  # You'll need to provide the correct username
REMOTE_DATA_DIR = '/srv/bill_review/data'
REMOTE_DB_PATH = '/srv/bill_review/filemaker.db'

def connect_to_remote_db():
    """Connect to the remote SQLite database via SSH."""
    try:
        # Create SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to the remote server
        # Note: You'll need to provide the correct SSH key path
        ssh.connect(REMOTE_HOST, username=REMOTE_USER, key_filename='~/.ssh/id_rsa')
        
        # Create SFTP client
        sftp = ssh.open_sftp()
        
        # Execute SQLite commands remotely
        stdin, stdout, stderr = ssh.exec_command(f'sqlite3 {REMOTE_DB_PATH}')
        
        return ssh, sftp
    except Exception as e:
        logger.error(f"Error connecting to remote database: {e}")
        raise

def execute_remote_sql(ssh, sql, params=None):
    """Execute SQL command on remote database."""
    try:
        if params:
            # Convert params to string format for command line
            param_str = ' '.join(str(p) for p in params)
            cmd = f'sqlite3 {REMOTE_DB_PATH} "{sql}" {param_str}'
        else:
            cmd = f'sqlite3 {REMOTE_DB_PATH} "{sql}"'
        
        stdin, stdout, stderr = ssh.exec_command(cmd)
        error = stderr.read().decode()
        if error:
            logger.error(f"SQL error: {error}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error executing remote SQL: {e}")
        return False

def move_to_archive(file_path, sftp):
    """Move processed file to archive directory on remote server."""
    try:
        # Get the filename without the path
        filename = os.path.basename(file_path)
        
        # Create remote archive path
        remote_archive_dir = os.path.join(REMOTE_DATA_DIR, 'rate_updates', 'archive')
        
        # Create archive directory if it doesn't exist
        try:
            sftp.stat(remote_archive_dir)
        except FileNotFoundError:
            sftp.mkdir(remote_archive_dir)
            logger.info(f"Created remote archive directory: {remote_archive_dir}")
        
        # Create destination path
        dest_path = os.path.join(remote_archive_dir, filename)
        
        # If a file with the same name already exists in the archive, add a timestamp
        try:
            sftp.stat(dest_path)
            filename_parts = os.path.splitext(filename)
            timestamped_filename = f"{filename_parts[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}{filename_parts[1]}"
            dest_path = os.path.join(remote_archive_dir, timestamped_filename)
        except FileNotFoundError:
            pass
        
        # Move the file
        sftp.rename(file_path, dest_path)
        logger.info(f"Moved file to remote archive: {file_path} -> {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Error moving file to remote archive: {file_path} - {e}")
        return False

def process_json_file(file_path, ssh, sftp):
    """Process a single JSON file and update the appropriate table."""
    try:
        # Read the file from remote server
        with sftp.open(file_path, 'r') as f:
            content = f.read()
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
        
        success = False
        
        if rate_type == 'create_ota':
            # Process OTA rates
            if not isinstance(data.get('rates', []), list) or not data.get('rates'):
                logger.error(f"Invalid or missing 'rates' data for current_otas: {data}")
                return False
            
            if not data.get('order_id'):
                logger.error(f"Missing order_id for current_otas: {data}")
                return False
            
            for rate_item in data['rates']:
                if not isinstance(rate_item, dict):
                    logger.error(f"Rate item is not a dictionary: {rate_item}")
                    continue
                    
                if 'cpt_code' not in rate_item or 'rate' not in rate_item:
                    logger.error(f"Missing required fields in rate item: {rate_item}")
                    continue
                
                sql = """
                INSERT INTO current_otas (ID_Order_PrimaryKey, CPT, modifier, rate)
                VALUES (?, ?, ?, ?)
                """
                params = (
                    data['order_id'],
                    rate_item['cpt_code'],
                    rate_item.get('modifier', ''),
                    str(rate_item['rate'])
                )
                
                if execute_remote_sql(ssh, sql, params):
                    logger.info(f"Inserted into current_otas: Order ID: {data['order_id']}, CPT: {rate_item['cpt_code']}")
                    success = True
        
        elif rate_type in ['category', 'individual']:
            # Process PPO rates
            if not data.get('tin'):
                logger.error(f"Missing TIN for PPO data: {data}")
                return False
            
            unique_id = f"PPO_{data['tin']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            if rate_type == 'individual' and 'rates' in data and isinstance(data['rates'], list):
                for rate_item in data['rates']:
                    if not isinstance(rate_item, dict):
                        logger.error(f"Rate item is not a dictionary: {rate_item}")
                        continue
                        
                    if 'cpt_code' not in rate_item or 'rate' not in rate_item:
                        logger.error(f"Missing required fields in rate item: {rate_item}")
                        continue
                    
                    sql = """
                    INSERT INTO ppo (id, RenderingState, TIN, provider_name, proc_cd, modifier, 
                                 proc_desc, proc_category, rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        unique_id,
                        '',  # RenderingState
                        data['tin'],
                        data.get('ota_data', {}).get('provider_name', '') if isinstance(data.get('ota_data'), dict) else '',
                        rate_item['cpt_code'],
                        rate_item.get('modifier', ''),
                        '',  # proc_desc
                        '',  # proc_category
                        str(rate_item['rate'])
                    )
                    
                    if execute_remote_sql(ssh, sql, params):
                        logger.info(f"Inserted individual rate into ppo: TIN: {data['tin']}, CPT: {rate_item['cpt_code']}")
                        success = True
            
            elif rate_type == 'category' and 'category_rates' in data:
                category_rates = data['category_rates']
                
                if isinstance(category_rates, dict):
                    for category, rate in category_rates.items():
                        if not category or rate is None:
                            continue
                        
                        # Get procedure codes for category
                        sql = """
                        SELECT proc_cd, modifier, proc_desc, category 
                        FROM dim_proc 
                        WHERE LOWER(category) = LOWER(?)
                        """
                        stdin, stdout, stderr = ssh.exec_command(f'sqlite3 {REMOTE_DB_PATH} "{sql}" "{category}"')
                        proc_codes = stdout.read().decode().strip().split('\n')
                        
                        for proc_line in proc_codes:
                            if not proc_line:
                                continue
                                
                            proc_code, modifier, proc_desc, proc_category = proc_line.split('|')
                            
                            sql = """
                            INSERT INTO ppo (id, RenderingState, TIN, provider_name, proc_cd, 
                                         modifier, proc_desc, proc_category, rate)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            params = (
                                unique_id,
                                '',  # RenderingState
                                data['tin'],
                                data.get('ota_data', {}).get('provider_name', '') if isinstance(data.get('ota_data'), dict) else '',
                                proc_code,
                                modifier if modifier else '',
                                proc_desc if proc_desc else '',
                                proc_category if proc_category else '',
                                str(rate)
                            )
                            
                            if execute_remote_sql(ssh, sql, params):
                                logger.info(f"Inserted into ppo: TIN: {data['tin']}, Category: {category}, CPT: {proc_code}")
                                success = True
        
        else:
            logger.warning(f"Unknown rate_type '{rate_type}' in file {file_path}")
            return False
        
        return success
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file {file_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return False

def main():
    """Main function to process all JSON files in the rate_updates directory."""
    logger.info("Starting rate updater process")
    
    ssh = None
    sftp = None
    
    try:
        # Connect to remote database
        ssh, sftp = connect_to_remote_db()
        
        # Get list of all JSON files in the remote rate_updates directory
        remote_rate_updates_dir = os.path.join(REMOTE_DATA_DIR, 'rate_updates')
        json_files = []
        for file in sftp.listdir(remote_rate_updates_dir):
            if file.endswith('.json') and not file.startswith('.'):
                json_files.append(os.path.join(remote_rate_updates_dir, file))
        
        if not json_files:
            logger.warning(f"No JSON files found in {remote_rate_updates_dir}")
            return
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        # Process each JSON file
        successful = 0
        failed = 0
        archived = 0
        
        for file_path in json_files:
            try:
                logger.info(f"Processing file: {file_path}")
                result = process_json_file(file_path, ssh, sftp)
                
                if result:
                    successful += 1
                    # Move processed file to archive
                    if move_to_archive(file_path, sftp):
                        archived += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Exception processing file {file_path}: {e}")
                failed += 1
        
        logger.info(f"Completed processing. Successful: {successful}, Failed: {failed}, Archived: {archived}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
    finally:
        # Clean up
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

if __name__ == "__main__":
    main() 