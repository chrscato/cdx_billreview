import os
import sqlite3
import pandas as pd
import boto3
import tempfile
from dotenv import load_dotenv
import logging
import json
from datetime import datetime
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from project root
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(env_path)

# S3 Configuration
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = 'data/filemaker'

# Backup Configuration
BACKUP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "db_backups")
os.makedirs(BACKUP_DIR, exist_ok=True)

logging.info(f"Using S3 bucket: {S3_BUCKET}")
logging.info(f"Using S3 prefix: {S3_PREFIX}")

class SyncError(Exception):
    """Custom exception for sync errors"""
    pass

def create_backup(db_path):
    """Create a backup of the database before syncing"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"filemaker_backup_{timestamp}.db")
    
    try:
        if os.path.exists(db_path):
            shutil.copy2(db_path, backup_path)
            logging.info(f"Created backup at: {backup_path}")
            return backup_path
    except Exception as e:
        logging.error(f"Failed to create backup: {str(e)}")
        return None

def validate_schema(df, table_name, conn):
    """Validate that the new data schema matches existing table schema"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        existing_columns = [description[0] for description in cursor.description]
        new_columns = df.columns.tolist()
        
        if set(existing_columns) != set(new_columns):
            missing = set(existing_columns) - set(new_columns)
            extra = set(new_columns) - set(existing_columns)
            raise SyncError(
                f"Schema mismatch for table {table_name}. "
                f"Missing columns: {missing}, Extra columns: {extra}"
            )
        return True
    except sqlite3.OperationalError:
        # Table doesn't exist yet, which is fine
        return True

def validate_sync(conn, table_name, expected_rows):
    """Validate that the sync was successful"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        actual_rows = cursor.fetchone()[0]
        
        if actual_rows != expected_rows:
            raise SyncError(
                f"Row count mismatch for table {table_name}. "
                f"Expected: {expected_rows}, Actual: {actual_rows}"
            )
        return True
    except Exception as e:
        logging.error(f"Validation failed for {table_name}: {str(e)}")
        return False

def get_db_connection(db_path, create_if_not_exists=True):
    """Create a connection to the SQLite database with foreign key support"""
    if create_if_not_exists or os.path.exists(db_path):
        logging.info(f"Connecting to database at: {db_path}")
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    else:
        logging.error(f"Database file not found: {db_path}")
        return None

def get_s3_tables(s3_client):
    """Get list of all Parquet files in the S3 prefix"""
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        tables = []
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                table_name = os.path.basename(obj['Key']).replace('.parquet', '')
                tables.append(table_name)
        logging.info(f"Found {len(tables)} tables in S3: {', '.join(tables)}")
        return tables
    except Exception as e:
        logging.error(f"Error listing S3 objects: {str(e)}")
        return []

def create_temp_table(conn, table_name, df):
    """Create a temporary table for atomic swap"""
    temp_table = f"temp_{table_name}"
    df.to_sql(temp_table, conn, index=False, if_exists='replace')
    return temp_table

def atomic_table_swap(conn, table_name, temp_table):
    """Atomically swap the temporary table with the target table"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}_old")
        cursor.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_old")
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
        cursor.execute(f"DROP TABLE {table_name}_old")
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to swap tables: {str(e)}")
        return False

def sync_table_from_s3(conn, table_name, s3_client):
    """Sync a single table from S3 Parquet file to SQLite with safeguards"""
    logging.info(f"\nProcessing table: {table_name}")
    
    if table_name == 'sqlite_sequence':
        logging.info("Skipping sqlite_sequence table")
        return True
    
    try:
        # Create a temporary file to store the Parquet data
        temp_dir = tempfile.gettempdir()
        temp_parquet = os.path.join(temp_dir, f"{table_name}.parquet")
        
        # Download from S3
        s3_key = f"{S3_PREFIX}/{table_name}.parquet"
        logging.info(f"Downloading from S3: s3://{S3_BUCKET}/{s3_key}")
        s3_client.download_file(S3_BUCKET, s3_key, temp_parquet)
        
        # Read Parquet file
        df = pd.read_parquet(temp_parquet)
        logging.info(f"Read {len(df)} rows from Parquet file")
        
        # Clean up temporary file
        os.remove(temp_parquet)
        
        # Start transaction
        conn.execute("BEGIN TRANSACTION")
        
        try:
            # Validate schema if table exists
            validate_schema(df, table_name, conn)
            
            # Create temporary table
            temp_table = create_temp_table(conn, table_name, df)
            
            # Atomic swap
            if not atomic_table_swap(conn, table_name, temp_table):
                raise SyncError(f"Failed to swap tables for {table_name}")
            
            # Validate sync
            if not validate_sync(conn, table_name, len(df)):
                raise SyncError(f"Sync validation failed for {table_name}")
            
            # Log column types for reference
            logging.info("Column types:")
            for col, dtype in df.dtypes.items():
                logging.info(f"  {col}: {dtype}")
            
            logging.info(f"Successfully synced {table_name} from S3 ({len(df)} rows)")
            return True
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error during transaction for {table_name}: {str(e)}")
            return False
            
    except Exception as e:
        logging.error(f"Error syncing {table_name}: {str(e)}")
        return False

def main():
    # Get the database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "filemaker.db")
    
    # Create backup before starting
    backup_path = create_backup(db_path)
    if not backup_path:
        logging.error("Failed to create backup, aborting sync")
        return
    
    # Initialize S3 client
    try:
        s3_client = boto3.client('s3')
        # Test S3 connection
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logging.info("Successfully connected to S3")
    except Exception as e:
        logging.error(f"Error connecting to S3: {str(e)}")
        return
    
    # Get list of tables from S3
    tables = get_s3_tables(s3_client)
    if not tables:
        logging.error("No tables found in S3")
        return
    
    # Connect to database (create if not exists)
    conn = get_db_connection(db_path)
    if not conn:
        return
    
    try:
        # Process each table
        successful_syncs = 0
        failed_tables = []
        
        for table in tables:
            if sync_table_from_s3(conn, table, s3_client):
                successful_syncs += 1
            else:
                failed_tables.append(table)
        
        # Log sync results
        logging.info(f"\nSync complete! {successful_syncs}/{len(tables)} tables synced from S3 successfully")
        if failed_tables:
            logging.warning(f"Failed tables: {', '.join(failed_tables)}")
            logging.info(f"Backup available at: {backup_path}")
    
    finally:
        # Close connection
        conn.close()

if __name__ == "__main__":
    main() 