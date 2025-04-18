import os
import sqlite3
import pandas as pd
import boto3
import tempfile
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from project root
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(env_path)

# S3 Configuration
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = 'data/filemaker'

logging.info(f"Using S3 bucket: {S3_BUCKET}")
logging.info(f"Using S3 prefix: {S3_PREFIX}")

def get_db_connection(db_path):
    """Create a connection to the SQLite database."""
    logging.info(f"Connecting to database at: {db_path}")
    return sqlite3.connect(db_path)

def get_all_tables(conn):
    """Get list of all tables in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    logging.info(f"Found {len(tables)} tables: {', '.join(tables)}")
    return tables

def sync_table_to_s3(conn, table_name, s3_client):
    """Sync a single table to S3 as a Parquet file."""
    logging.info(f"\nProcessing table: {table_name}")
    
    try:
        # Read the entire table into a pandas DataFrame
        logging.info(f"Reading table {table_name} into DataFrame...")
        
        # For problematic tables, read all columns as strings first
        if table_name in ['ppo', 'line_items', 'current_otas']:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn, dtype=str)
        else:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            
        logging.info(f"Read {len(df)} rows from {table_name}")
        
        # Convert timestamp columns to datetime if they exist
        timestamp_columns = ['created_at', 'updated_at', 'timestamp']
        for col in timestamp_columns:
            if col in df.columns:
                logging.info(f"Converting {col} to datetime in {table_name}")
                try:
                    df[col] = pd.to_datetime(df[col], format='mixed')
                except Exception as e:
                    logging.warning(f"Could not convert all values in {col} to datetime: {str(e)}")
        
        # Create a temporary file to store the Parquet data
        temp_dir = tempfile.gettempdir()
        temp_parquet = os.path.join(temp_dir, f"{table_name}.parquet")
        logging.info(f"Writing to temporary file: {temp_parquet}")
        
        # Write to Parquet file, allowing mixed types
        df.to_parquet(temp_parquet, index=False, engine='pyarrow')
        
        # Upload to S3
        s3_key = f"{S3_PREFIX}/{table_name}.parquet"
        logging.info(f"Uploading to S3: s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(temp_parquet, S3_BUCKET, s3_key)
        
        # Clean up temporary file
        os.remove(temp_parquet)
        
        # Log column types for reference
        logging.info("Column types:")
        for col, dtype in df.dtypes.items():
            logging.info(f"  {col}: {dtype}")
        
        logging.info(f"Successfully synced {table_name} to S3 ({len(df)} rows)")
        return True
        
    except Exception as e:
        logging.error(f"Error syncing {table_name}: {str(e)}")
        return False

def main():
    # Get the database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "filemaker.db")
    
    if not os.path.exists(db_path):
        logging.error(f"Database file not found: {db_path}")
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
    
    # Create S3 prefix if it doesn't exist
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/")
        logging.info(f"Created S3 prefix: {S3_PREFIX}/")
    except Exception as e:
        logging.error(f"Error creating S3 prefix: {str(e)}")
        return
    
    # Connect to database
    conn = get_db_connection(db_path)
    
    # Get all tables
    tables = get_all_tables(conn)
    
    # Process each table
    successful_syncs = 0
    for table in tables:
        if sync_table_to_s3(conn, table, s3_client):
            successful_syncs += 1
    
    # Close connection
    conn.close()
    
    logging.info(f"\nSync complete! {successful_syncs}/{len(tables)} tables synced successfully")

if __name__ == "__main__":
    main() 