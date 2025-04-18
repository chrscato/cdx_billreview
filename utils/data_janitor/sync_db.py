#!/usr/bin/env python3
"""
sync_db.py

Syncs SQLite database to Parquet files in S3 and provides utilities for querying.
Implements a dual-database approach:
- SQLite for local development and GUI browsing
- Parquet in S3 for cloud-scale analytics
"""
import os
import sys
import hashlib
import json
import tempfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import duckdb
from dotenv import load_dotenv

# Add the project root to Python path
project_root = str(Path(__file__).resolve().parents[2])
sys.path.append(project_root)

# Import S3 helper functions
from utils.s3_utils import upload, download, list_objects

# Load environment variables
load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')

# Paths
SQLITE_PATH = os.path.join(project_root, 'reference_tables', 'orders2.db')
S3_PARQUET_PREFIX = 'reference_tables/parquet/'
S3_META_PATH = 'reference_tables/db_meta.json'

# Tables to sync
TABLES = [
    'orders',
    'line_items'
]

def calculate_table_hash(df):
    """Calculate hash of DataFrame for change detection."""
    return hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()

def get_current_meta():
    """Get current metadata from S3 or create new."""
    try:
        meta_local = os.path.join(tempfile.gettempdir(), 'db_meta.json')
        download(S3_META_PATH, meta_local)
        with open(meta_local, 'r') as f:
            return json.load(f)
    except:
        return {
            'last_sync': None,
            'table_hashes': {}
        }

def save_meta(meta):
    """Save metadata back to S3."""
    meta_local = os.path.join(tempfile.gettempdir(), 'db_meta.json')
    with open(meta_local, 'w') as f:
        json.dump(meta, f, indent=4)
    upload(meta_local, S3_META_PATH)

def sync_to_parquet():
    """Sync SQLite database to Parquet files in S3."""
    print("Starting database sync...")
    
    # Get current metadata
    meta = get_current_meta()
    
    # Connect to databases
    duck_con = duckdb.connect()
    
    changes = False
    for table in TABLES:
        print(f"\nProcessing table: {table}")
        
        # Read from SQLite
        df = pd.read_sql(f"SELECT * FROM {table}", f"sqlite:///{SQLITE_PATH}")
        
        # Calculate hash
        current_hash = calculate_table_hash(df)
        previous_hash = meta.get('table_hashes', {}).get(table)
        
        if current_hash == previous_hash:
            print(f"✓ No changes detected for {table}")
            continue
        
        # Convert to Parquet and upload
        parquet_local = os.path.join(tempfile.gettempdir(), f"{table}.parquet")
        s3_key = f"{S3_PARQUET_PREFIX}{table}.parquet"
        
        print(f"Converting {table} to Parquet...")
        df.to_parquet(parquet_local, index=False)
        
        print(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
        upload(parquet_local, s3_key)
        
        # Update metadata
        meta['table_hashes'][table] = current_hash
        changes = True
        
        # Cleanup
        os.remove(parquet_local)
        
        print(f"✔ Synced {table}")
    
    if changes:
        meta['last_sync'] = datetime.now().isoformat()
        save_meta(meta)
        print("\n✔ Sync complete with changes")
    else:
        print("\n✔ Sync complete - no changes needed")

def query_parquet(sql_query):
    """Query Parquet files directly from S3 using DuckDB."""
    con = duckdb.connect()
    
    # Register tables
    for table in TABLES:
        s3_path = f"s3://{S3_BUCKET}/{S3_PARQUET_PREFIX}{table}.parquet"
        con.execute(f"""
            CREATE VIEW {table} AS 
            SELECT * FROM read_parquet('{s3_path}')
        """)
    
    # Execute query
    return con.execute(sql_query).fetchdf()

def restore_sqlite(output_path=None):
    """Restore SQLite database from Parquet files."""
    if output_path is None:
        output_path = os.path.join(project_root, 'reference_tables', 'restored.db')
    
    print(f"Restoring database to: {output_path}")
    
    con = duckdb.connect(output_path)
    con.execute("INSTALL sqlite; LOAD sqlite;")
    
    for table in TABLES:
        s3_path = f"s3://{S3_BUCKET}/{S3_PARQUET_PREFIX}{table}.parquet"
        print(f"Restoring table: {table}")
        
        con.execute(f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_parquet('{s3_path}');
        """)
    
    con.execute(f"EXPORT DATABASE '{output_path}' (FORMAT SQLITE);")
    print(f"✔ Database restored to: {output_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync and manage dual SQLite/Parquet database setup")
    parser.add_argument('action', choices=['sync', 'query', 'restore'], help='Action to perform')
    parser.add_argument('--query', help='SQL query to execute (for query action)')
    parser.add_argument('--output', help='Output path for restored database (for restore action)')
    
    args = parser.parse_args()
    
    if args.action == 'sync':
        sync_to_parquet()
    elif args.action == 'query':
        if not args.query:
            print("❌ Please provide a SQL query with --query")
            sys.exit(1)
        result = query_parquet(args.query)
        print(result)
    else:  # restore
        restore_sqlite(args.output) 