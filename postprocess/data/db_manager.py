import sqlite3
import os
from pathlib import Path
import tempfile
import paramiko
from config.settings import DB_PATH
from postprocess.data.db_logger import db_logger

# Remote database settings
REMOTE_HOST = "159.223.104.254"
REMOTE_USER = "root"
REMOTE_DB_PATH = "/srv/bill_review/filemaker.db"
REMOTE_KEY_PATH = os.path.expanduser("~/.ssh/id_rsa")  # Path to your SSH key
USE_REMOTE_DB = True  # Toggle to use remote or local database

def get_db_connection():
    """Get database connection (local or remote)"""
    if USE_REMOTE_DB:
        return get_remote_db_connection()
    else:
        return get_local_db_connection()

def get_local_db_connection():
    """Get a connection to the local database"""
    if not Path(DB_PATH).exists():
        print(f"Error: Database file not found at {DB_PATH}")
        return None, None
    
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn, DB_PATH
    except Exception as e:
        print(f"Error connecting to local database: {e}")
        return None, None

def get_remote_db_connection():
    """Create an SSH tunnel and connect to the remote database"""
    try:
        # Create a temporary file to hold the database
        temp_db = tempfile.NamedTemporaryFile(delete=False)
        temp_db.close()
        
        # Setup SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to the remote server
        try:
            ssh.connect(REMOTE_HOST, username=REMOTE_USER, key_filename=REMOTE_KEY_PATH)
        except Exception as e:
            print(f"SSH connection error: {e}")
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)
            return None, None
        
        # Copy the remote database file to local temp file
        sftp = ssh.open_sftp()
        sftp.get(REMOTE_DB_PATH, temp_db.name)
        sftp.close()
        ssh.close()
        
        # Connect to the copied database
        conn = sqlite3.connect(temp_db.name)
        return conn, temp_db.name
    except Exception as e:
        print(f"Error connecting to remote database: {e}")
        # Clean up if possible
        if 'temp_db' in locals() and os.path.exists(temp_db.name):
            os.unlink(temp_db.name)
        return None, None

def push_db_changes_to_remote(temp_db_path):
    """Push the updated database back to the remote server"""
    try:
        # Setup SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to the remote server
        ssh.connect(REMOTE_HOST, username=REMOTE_USER, key_filename=REMOTE_KEY_PATH)
        
        # Copy the local temp file back to the remote server
        sftp = ssh.open_sftp()
        
        # Create a backup of remote database first
        backup_name = f"{REMOTE_DB_PATH}.bak.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        sftp.rename(REMOTE_DB_PATH, backup_name)
        
        # Upload the updated database
        sftp.put(temp_db_path, REMOTE_DB_PATH)
        sftp.close()
        ssh.close()
        
        print(f"Successfully updated remote database (backup created at {backup_name})")
        return True
    except Exception as e:
        print(f"Error pushing database changes to remote: {e}")
        return False

def initialize_database():
    """Initialize SQLite database connection"""
    conn, db_path = get_db_connection()
    if not conn:
        return False
    
    try:
        # Check if the line_items table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='line_items'")
        if not cursor.fetchone():
            print("Warning: line_items table not found in database")
            conn.close()
            
            # Clean up temp file if using remote
            if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
                os.unlink(db_path)
                
            return False
            
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        return True
    except Exception as e:
        print(f"Error connecting to database: {e}")
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        return False

def check_if_item_paid(line_item_id, order_id):
    """
    Check if a line item has already been paid
    
    Args:
        line_item_id (int): The line item ID
        order_id (str): The order ID
        
    Returns:
        bool: True if the item has been paid, False otherwise
    """
    if not line_item_id or not order_id:
        return False
    
    conn, db_path = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check if the line item exists and has been paid
        cursor.execute(
            'SELECT BR_paid FROM line_items WHERE id = ? AND Order_ID = ? AND BR_paid IS NOT NULL',
            (line_item_id, order_id)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        db_logger.log(
            function="check_if_item_paid",
            action="read",
            params={"line_item_id": line_item_id, "order_id": order_id},
            result=result is not None
        )
        return result is not None
    except Exception as e:
        print(f"Error checking if item paid: {e}")
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        db_logger.log(
            function="check_if_item_paid",
            action="read",
            params={"line_item_id": line_item_id, "order_id": order_id},
            result=f"Exception: {e}"
        )
        return False

def update_payment_info(line_item_id, order_id, br_paid, br_rate, eobr_doc_no, hcfa_doc_no, br_date_processed):
    """
    Update payment information for a line item
    
    Args:
        line_item_id (int): The line item ID
        order_id (str): The order ID
        br_paid (str): The amount paid
        br_rate (float): The rate applied
        eobr_doc_no (str): The EOBR document number
        hcfa_doc_no (str): The HCFA document number
        br_date_processed (str): The date the payment was processed
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not line_item_id or not order_id:
        return False
    
    conn, db_path = get_db_connection()
    if not conn:
        return False
    
    try:
        # Update the line_items table
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE line_items SET 
            BR_paid = ?,
            BR_rate = ?,
            EOBR_doc_no = ?,
            HCFA_doc_no = ?,
            BR_date_processed = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND Order_ID = ?
        ''', (br_paid, br_rate, eobr_doc_no, hcfa_doc_no, br_date_processed, line_item_id, order_id))
        
        rows_affected = cursor.rowcount
        conn.commit()
        
        print(f"Updated payment info for line item {line_item_id}, order {order_id}: {rows_affected} row(s) affected")
        
        # If using remote DB, push changes back to server
        if USE_REMOTE_DB:
            conn.close()  # Close before pushing
            push_db_changes_to_remote(db_path)
            
            # Clean up temp file
            if db_path and os.path.exists(db_path) and db_path != DB_PATH:
                os.unlink(db_path)
        else:
            conn.close()
            
        db_logger.log(
            function="update_payment_info",
            action="update",
            params={
                "line_item_id": line_item_id,
                "order_id": order_id,
                "br_paid": br_paid,
                "br_rate": br_rate,
                "eobr_doc_no": eobr_doc_no,
                "hcfa_doc_no": hcfa_doc_no,
                "br_date_processed": br_date_processed
            },
            result=f"rows_affected: {rows_affected}"
        )
        return rows_affected > 0
        
    except Exception as e:
        print(f"Error updating payment info: {e}")
        conn.rollback()
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        db_logger.log(
            function="update_payment_info",
            action="update",
            params={
                "line_item_id": line_item_id,
                "order_id": order_id,
                "br_paid": br_paid,
                "br_rate": br_rate,
                "eobr_doc_no": eobr_doc_no,
                "hcfa_doc_no": hcfa_doc_no,
                "br_date_processed": br_date_processed
            },
            result=f"Exception: {e}"
        )
        return False

def list_line_items(order_id=None):
    """List line items in the database, optionally filtered by order_id"""
    conn, db_path = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        if order_id:
            cursor.execute('SELECT id, Order_ID, CPT, BR_paid, BR_rate, EOBR_doc_no FROM line_items WHERE Order_ID = ?', (order_id,))
        else:
            cursor.execute('SELECT id, Order_ID, CPT, BR_paid, BR_rate, EOBR_doc_no FROM line_items LIMIT 10')
        
        rows = cursor.fetchall()
        
        print(f"Found {len(rows)} line items:")
        for row in rows:
            print(f"  ID: {row[0]}, Order: {row[1]}, CPT: {row[2]}, Paid: {row[3]}, Rate: {row[4]}, EOBR: {row[5]}")
            
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        db_logger.log(
            function="list_line_items",
            action="read",
            params={"order_id": order_id},
            result=f"rows: {len(rows)}"
        )
        return rows
    except Exception as e:
        print(f"Error listing line items: {e}")
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != DB_PATH:
            os.unlink(db_path)
            
        db_logger.log(
            function="list_line_items",
            action="read",
            params={"order_id": order_id},
            result=f"Exception: {e}"
        )
        return None