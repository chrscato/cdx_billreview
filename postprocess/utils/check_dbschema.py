import os
import sqlite3
import json
import tempfile
import paramiko
from datetime import datetime
from pathlib import Path

# Remote database settings (from existing code)
REMOTE_HOST = "159.223.104.254"
REMOTE_USER = "root"
REMOTE_DB_PATH = "/srv/bill_review/filemaker.db"
REMOTE_KEY_PATH = os.path.expanduser("~/.ssh/id_rsa")
USE_REMOTE_DB = True  # Toggle to use remote or local database
LOCAL_DB_PATH = "filemaker.db"  # Change this if needed

def get_db_connection():
    """Get database connection (local or remote)"""
    if USE_REMOTE_DB:
        return get_remote_db_connection()
    else:
        return get_local_db_connection()

def get_local_db_connection():
    """Get a connection to the local database"""
    if not Path(LOCAL_DB_PATH).exists():
        print(f"Error: Database file not found at {LOCAL_DB_PATH}")
        return None, None
    
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn, LOCAL_DB_PATH
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
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn, temp_db.name
    except Exception as e:
        print(f"Error connecting to remote database: {e}")
        # Clean up if possible
        if 'temp_db' in locals() and os.path.exists(temp_db.name):
            os.unlink(temp_db.name)
        return None, None

def analyze_database():
    """Analyze the database and return information needed for updates"""
    conn, db_path = get_db_connection()
    if not conn:
        return {"error": "Failed to connect to database"}
    
    results = {
        "tables": {},
        "payment_stats": {},
        "schema_needs_update": True,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        # Get list of all tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row['name'] for row in cursor.fetchall()]
        results["tables_list"] = tables
        
        # Check orders table schema
        if "orders" in tables:
            cursor.execute("PRAGMA table_info(orders)")
            columns = {row['name']: dict(row) for row in cursor.fetchall()}
            results["tables"]["orders"] = {"columns": columns}
            
            # Check if BILLS_PAID field already exists
            if "BILLS_PAID" in columns:
                results["schema_needs_update"] = False
            
            # Count orders
            cursor.execute("SELECT COUNT(*) as count FROM orders")
            results["tables"]["orders"]["count"] = cursor.fetchone()['count']
        
        # Check line_items table schema
        if "line_items" in tables:
            cursor.execute("PRAGMA table_info(line_items)")
            columns = {row['name']: dict(row) for row in cursor.fetchall()}
            results["tables"]["line_items"] = {"columns": columns}
            
            # Count line items
            cursor.execute("SELECT COUNT(*) as count FROM line_items")
            results["tables"]["line_items"]["count"] = cursor.fetchone()['count']
            
            # Check for payment fields
            payment_fields = [col for col in columns.keys() if 'paid' in col.lower() or 'payment' in col.lower()]
            results["tables"]["line_items"]["payment_fields"] = payment_fields
            
            # Count line items with payments
            if "BR_paid" in columns:
                cursor.execute("SELECT COUNT(*) as count FROM line_items WHERE BR_paid IS NOT NULL")
                results["payment_stats"]["line_items_with_payments"] = cursor.fetchone()['count']
            
            # Get stats by order
            cursor.execute("""
                SELECT Order_ID, COUNT(*) as total_items, 
                SUM(CASE WHEN BR_paid IS NOT NULL THEN 1 ELSE 0 END) as paid_items 
                FROM line_items 
                GROUP BY Order_ID
                HAVING paid_items > 0
            """)
            
            orders_with_payments = []
            for row in cursor.fetchall():
                orders_with_payments.append(dict(row))
            
            results["payment_stats"]["orders_with_payments"] = orders_with_payments
            results["payment_stats"]["orders_with_payments_count"] = len(orders_with_payments)
        
        # Sample some paid line items
        if "line_items" in tables and results["payment_stats"].get("line_items_with_payments", 0) > 0:
            cursor.execute("""
                SELECT * FROM line_items 
                WHERE BR_paid IS NOT NULL 
                ORDER BY BR_date_processed DESC
                LIMIT 5
            """)
            
            sample_paid_items = []
            for row in cursor.fetchall():
                sample_paid_items.append(dict(row))
            
            results["payment_stats"]["sample_paid_items"] = sample_paid_items
        
    except Exception as e:
        results["error"] = f"Error analyzing database: {str(e)}"
    finally:
        conn.close()
        
        # Clean up temp file if using remote
        if USE_REMOTE_DB and db_path and os.path.exists(db_path) and db_path != LOCAL_DB_PATH:
            os.unlink(db_path)
    
    return results

if __name__ == "__main__":
    print("Analyzing database...")
    results = analyze_database()
    
    if "error" in results:
        print(f"Error: {results['error']}")
    else:
        # Print summary
        print("\nDatabase Analysis Summary:")
        print("-" * 50)
        
        print(f"\nFound {len(results['tables_list'])} tables:")
        for table in results['tables_list']:
            print(f"  - {table}")
        
        if "orders" in results["tables"]:
            print(f"\nOrders table: {results['tables']['orders']['count']} records")
            if results["schema_needs_update"]:
                print("  - Needs update: BILLS_PAID field not found")
            else:
                print("  - Schema already has BILLS_PAID field")
        
        if "line_items" in results["tables"]:
            print(f"\nLine items table: {results['tables']['line_items']['count']} records")
            payment_fields = results["tables"]["line_items"].get("payment_fields", [])
            print(f"  - Payment-related fields: {', '.join(payment_fields)}")
        
        if "payment_stats" in results:
            stats = results["payment_stats"]
            print("\nPayment Statistics:")
            if "line_items_with_payments" in stats:
                print(f"  - Line items with payments: {stats['line_items_with_payments']}")
            if "orders_with_payments_count" in stats:
                print(f"  - Orders with payments: {stats['orders_with_payments_count']}")
        
        # Save full results to file
        with open("db_analysis_results.json", "w") as f:
            json.dump(results, f, indent=2, default=str)
            
        print("\nFull results saved to db_analysis_results.json")