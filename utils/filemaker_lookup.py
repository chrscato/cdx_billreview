import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re
from pathlib import Path
import sqlite3
import os

# Path to the SQLite database (in root directory)
DB_PATH = 'filemaker.db'

def normalize_name(name: str) -> str:
    """Normalize a name by removing special characters and converting to lowercase."""
    if not isinstance(name, str):
        return ""
    # Remove special characters and extra spaces
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    # Convert to lowercase and strip whitespace
    return name.lower().strip()

def load_filemaker_orders() -> pd.DataFrame:
    """
    Load and normalize patient names from the local SQLite database.
    Joins orders and line_items tables to get dates of service and CPT codes.
    
    Returns:
        pd.DataFrame: Combined and normalized orders data with dates and CPTs
    """
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        
        # SQL query to join orders and line_items tables
        query = """
        SELECT 
            o.Order_ID,
            o.Patient_First_Name,
            o.Patient_Last_Name,
            GROUP_CONCAT(DISTINCT li.DOS) as DOS_list,
            GROUP_CONCAT(DISTINCT li.CPT) as CPTs
        FROM orders o
        LEFT JOIN line_items li ON o.Order_ID = li.Order_ID
        GROUP BY o.Order_ID, o.Patient_First_Name, o.Patient_Last_Name
        """
        
        # Load data into DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Normalize patient names
        df['normalized_last_name'] = df['Patient_Last_Name'].apply(normalize_name)
        df['normalized_first_name'] = df['Patient_First_Name'].apply(normalize_name)
        
        # Convert GROUP_CONCAT results to lists
        df['DOS_list'] = df['DOS_list'].apply(lambda x: x.split(',') if x else [])
        df['CPTs'] = df['CPTs'].apply(lambda x: x.split(',') if x else [])
        
        return df
        
    except Exception as e:
        print(f"Error loading orders: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

def search_orders(last_name=None, first_name=None, dos=None):
    """
    Search for orders in the FileMaker database based on name and optional date of service.
    Returns a list of matching orders with their details.
    
    Args:
        last_name (str, optional): Last name to search for
        first_name (str, optional): First name to search for
        dos (str, optional): Date of service in YYYY-MM-DD format (ignored)
        
    Returns:
        list: List of dictionaries containing order details
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Build the WHERE clause based on provided parameters
        conditions = []
        params = []
        
        if last_name:
            conditions.append("LOWER(REPLACE(Patient_Last_Name, ' ', '')) LIKE LOWER(REPLACE(?, ' ', ''))")
            params.append(f"%{last_name}%")
            
        if first_name:
            conditions.append("LOWER(REPLACE(Patient_First_Name, ' ', '')) LIKE LOWER(REPLACE(?, ' ', ''))")
            params.append(f"%{first_name}%")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Query to get matching orders with their line items
        query = f"""
            SELECT 
                o.Order_ID,
                o.Patient_First_Name || ' ' || o.Patient_Last_Name as patient_name,
                o.Patient_DOB,
                o.provider_name,
                GROUP_CONCAT(DISTINCT li.DOS) as DOS_list,
                GROUP_CONCAT(DISTINCT li.CPT) as CPTs,
                GROUP_CONCAT(DISTINCT li.Description) as CPT_descriptions
            FROM orders o
            LEFT JOIN line_items li ON o.Order_ID = li.Order_ID
            WHERE {where_clause}
            GROUP BY o.Order_ID
            ORDER BY o.Order_ID DESC
            LIMIT 5
        """
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Convert results to list of dictionaries
        matches = []
        for row in results:
            order_id, patient_name, patient_dob, provider_name, dos_list, cpts, cpt_descriptions = row
            
            # Convert comma-separated strings back to lists
            DOS_list = dos_list.split(',') if dos_list else []
            CPTs = cpts.split(',') if cpts else []
            CPT_descriptions = cpt_descriptions.split(',') if cpt_descriptions else []
            
            matches.append({
                'order_id': order_id,
                'patient_name': patient_name,
                'patient_dob': patient_dob,
                'provider_name': provider_name,
                'DOS_list': DOS_list,
                'CPTs': CPTs,
                'CPT_descriptions': CPT_descriptions
            })
        
        return matches
        
    except Exception as e:
        print(f"Error searching orders: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

def inspect_orders_data():
    """
    Inspect the structure and content of the orders and line_items tables.
    Prints column names, data types, and sample data.
    """
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        
        # Get orders table info
        orders_df = pd.read_sql_query("SELECT * FROM orders LIMIT 5", conn)
        print("\n=== Orders Table Structure ===")
        print("\nColumns and Data Types:")
        print(orders_df.dtypes)
        print("\nSample Data:")
        print(orders_df)
        
        # Get line_items table info
        line_items_df = pd.read_sql_query("SELECT * FROM line_items LIMIT 5", conn)
        print("\n=== Line Items Table Structure ===")
        print("\nColumns and Data Types:")
        print(line_items_df.dtypes)
        print("\nSample Data:")
        print(line_items_df)
        
        return True
    except Exception as e:
        print(f"Error inspecting data: {str(e)}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    inspect_orders_data() 