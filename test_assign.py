import os
import json
import tempfile
from datetime import datetime
from utils.s3_utils import upload_json_to_s3, get_s3_json, download
import pandas as pd
import sqlite3

def create_test_file():
    """Create a test unmapped file in S3."""
    test_data = {
        "patient_info": {
            "first_name": "Test",
            "last_name": "Patient"
        },
        "billing_info": {
            "provider": "Test Provider"
        }
    }
    
    test_filename = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    test_key = f"data/hcfa_json/valid/unmapped/{test_filename}"
    
    upload_json_to_s3(test_data, test_key)
    return test_filename

def test_assign(filename, order_id):
    """Test assigning a FileMaker order to a file."""
    try:
        # Get the current JSON data
        unmapped_key = f'data/hcfa_json/valid/unmapped/{filename}'
        json_data = get_s3_json(unmapped_key)

        # Get FileMaker number from SQLite database
        filemaker_number = order_id  # Default to order_id if not found
        try:
            conn = sqlite3.connect('filemaker.db')
            cursor = conn.cursor()
            cursor.execute("SELECT FileMaker_Record_Number FROM orders WHERE Order_ID = ?", (order_id,))
            result = cursor.fetchone()
            if result:
                filemaker_number = result[0]
        except Exception as e:
            print(f"Could not get FileMaker number from database: {str(e)}")
        finally:
            conn.close()

        # Add mapping info
        json_data['mapping_info'] = {
            'order_id': order_id,
            'filemaker_number': filemaker_number,
            'mapping_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Save to mapped location
        mapped_key = f'data/hcfa_json/valid/mapped/{filename}'
        upload_json_to_s3(json_data, mapped_key)

        print(f"Successfully assigned order {order_id} to {filename}")
        print(f"FileMaker number: {filemaker_number}")
        print(f"Saved to: {mapped_key}")

    except Exception as e:
        print(f"Error in test_assign: {str(e)}")

if __name__ == "__main__":
    # Create a test file
    test_filename = create_test_file()
    print(f"Created test file: {test_filename}")
    
    # Test with a sample order_id
    test_order_id = "12345"  # Replace with a real order_id from your database
    test_assign(test_filename, test_order_id) 