import json
import os
import sqlite3
import argparse
import boto3
from pathlib import Path
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_proc_desc(cursor, proc_cd):
    """Get procedure description and category from dim_proc table."""
    if not proc_cd:
        return None, None
    cursor.execute("SELECT proc_desc, category FROM dim_proc WHERE proc_cd = ?", (proc_cd,))
    result = cursor.fetchone()
    if result:
        return {
            'proc_desc': result['proc_desc'],
            'category': result['category']
        }
    return None

def process_json_file(s3_client, bucket_name, file_key, conn):
    try:
        # Skip if file is already in staging
        if 'staging' in file_key:
            print(f"Skipping {file_key} - already in staging")
            return
            
        # Download JSON file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Extract order_id from mapping_info
        if 'mapping_info' in json_data and 'order_id' in json_data['mapping_info']:
            order_id = json_data['mapping_info']['order_id']
            print(f"Found order_id in mapping_info: {order_id}")
        else:
            print("Could not find order_id in mapping_info")
            return
        
        # Query orders table
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders WHERE Order_ID = ?", (order_id,))
        order_result = cursor.fetchone()
        
        if not order_result:
            print(f"No order found for Order_ID: {order_id}")
            return
        
        # Query line_items table
        cursor.execute("SELECT * FROM line_items WHERE Order_ID = ?", (order_id,))
        line_items = cursor.fetchall()
        
        # Get provider_id from order
        provider_id = order_result['Provider_ID']
        
        # Query providers table
        cursor.execute("""
            SELECT 
                "Address 1 Full", "Address Line 1", "Address Line 2", "Billing Address 1", 
                "Billing Address 2", "Billing Address City", "Billing Address Postal Code", 
                "Billing Address State", "Billing Name", "City", "Contract Date", 
                "Contract Date Renewal", "Country", "DBA Name Billing Name", "Email",
                "Fax Number", "lat", "Latitude", "Location", "lon", "Longitude", 
                "Need OTA", "NPI", "Phone", "Postal Code", "PrimaryKey", "Provider Network", 
                "Provider Status", "Provider Type", "Record Status", "ServicesProvided", 
                "State", "Status", "TIN", "Website"
            FROM providers 
            WHERE PrimaryKey = ?
        """, (provider_id,))
        provider_result = cursor.fetchone()
        
        # Add procedure descriptions to service lines
        if 'service_lines' in json_data:
            for service_line in json_data['service_lines']:
                if 'cpt_code' in service_line:
                    proc_info = get_proc_desc(cursor, service_line['cpt_code'])
                    if proc_info:
                        service_line['proc_desc'] = proc_info['proc_desc']
                        service_line['category'] = proc_info['category']

        # Create filemaker section with procedure descriptions for line items
        line_items_with_desc = []
        for item in line_items:
            item_dict = dict(item)
            if 'CPT' in item_dict:
                proc_info = get_proc_desc(cursor, item_dict['CPT'])
                if proc_info:
                    item_dict['proc_desc'] = proc_info['proc_desc']
                    item_dict['category'] = proc_info['category']
            line_items_with_desc.append(item_dict)

        filemaker_data = {
            "order": dict(order_result) if order_result else None,
            "line_items": line_items_with_desc,
            "provider": dict(provider_result) if provider_result else None
        }
        
        # Add filemaker section to JSON
        json_data['filemaker'] = filemaker_data
        
        # Save updated JSON to S3 - always use first-level staging
        filename = os.path.basename(file_key)
        output_key = f"{os.path.dirname(file_key)}/staging/{filename}"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=json.dumps(json_data, indent=2, ensure_ascii=False).encode('utf-8')
        )
        
        # Delete the original file
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        
        print(f"Processed {file_key} and moved to staging")
        
    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")
        if 'json_data' in locals():
            print("JSON structure:", json.dumps(json_data, indent=2))

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process JSON files with FileMaker data')
    parser.add_argument('--file', help='Process a specific JSON file')
    args = parser.parse_args()
    
    # Get S3 configuration from environment variables
    bucket_name = os.getenv('S3_BUCKET')
    input_prefix = os.getenv('VALIDATE_VALID_PREFIX', 'data/hcfa_json/valid/')
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    # Connect to SQLite database
    conn = sqlite3.connect('filemaker.db')
    conn.row_factory = sqlite3.Row  # This enables column access by name
    
    try:
        if args.file:
            # Process single file
            file_key = f"{input_prefix}mapped/{args.file}"
            try:
                s3_client.head_object(Bucket=bucket_name, Key=file_key)
                process_json_file(s3_client, bucket_name, file_key, conn)
            except ClientError:
                print(f"File not found in S3: {file_key}")
        else:
            # Process all files
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(
                Bucket=bucket_name,
                Prefix=f"{input_prefix}mapped/"
            ):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.json'):
                        process_json_file(s3_client, bucket_name, obj['Key'], conn)
    finally:
        # Close database connection
        conn.close()

if __name__ == "__main__":
    main() 