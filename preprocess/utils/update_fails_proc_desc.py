import json
import os
import sqlite3
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_proc_desc(cursor, proc_cd):
    """Get procedure description and category from dim_proc table."""
    if not proc_cd:
        return None
    cursor.execute("SELECT proc_desc, category FROM dim_proc WHERE proc_cd = ?", (proc_cd,))
    result = cursor.fetchone()
    if result:
        return {
            'proc_desc': result['proc_desc'],
            'category': result['category']
        }
    return None

def process_fail_file(s3_client, bucket_name, file_key, conn):
    try:
        # Download JSON file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        
        cursor = conn.cursor()
        modified = False

        # Add procedure descriptions to service lines
        if 'service_lines' in json_data:
            for service_line in json_data['service_lines']:
                if 'cpt_code' in service_line:
                    proc_info = get_proc_desc(cursor, service_line['cpt_code'])
                    if proc_info:
                        # Update or add proc_desc and category
                        if service_line.get('proc_desc') != proc_info['proc_desc'] or \
                           service_line.get('category') != proc_info['category']:
                            service_line['proc_desc'] = proc_info['proc_desc']
                            service_line['category'] = proc_info['category']
                            modified = True

        # Add procedure descriptions to filemaker line items
        if 'filemaker' in json_data and 'line_items' in json_data['filemaker']:
            for item in json_data['filemaker']['line_items']:
                if 'CPT' in item:
                    proc_info = get_proc_desc(cursor, item['CPT'])
                    if proc_info:
                        # Update or add proc_desc and category
                        if item.get('proc_desc') != proc_info['proc_desc'] or \
                           item.get('category') != proc_info['category']:
                            item['proc_desc'] = proc_info['proc_desc']
                            item['category'] = proc_info['category']
                            modified = True

        if modified:
            # Save updated JSON back to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=json.dumps(json_data, indent=2, ensure_ascii=False).encode('utf-8')
            )
            print(f"Updated procedure descriptions and categories in {file_key}")
        else:
            print(f"No updates needed for {file_key}")
        
    except Exception as e:
        print(f"Error processing {file_key}: {str(e)}")
        if 'json_data' in locals():
            print("JSON structure:", json.dumps(json_data, indent=2))

def main():
    # Get S3 configuration from environment variables
    bucket_name = os.getenv('S3_BUCKET')
    fails_prefix = 'data/hcfa_json/valid/mapped/staging/fails/'  # Adjust this if your fails prefix is different
    
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
        # Process all files in fails bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        file_count = 0
        updated_count = 0
        
        print("Starting to process files in fails bucket...")
        
        for page in paginator.paginate(
            Bucket=bucket_name,
            Prefix=fails_prefix
        ):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.json'):
                    file_count += 1
                    process_fail_file(s3_client, bucket_name, obj['Key'], conn)
                    updated_count += 1
                    
                    # Print progress every 10 files
                    if file_count % 10 == 0:
                        print(f"Processed {file_count} files so far...")
        
        print(f"\nProcessing complete!")
        print(f"Total files processed: {file_count}")
        print(f"Files updated: {updated_count}")
        
    finally:
        # Close database connection
        conn.close()

if __name__ == "__main__":
    main() 