from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import os
import json
from datetime import datetime
import pandas as pd
from utils.filemaker_lookup import search_orders
from utils.s3_utils import download, upload, move, get_s3_json, upload_json_to_s3, list_s3_files
import sqlite3
import boto3
from preprocess.utils.stage_filemaker_data import process_json_file
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

preprocessing = Blueprint('preprocessing', __name__)

def get_mapped_file_count():
    """Count the number of JSON files in the mapped directory."""
    bucket_name = os.getenv('S3_BUCKET')
    prefix = 'data/hcfa_json/valid/mapped/'
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        json_files = [obj['Key'] for obj in response.get('Contents', []) 
                     if obj['Key'].endswith('.json') and 'staging' not in obj['Key']]
        return len(json_files)
    except Exception as e:
        print(f"Error counting mapped files: {str(e)}")
        return 0

@preprocessing.route('/processing')
def index():
    """Render the processing page with file counts."""
    mapped_count = get_mapped_file_count()
    return render_template('processing.html', mapped_count=mapped_count)

@preprocessing.route('/processing/stage', methods=['POST'])
def stage_mapped_files():
    """Process all mapped files and move them to staging."""
    try:
        bucket_name = os.getenv('S3_BUCKET')
        prefix = 'data/hcfa_json/valid/mapped/'
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        # Connect to SQLite database
        conn = sqlite3.connect('filemaker.db')
        conn.row_factory = sqlite3.Row
        
        try:
            # Get all JSON files in mapped directory
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            json_files = [obj['Key'] for obj in response.get('Contents', []) 
                         if obj['Key'].endswith('.json') and 'staging' not in obj['Key']]
            
            # Process each file
            for file_key in json_files:
                process_json_file(s3_client, bucket_name, file_key, conn)
            
            flash("Staging completed! All mapped files were enriched and moved to staging.")
            
        finally:
            conn.close()
            
    except Exception as e:
        flash(f"Error during staging: {str(e)}", "error")
        
    return redirect(url_for('preprocessing.index'))

@preprocessing.route('/preprocessing/unmapped/<filename>/assign', methods=['POST'])
def assign_filemaker_order(filename):
    try:
        # Get order_id from form data
        order_id = request.form.get('order_id')
        if not order_id:
            return jsonify({'success': False, 'error': 'No order_id provided'}), 400

        # Download the JSON from unmapped directory
        unmapped_path = f'data/hcfa_json/valid/unmapped/{filename}'
        json_data = get_s3_json(unmapped_path)
        if not json_data:
            return jsonify({'success': False, 'error': 'Could not find JSON file'}), 404

        # Get filemaker_number from orders.parquet or SQLite
        try:
            # Try to get filemaker_number from SQLite first
            conn = sqlite3.connect('filemaker.db')
            cursor = conn.cursor()
            cursor.execute('SELECT FileMaker_Record_Number FROM orders WHERE Order_ID = ?', (order_id,))
            result = cursor.fetchone()
            filemaker_number = result[0] if result else order_id
            conn.close()
        except Exception as e:
            print(f"Error getting filemaker_number from SQLite: {str(e)}")
            filemaker_number = order_id  # Fallback to order_id

        # Add mapping_info block
        json_data['mapping_info'] = {
            'order_id': order_id,
            'filemaker_number': filemaker_number,
            'mapping_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mapped_by': 'admin_user@clarity-dx.com'  # TODO: Replace with actual user when auth is added
        }

        # Save updated JSON to mapped directory
        mapped_path = f'data/hcfa_json/valid/mapped/{filename}'
        upload_json_to_s3(mapped_path, json_data)

        # Delete original from unmapped directory
        move(unmapped_path, mapped_path)

        return jsonify({'success': True})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500 