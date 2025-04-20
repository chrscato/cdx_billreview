from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, move
import os
import re
import boto3
import subprocess
from pathlib import Path
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import tempfile
import logging
import sys
from datetime import datetime
import pandas as pd
import sqlite3

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Get the absolute path of the project root
project_root = Path(__file__).resolve().parents[2]
env_path = project_root / '.env'
logger.info(f"Looking for .env file at: {env_path}")
logger.info(f"Current working directory: {os.getcwd()}")

# Load environment variables
load_dotenv(dotenv_path=env_path)

# Debug environment variables
logger.info("Environment variables after loading:")
for var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION', 'S3_BUCKET']:
    # Only show first/last 4 characters for sensitive info
    value = os.getenv(var, 'NOT SET')
    if var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'] and value != 'NOT SET':
        value = f"{value[:4]}...{value[-4:]}"
    logger.info(f"{var}: {value}")

# Check required environment variables
required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION', 'S3_BUCKET']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")

# Initialize Blueprint
preprocessing_bp = Blueprint('preprocessing', __name__)

# Initialize S3 client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    # Test S3 connection
    s3_client.list_buckets()
    logger.info("Successfully connected to S3")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {str(e)}")
    s3_client = None

S3_BUCKET = os.getenv('S3_BUCKET')

def get_invalid_files_count():
    prefix = 'data/hcfa_json/invalid/'
    files = list_objects(prefix)
    return len(files)

def get_unmapped_files_count():
    prefix = 'data/hcfa_json/valid/unmapped/'
    files = list_objects(prefix)
    return len(files)

def get_mapped_files_count():
    """Count the number of JSON files in the mapped directory."""
    prefix = 'data/hcfa_json/valid/mapped/'
    files = list_objects(prefix)
    return len([f for f in files if f.endswith('.json') and 'staging' not in f])

@preprocessing_bp.route('/')
def index():
    invalid_count = get_invalid_files_count()
    unmapped_count = get_unmapped_files_count()
    mapped_count = get_mapped_files_count()
    return render_template('preprocessing.html', 
                         invalid_count=invalid_count,
                         unmapped_count=unmapped_count,
                         mapped_count=mapped_count)

@preprocessing_bp.route('/invalid')
def list_invalid_files():
    # Get list of files from S3
    prefix = 'data/hcfa_json/invalid/'
    files = list_objects(prefix)
    
    # Extract just the filenames from the full paths
    filenames = [os.path.basename(f) for f in files]
    
    return render_template('preprocessing/invalid.html', 
                         files=filenames,
                         invalid_count=len(filenames))

@preprocessing_bp.route('/invalid/<filename>')
def view_invalid_file(filename):
    # Construct the full S3 key
    key = f'data/hcfa_json/invalid/{filename}'
    
    try:
        # Get the JSON data
        json_data = get_s3_json(key)
        invalid_count = get_invalid_files_count()
        
        # Get list of all invalid files
        prefix = 'data/hcfa_json/invalid/'
        all_files = [os.path.basename(f) for f in list_objects(prefix)]
        
        # Find current file index
        current_index = all_files.index(filename)
        
        # Get next and previous filenames
        next_file = all_files[current_index + 1] if current_index < len(all_files) - 1 else None
        prev_file = all_files[current_index - 1] if current_index > 0 else None
        
        return render_template('preprocessing/edit_invalid.html', 
                             filename=filename,
                             json_data=json_data,
                             invalid_count=invalid_count,
                             next_file=next_file,
                             prev_file=prev_file,
                             current_index=current_index + 1,  # 1-based index for display
                             total_files=len(all_files))
    except Exception as e:
        return render_template('preprocessing/error.html', error=str(e))

@preprocessing_bp.route('/invalid/<filename>/submit', methods=['POST'])
def submit_invalid_file(filename):
    form_data = request.form.to_dict(flat=False)
    action = form_data.get('action', ['save'])[0]
    
    # Initialize the JSON structure
    json_data = {
        'patient_info': {},
        'billing_info': {},
        'service_lines': []
    }
    
    # Process patient info
    for key, value in form_data.items():
        if key.startswith('patient_info['):
            field = re.match(r'patient_info\[(.*?)\]', key).group(1)
            json_data['patient_info'][field] = value[0]
    
    # Process billing info
    for key, value in form_data.items():
        if key.startswith('billing_info['):
            field = re.match(r'billing_info\[(.*?)\]', key).group(1)
            json_data['billing_info'][field] = value[0]
    
    # Process service lines
    service_line_pattern = re.compile(r'service_lines\[(\d+)\]\[(.*?)\]')
    service_lines = {}
    
    for key, value in form_data.items():
        if key.startswith('service_lines['):
            match = service_line_pattern.match(key)
            if match:
                index = int(match.group(1))
                field = match.group(2)
                
                # Initialize this service line if it doesn't exist
                if index not in service_lines:
                    service_lines[index] = {}
                
                # Handle modifiers specially - convert comma-separated string to array
                if field == 'modifiers':
                    modifiers = value[0].split(',') if value[0] else []
                    # Remove empty strings that might come from trailing commas
                    modifiers = [m.strip() for m in modifiers if m.strip()]
                    service_lines[index][field] = modifiers
                else:
                    service_lines[index][field] = value[0]
    
    # Convert service_lines dict to list, maintaining order
    json_data['service_lines'] = [
        service_lines[i] for i in sorted(service_lines.keys())
    ]
    
    try:
        invalid_key = f'data/hcfa_json/invalid/{filename}'
        
        if action == 'approve':
            # Move to valid folder
            valid_key = f'data/hcfa_json/valid/{filename}'
            upload_json_to_s3(json_data, valid_key)
            move(invalid_key, valid_key)  # This will copy and delete the original
        else:
            # Save changes back to invalid folder
            upload_json_to_s3(json_data, invalid_key)
        
        # Redirect back to the invalid files list
        return redirect(url_for('preprocessing.list_invalid_files'))
        
    except Exception as e:
        return render_template('preprocessing/error.html', 
                             error=f"Failed to {action} file: {str(e)}")

@preprocessing_bp.route('/dropoff')
def dropoff():
    return render_template('preprocessing/dropoff.html')

@preprocessing_bp.route('/upload', methods=['POST'])
def upload_file():
    try:
        # Check if S3 client is properly initialized
        if s3_client is None:
            logger.error("S3 client not properly initialized")
            return jsonify({
                'success': False,
                'error': 'S3 Configuration Error',
                'details': 'S3 client not properly initialized. Check server logs.'
            }), 500

        if 'file' not in request.files:
            logger.warning("No file provided in request")
            return jsonify({
                'success': False,
                'error': 'No file was provided in the request',
                'details': 'Please select a file before uploading'
            }), 400
        
        file = request.files['file']
        if file.filename == '':
            logger.warning("Empty filename provided")
            return jsonify({
                'success': False,
                'error': 'No filename detected',
                'details': 'Please select a valid file'
            }), 400
        
        if not file.filename.lower().endswith('.pdf'):
            logger.warning(f"Invalid file type: {file.filename}")
            return jsonify({
                'success': False,
                'error': 'Invalid file type',
                'details': f'File must be a PDF. Received: {file.filename}'
            }), 400
        
        # Upload to S3
        s3_key = f"data/batches/{file.filename}"
        logger.info(f"Attempting to upload file to S3: {s3_key}")
        
        # Create a temporary file to handle the upload
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        tmp_file_path = tmp_file.name
        try:
            # Save the uploaded file to the temporary file
            file.save(tmp_file_path)
            tmp_file.close()  # Close the file handle explicitly
            logger.debug(f"Saved file to temporary location: {tmp_file_path}")
            
            # Upload the temp file to S3
            with open(tmp_file_path, 'rb') as f:
                s3_client.upload_fileobj(f, S3_BUCKET, s3_key)
            
            logger.debug("Upload to S3 complete")
            
        finally:
            # Clean up: close and delete temp file
            try:
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)
                    logger.debug("Cleaned up temporary file")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {tmp_file_path}: {e}")
            
        logger.info(f"Successfully uploaded file to S3: {s3_key}")
        return jsonify({
            'success': True,
            'message': f'Successfully uploaded {file.filename} to S3',
            'details': {
                'filename': file.filename,
                's3_key': s3_key,
                'bucket': S3_BUCKET
            }
        }), 200
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"AWS Error: {error_code} - {error_message}")
        return jsonify({
            'success': False,
            'error': 'S3 Upload Error',
            'details': f'{error_code}: {error_message}'
        }), 500
    except Exception as e:
        logger.error(f"Unexpected error during upload: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Server Error',
            'details': str(e)
        }), 500

@preprocessing_bp.route('/start', methods=['POST'])
def start_preprocessing():
    try:
        # Get the project root directory
        project_root = Path(__file__).resolve().parents[2]
        logger.info(f"Starting preprocessing from directory: {project_root}")
        
        # Get Python executable path
        python_exe = sys.executable
        logger.info(f"Using Python interpreter: {python_exe}")
        
        # Run the preprocessing script
        process = subprocess.Popen(
            [python_exe, '-m', 'preprocess.main'],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env={**os.environ, 'PYTHONPATH': str(project_root)}
        )
        
        output = []
        # Read output line by line
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                clean_line = line.strip()
                logger.info(f"Pipeline output: {clean_line}")
                output.append(clean_line)
        
        process.wait()
        success = process.returncode == 0
        
        if success:
            logger.info("Preprocessing completed successfully")
        else:
            logger.error("Preprocessing failed")
            
        return jsonify({
            'success': success,
            'output': output,
            'message': 'Preprocessing completed successfully' if success else 'Preprocessing failed'
        })
        
    except Exception as e:
        logger.error(f"Error starting preprocessing: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@preprocessing_bp.route('/invalid/<filename>/pdf')
def get_pdf_url(filename):
    try:
        # Convert json filename to pdf filename
        pdf_filename = filename.replace('.json', '.pdf')
        pdf_key = f'data/hcfa_pdf/archived/{pdf_filename}'
        
        # Generate presigned URL that expires in 1 hour
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': pdf_key,
                'ResponseContentType': 'application/pdf'
            },
            ExpiresIn=3600
        )
        return jsonify({'url': url})
    except Exception as e:
        logger.error(f"Error generating PDF URL: {str(e)}")
        return jsonify({'error': str(e)}), 404

@preprocessing_bp.route('/invalid/<filename>/preview/<section>')
def get_preview_url(filename, section):
    try:
        # Get base filename without extension
        base_filename = filename.replace('.json', '')
        preview_key = f'data/hcfa_pdf/preview/{base_filename}/{section}.png'
        
        # Generate presigned URL that expires in 1 hour
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': preview_key,
                'ResponseContentType': 'image/png'
            },
            ExpiresIn=3600
        )
        return jsonify({'url': url})
    except Exception as e:
        logger.error(f"Error generating preview URL: {str(e)}")
        return jsonify({'error': str(e)}), 404

@preprocessing_bp.route('/unmapped')
def list_unmapped_files():
    # Get list of files from S3
    prefix = 'data/hcfa_json/valid/unmapped/'
    files = list_objects(prefix)
    
    # Extract just the filenames from the full paths
    filenames = [os.path.basename(f) for f in files]
    
    return render_template('preprocessing/unmapped.html', 
                         files=filenames,
                         unmapped_count=len(filenames))

@preprocessing_bp.route('/unmapped/<filename>')
def view_unmapped_file(filename):
    # Construct the full S3 key
    key = f'data/hcfa_json/valid/unmapped/{filename}'
    
    try:
        # Get the JSON data
        json_data = get_s3_json(key)
        unmapped_count = get_unmapped_files_count()
        
        # Get list of all unmapped files
        prefix = 'data/hcfa_json/valid/unmapped/'
        all_files = [os.path.basename(f) for f in list_objects(prefix)]
        
        # Find current file index
        current_index = all_files.index(filename)
        
        # Get next and previous filenames
        next_file = all_files[current_index + 1] if current_index < len(all_files) - 1 else None
        prev_file = all_files[current_index - 1] if current_index > 0 else None
        
        return render_template('preprocessing/edit_unmapped.html', 
                             filename=filename,
                             json_data=json_data,
                             unmapped_count=unmapped_count,
                             next_file=next_file,
                             prev_file=prev_file,
                             current_index=current_index + 1,  # 1-based index for display
                             total_files=len(all_files))
    except Exception as e:
        return render_template('preprocessing/error.html', error=str(e))

@preprocessing_bp.route('/unmapped/<filename>/submit', methods=['POST'])
def submit_unmapped_file(filename):
    form_data = request.form.to_dict(flat=False)
    action = form_data.get('action', ['save'])[0]
    
    # Initialize the JSON structure
    json_data = {
        'patient_info': {},
        'billing_info': {},
        'service_lines': []
    }
    
    # Process patient info
    for key, value in form_data.items():
        if key.startswith('patient_info['):
            field = re.match(r'patient_info\[(.*?)\]', key).group(1)
            json_data['patient_info'][field] = value[0]
    
    # Process billing info
    for key, value in form_data.items():
        if key.startswith('billing_info['):
            field = re.match(r'billing_info\[(.*?)\]', key).group(1)
            json_data['billing_info'][field] = value[0]
    
    # Process service lines
    service_line_pattern = re.compile(r'service_lines\[(\d+)\]\[(.*?)\]')
    service_lines = {}
    
    for key, value in form_data.items():
        if key.startswith('service_lines['):
            match = service_line_pattern.match(key)
            if match:
                index = int(match.group(1))
                field = match.group(2)
                
                # Initialize this service line if it doesn't exist
                if index not in service_lines:
                    service_lines[index] = {}
                
                # Handle modifiers specially - convert comma-separated string to array
                if field == 'modifiers':
                    modifiers = value[0].split(',') if value[0] else []
                    # Remove empty strings that might come from trailing commas
                    modifiers = [m.strip() for m in modifiers if m.strip()]
                    service_lines[index][field] = modifiers
                else:
                    service_lines[index][field] = value[0]
    
    # Convert service_lines dict to list, maintaining order
    json_data['service_lines'] = [
        service_lines[i] for i in sorted(service_lines.keys())
    ]
    
    try:
        unmapped_key = f'data/hcfa_json/valid/unmapped/{filename}'
        
        if action == 'approve':
            # Move to valid folder
            valid_key = f'data/hcfa_json/valid/{filename}'
            upload_json_to_s3(json_data, valid_key)
            move(unmapped_key, valid_key)  # This will copy and delete the original
        else:
            # Save changes back to unmapped folder
            upload_json_to_s3(json_data, unmapped_key)
        
        # Redirect back to the unmapped files list
        return redirect(url_for('preprocessing.list_unmapped_files'))
        
    except Exception as e:
        return render_template('preprocessing/error.html', 
                             error=f"Failed to {action} file: {str(e)}")

@preprocessing_bp.route('/unmapped/<filename>/pdf')
def get_unmapped_pdf_url(filename):
    try:
        # Convert json filename to pdf filename
        pdf_filename = filename.replace('.json', '.pdf')
        pdf_key = f'data/hcfa_pdf/archived/{pdf_filename}'
        
        # Generate presigned URL that expires in 1 hour
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': pdf_key,
                'ResponseContentType': 'application/pdf'
            },
            ExpiresIn=3600
        )
        return jsonify({'url': url})
    except Exception as e:
        logger.error(f"Error generating PDF URL: {str(e)}")
        return jsonify({'error': str(e)}), 404

@preprocessing_bp.route('/unmapped/<filename>/preview/<section>')
def get_unmapped_preview_url(filename, section):
    try:
        # Get base filename without extension
        base_filename = filename.replace('.json', '')
        preview_key = f'data/hcfa_pdf/preview/{base_filename}/{section}.png'
        
        # Generate presigned URL that expires in 1 hour
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': preview_key,
                'ResponseContentType': 'image/png'
            },
            ExpiresIn=3600
        )
        return jsonify({'url': url})
    except Exception as e:
        logger.error(f"Error generating preview URL: {str(e)}")
        return jsonify({'error': str(e)}), 404

@preprocessing_bp.route('/unmapped/<filename>/search', methods=['POST'])
def search_filemaker_orders(filename):
    """
    Search for FileMaker orders matching the given patient information.
    Accepts last_name, first_name, and dos via form or JSON body.
    All parameters are optional - will match any orders that match the provided criteria.
    """
    try:
        # Get search parameters from request
        if request.is_json:
            data = request.get_json()
            last_name = data.get('last_name', '')
            first_name = data.get('first_name', '')
            dos = data.get('dos', '')
        else:
            data = request.form
            last_name = data.get('last_name', '')
            first_name = data.get('first_name', '')
            dos = data.get('dos', '')

        # Import here to avoid circular imports
        from utils.filemaker_lookup import search_orders

        # Search for matching orders
        results = search_orders(
            last_name=last_name,
            first_name=first_name,
            dos=dos
        )

        return jsonify({
            'success': True,
            'matches': results
        })

    except ValueError as e:
        return jsonify({
            'success': False,
            'error': 'Invalid date format',
            'details': str(e)
        }), 400
    except Exception as e:
        logger.error(f"Error searching FileMaker orders: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Search failed',
            'details': str(e)
        }), 500

@preprocessing_bp.route('/unmapped/<filename>/assign', methods=['POST'])
def assign_filemaker_order(filename):
    """
    Assign a FileMaker order to an unmapped file.
    Accepts order_id via form or JSON body.
    Updates the JSON with mapping info and moves the file to mapped directory.
    """
    try:
        # Get order_id from request
        if request.is_json:
            data = request.get_json()
            order_id = data.get('order_id')
        else:
            order_id = request.form.get('order_id')

        # Validate required parameter
        if not order_id:
            return jsonify({
                'success': False,
                'error': 'Missing required parameter',
                'details': 'order_id is required'
            }), 400

        # Get the current JSON data
        unmapped_key = f'data/hcfa_json/valid/unmapped/{filename}'
        json_data = get_s3_json(unmapped_key)

        # Get FileMaker number from orders.parquet
        filemaker_number = order_id  # Default to order_id if not found
        try:
            # Create temporary file for orders.parquet
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
                # Download orders.parquet
                download('data/filemaker/orders.parquet', temp_file.name)
                
                # Read orders.parquet
                orders_df = pd.read_parquet(temp_file.name)
                
                # Find matching order
                matching_order = orders_df[orders_df['Order_ID'] == order_id]
                if not matching_order.empty:
                    # Get FileMaker number if available
                    filemaker_number = matching_order.iloc[0].get('FileMaker_Record_Number', order_id)
        except Exception as e:
            logger.warning(f"Could not get FileMaker number from orders.parquet: {str(e)}")
            # Continue with order_id as fallback

        # Add mapping info
        json_data['mapping_info'] = {
            'order_id': order_id,
            'filemaker_number': filemaker_number,
            'mapping_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Save to mapped location
        mapped_key = f'data/hcfa_json/valid/mapped/{filename}'
        upload_json_to_s3(json_data, mapped_key)

        # Delete the old file
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=unmapped_key)
        except Exception as e:
            logger.error(f"Error deleting old file: {str(e)}")
            # Continue even if delete fails

        return jsonify({
            'success': True,
            'message': f'Successfully assigned FileMaker order {order_id} to {filename}'
        })

    except Exception as e:
        logger.error(f"Error assigning FileMaker order: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'details': str(e)
        }), 500

@preprocessing_bp.route('/stage_mapped_files', methods=['POST'])
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