from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, move
import os
import re
import boto3
import json
from pathlib import Path
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Blueprint
processing_bp = Blueprint('processing', __name__)

# Initialize S3 client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    logger.info("Successfully connected to S3")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {str(e)}")
    s3_client = None

S3_BUCKET = os.getenv('S3_BUCKET')

def get_fail_files_count():
    """Count the number of JSON files in the fails directory."""
    prefix = 'data/hcfa_json/valid/mapped/staging/fails/'
    files = list_objects(prefix)
    return len([f for f in files if f.endswith('.json')])

def get_mapped_files_count():
    """Count the number of JSON files in the mapped directory."""
    prefix = 'data/hcfa_json/valid/mapped/'
    files = list_objects(prefix)
    return len([f for f in files if f.endswith('.json') and 'staging' not in f])

@processing_bp.route('/')
def processing():
    """Render the processing page with file counts."""
    fails_count = get_fail_files_count()
    mapped_count = get_mapped_files_count()
    return render_template('processing.html', 
                        mapped_count=mapped_count,
                        fails_count=fails_count)

@processing_bp.route('/fails')
def list_fail_files():
    """List all files that failed processing validation."""
    # Get list of files from S3
    prefix = 'data/hcfa_json/valid/mapped/staging/fails/'
    files = list_objects(prefix)
    
    # Extract just the filenames from the full paths
    filenames = [os.path.basename(f) for f in files if f.endswith('.json')]
    
    return render_template('processing/fails.html', 
                        files=filenames,
                        fails_count=len(filenames))

@processing_bp.route('/fails/<filename>')
def view_fail_file(filename):
    """View a specific file that failed processing validation."""
    # Construct the full S3 key
    key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
    
    try:
        # Get the JSON data
        json_data = get_s3_json(key)
        fails_count = get_fail_files_count()
        
        # Get list of all failed files
        prefix = 'data/hcfa_json/valid/mapped/staging/fails/'
        all_files = [os.path.basename(f) for f in list_objects(prefix) if f.endswith('.json')]
        
        # Find current file index
        try:
            current_index = all_files.index(filename) + 1  # 1-based index for display
        except ValueError:
            current_index = 1
        
        # Get next and previous filenames
        next_file = all_files[current_index] if current_index < len(all_files) else None
        prev_file = all_files[current_index - 2] if current_index > 1 else None
        
        # Normalize the JSON data structure if needed fields are missing
        if 'filemaker' not in json_data:
            json_data['filemaker'] = {'order': {}, 'line_items': [], 'provider': {}}
        elif 'order' not in json_data['filemaker']:
            json_data['filemaker']['order'] = {}
        elif 'line_items' not in json_data['filemaker']:
            json_data['filemaker']['line_items'] = []
        elif 'provider' not in json_data['filemaker']:
            json_data['filemaker']['provider'] = {}
            
        # Ensure validation_info is present
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {'status': 'UNKNOWN', 'failure_reasons': []}
        
        return render_template('processing/edit_fail.html', 
                              filename=filename,
                              json_data=json_data,
                              fails_count=fails_count,
                              next_file=next_file,
                              prev_file=prev_file,
                              current_index=current_index,
                              total_files=len(all_files))
    except Exception as e:
        logger.error(f"Error viewing failed file {filename}: {str(e)}")
        return render_template('preprocessing/error.html', error=str(e))

@processing_bp.route('/fails/<filename>/submit', methods=['POST'])
def submit_fail_file(filename):
    """Submit updates to a failed file."""
    form_data = request.form.to_dict(flat=False)
    action = form_data.get('action', ['save'])[0]
    
    # Get the current JSON data
    key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
    try:
        json_data = get_s3_json(key)
        
        # Process service lines
        service_line_pattern = re.compile(r'service_lines\[(\d+)\]\[(.*?)\]')
        service_lines = {}
        
        for form_key, value in form_data.items():
            if form_key.startswith('service_lines['):
                match = service_line_pattern.match(form_key)
                if match:
                    index = int(match.group(1))
                    field = match.group(2)
                    
                    # Initialize this service line if it doesn't exist
                    if index not in service_lines:
                        service_lines[index] = json_data['service_lines'][index].copy() if index < len(json_data['service_lines']) else {}
                    
                    # Handle modifiers specially - convert comma-separated string to array
                    if field == 'modifiers':
                        modifiers = value[0].split(',') if value[0] else []
                        # Remove empty strings that might come from trailing commas
                        modifiers = [m.strip() for m in modifiers if m.strip()]
                        service_lines[index][field] = modifiers
                    else:
                        service_lines[index][field] = value[0]
        
        # Update service lines in JSON data
        json_data['service_lines'] = [
            service_lines[i] for i in sorted(service_lines.keys())
        ]
        
        # Handle different actions
        if action == 'back_to_staging':
            # Move back to regular staging folder
            dest_key = f'data/hcfa_json/valid/mapped/staging/{filename}'
            upload_json_to_s3(json_data, dest_key)
            
            # Delete from fails folder
            s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
            
            flash('File has been moved back to staging.', 'success')
        else:
            # Regular save - keep in fails folder
            upload_json_to_s3(json_data, key)
            flash('Changes saved successfully.', 'success')
        
        # Redirect back to the fails files list
        return redirect(url_for('processing.list_fail_files'))
        
    except Exception as e:
        logger.error(f"Error processing failed file {filename}: {str(e)}", exc_info=True)
        return render_template('preprocessing/error.html', 
                              error=f"Failed to {action} file: {str(e)}")

@processing_bp.route('/fails/<filename>/pdf')
def get_fail_pdf_url(filename):
    """Generate a presigned URL for viewing the original PDF."""
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

@processing_bp.route('/fails/<filename>/override', methods=['POST'])
def override_fail_file(filename):
    """Handle the override and pay functionality for failed files."""
    try:
        # Get form data
        override_rates = {}
        for key, value in request.form.items():
            if key.startswith('override_rate['):
                index = int(key.replace('override_rate[', '').replace(']', ''))
                override_rates[index] = float(value)
        
        override_reason = request.form.get('override_reason', 'No reason provided')
        
        # Get current JSON data
        key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(key)
        
        # Create override object
        if 'override' not in json_data:
            json_data['override'] = {}
        
        # Set override rates for each service line
        json_data['override']['rates'] = []
        for i, line in enumerate(json_data.get('service_lines', [])):
            if i in override_rates:
                override_rate = override_rates[i]
                json_data['override']['rates'].append({
                    'cpt_code': line.get('cpt_code', ''),
                    'billed_amount': line.get('charge_amount', '$0.00'),
                    'override_rate': f"${override_rate:.2f}",
                    'units': line.get('units', 1)
                })
        
        # Set override details
        json_data['override']['reason'] = override_reason
        json_data['override']['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        json_data['override']['user'] = 'admin_user@clarity-dx.com'  # TODO: Replace with actual user
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'OVERRIDE'
        
        # Save to success folder
        dest_key = f'data/hcfa_json/valid/mapped/staging/success/{filename}'
        upload_json_to_s3(json_data, dest_key)
        
        # Delete from fails folder
        s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
        
        return jsonify({
            'success': True,
            'message': 'Override approved successfully'
        })
    
    except Exception as e:
        logger.error(f"Error processing override for {filename}: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500