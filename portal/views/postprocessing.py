import os
import json
import shutil
from flask import Blueprint, render_template, flash, redirect, request, url_for, current_app
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, move
from utils.summary_manager import remove_from_summary, update_summary, get_summary_entry
import re
import boto3
import sqlite3
from pathlib import Path
from datetime import datetime
import logging
import urllib.parse
from flask import session
from typing import Dict, List
from contextlib import contextmanager

# Define BASE_DIR at the module level
BASE_DIR = Path(__file__).resolve().parents[2]  # Assumes views/postprocessing.py is two levels down from project root

# Database path - filemaker.db is in the root directory, not in data/
FILEMAKER_DB = BASE_DIR / "filemaker.db"

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize Blueprint
postprocessing_bp = Blueprint('postprocessing', __name__)

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

def get_s3_json(key):
    """Get JSON data from S3."""
    try:
        logger.info(f"Attempting to get JSON from S3 with key: {key}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        logger.info(f"Successfully retrieved object from S3")
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Successfully parsed JSON data")
        return data
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found in S3: {key}")
        return None
    except Exception as e:
        logger.error(f"Error getting JSON from S3: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Error details: {str(e)}")
        return None

def upload_json_to_s3(json_data, key):
    """Upload JSON data to S3."""
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(json_data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        return True
    except Exception as e:
        logger.error(f"Error uploading JSON to S3: {str(e)}")
        return False

def get_fail_files_count():
    """Count the number of JSON files in the fails directory."""
    prefix = 'data/hcfa_json/valid/mapped/readyforprocess/fails/'
    files = list_objects(prefix)
    return len([f for f in files if f.endswith('.json')])

def get_mapped_files_count():
    """Count the number of JSON files in the mapped directory."""
    prefix = 'data/hcfa_json/valid/mapped/readyforprocess/'
    files = list_objects(prefix)
    return len([f for f in files if f.endswith('.json') and 'fails' not in f])

@postprocessing_bp.route('/')
def index():
    """View postprocessing dashboard."""
    try:
        # Read postprocess_fails_summary.json for stats
        stats = {
            'total_files': 0,
            'failure_types': {}
        }
        
        summary_path = os.path.join(current_app.root_path, 'data', 'dashboard', 'postprocessing_failed_summary.json')
        if os.path.exists(summary_path):
            with open(summary_path, 'r') as f:
                summary = json.load(f)
                
            stats['total_files'] = len(summary)
            
            # Count failure types
            for file in summary:
                for failure in file.get('failure_types', []):
                    stats['failure_types'][failure] = stats['failure_types'].get(failure, 0) + 1

        # Get current counts from S3
        fails_count = get_fail_files_count()
        mapped_count = get_mapped_files_count()

        return render_template('postprocessing.html', 
                            stats=stats,
                            mapped_count=mapped_count,
                            fails_count=fails_count)

    except Exception as e:
        current_app.logger.error(f"Error loading postprocessing dashboard: {str(e)}")
        flash('Error loading dashboard', 'error')
        return redirect(url_for('home.home'))

@postprocessing_bp.route('/summary')
def summary_dashboard():
    """Render the summary dashboard page."""
    json_path = BASE_DIR / "data" / "dashboard" / "postprocessing_failed_summary.json"
    
    try:
        with open(json_path, "r") as f:
            summary_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading summary data: {str(e)}")
        summary_data = []
    
    return render_template('postprocessing/summary.html', summary_json=summary_data)

@postprocessing_bp.route('/fails')
def list_fail_files():
    """List all files that failed postprocessing validation."""
    json_path = BASE_DIR / "data" / "dashboard" / "postprocessing_failed_summary.json"
    
    try:
        with open(json_path, "r") as f:
            all_files = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading summary data: {str(e)}")
        all_files = []
    
    # Get filter parameters from request or session
    filter_params = get_filter_params_from_request(request)
    
    # Apply filters
    files = all_files
    
    # Filter by filenames if specified
    filenames_param = filter_params.get('filenames')
    if filenames_param:
        selected = set(f.strip() for f in str(filenames_param).split(",") if f.strip())
        files = [f for f in files if f.get("filename") in selected]
    
    # Filter by type if specified
    type_param = filter_params.get('type')
    if type_param and type_param != "All Types":
        files = [f for f in files if type_param in f.get("failure_types", [])]
    
    # Filter by provider if specified
    provider_param = filter_params.get('provider')
    if provider_param and provider_param != "All Providers":
        files = [f for f in files if f.get("provider") == provider_param]
    
    # Filter by FileMaker status if specified
    filemaker_param = filter_params.get('filemaker')
    if filemaker_param and filemaker_param != "All":
        if filemaker_param == "needs_correction":
            files = [f for f in files if f.get("provider_validation", {}).get("is_valid") is False]
        elif filemaker_param == "valid":
            files = [f for f in files if f.get("provider_validation", {}).get("is_valid") is True]
    
    # Filter by age if specified
    age_param = filter_params.get('age')
    if age_param and age_param != "All Dates":
        if age_param == "0–30 days":
            files = [f for f in files if f.get("age_days", 0) <= 30]
        elif age_param == "31–60 days":
            files = [f for f in files if 30 < f.get("age_days", 0) <= 60]
        elif age_param == "60+ days":
            files = [f for f in files if f.get("age_days", 0) > 60]
    
    # Filter by search term if specified
    search_param = filter_params.get('q')
    if search_param:
        search_term = search_param.lower()
        files = [f for f in files if search_term in f.get("filename", "").lower()]
    
    # Check if request wants JSON format
    if request.args.get('format') == 'json':
        return jsonify({
            'success': True,
            'bills': files,
            'count': len(files),
            'filters': filter_params
        })
    
    return render_template(
        "postprocessing/fails.html",
        bills_json=files,
        fails_count=len(files),
        filter_params=filter_params
    )

@postprocessing_bp.route('/fails/<filename>/pdf')
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

def get_filter_params_from_request(request):
    """Extract filter parameters from request."""
    filter_params = {}
    
    # Check form data first
    if request.form and request.form.get('filter_params'):
        try:
            filter_params = json.loads(request.form.get('filter_params'))
        except (json.JSONDecodeError, TypeError):
            pass
            
    # Check query params
    for param in ['type', 'provider', 'age', 'q', 'filenames']:
        if param in request.args:
            filter_params[param] = request.args.get(param)
    
    # Check referrer as last resort
    if not filter_params:
        referrer = request.referrer or ""
        try:
            parsed_url = urllib.parse.urlparse(referrer)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            for param in ['type', 'provider', 'age', 'q', 'filenames']:
                if param in query_params:
                    filter_params[param] = query_params[param][0]
        except Exception:
            pass
    
    # Store in session for persistence
    if filter_params:
        session['filter_params'] = filter_params
    
    return filter_params

def build_redirect_url(url, filter_params):
    """Build a redirect URL with filter parameters."""
    if not filter_params:
        return url
        
    # Convert filter params to query string
    query_parts = []
    for key, value in filter_params.items():
        if value:
            query_parts.append(f"{key}={urllib.parse.quote(str(value))}")
    
    if query_parts:
        return f"{url}?{'&'.join(query_parts)}"
    return url

@postprocessing_bp.route('/fails/<filename>/move-to-readyforprocess', methods=['POST'])
def move_to_readyforprocess(filename):
    """Move a file from fails to readyforprocess directory."""
    try:
        # Source and destination paths
        source_key = f'data/hcfa_json/valid/mapped/readyforprocess/fails/{filename}'
        dest_key = f'data/hcfa_json/valid/mapped/readyforprocess/{filename}'
        
        # Move the file
        success = move(source_key, dest_key)
        
        if success:
            # Update summary
            remove_from_summary(filename, 'postprocessing_failed_summary.json')
            update_summary(filename, 'postprocessing_summary.json')
            
            flash('File moved successfully to readyforprocess directory', 'success')
            return jsonify({'success': True})
        else:
            flash('Failed to move file', 'error')
            return jsonify({'success': False, 'error': 'Failed to move file'}), 500

    except Exception as e:
        logger.error(f"Error moving file: {str(e)}")
        flash(f'Error moving file: {str(e)}', 'error')
        return jsonify({'success': False, 'error': str(e)}), 500

@postprocessing_bp.route('/fails/<filename>/send-to-garbage', methods=['POST'])
def send_to_garbage(filename):
    """Move a file to the garbage directory."""
    try:
        # Get the reason from the request
        data = request.get_json()
        reason = data.get('garbage_reason')
        
        if not reason:
            return jsonify({'success': False, 'error': 'No reason provided'}), 400
        
        # Source and destination paths
        source_key = f'data/hcfa_json/valid/mapped/readyforprocess/fails/{filename}'
        dest_key = f'data/hcfa_json/valid/mapped/readyforprocess/garbage/{filename}'
        
        # Move the file
        success = move(source_key, dest_key)
        
        if success:
            # Update summary
            remove_from_summary(filename, 'postprocessing_failed_summary.json')
            
            # Add to garbage summary
            garbage_entry = {
                'filename': filename,
                'reason': reason,
                'date_moved': datetime.now().isoformat()
            }
            update_summary(garbage_entry, 'postprocessing_garbage_summary.json')
            
            flash('File moved to garbage successfully', 'success')
            return jsonify({'success': True})
        else:
            flash('Failed to move file to garbage', 'error')
            return jsonify({'success': False, 'error': 'Failed to move file'}), 500
            
    except Exception as e:
        logger.error(f"Error moving file to garbage: {str(e)}")
        flash(f'Error moving file to garbage: {str(e)}', 'error')
        return jsonify({'success': False, 'error': str(e)}), 500

@postprocessing_bp.route('/fails/<filename>/deny', methods=['POST'])
def deny_fail_file(filename):
    """Deny a file and move it to the denied directory."""
    try:
        # Get the reason from the request
        data = request.get_json()
        reason = data.get('denial_reason')
        
        if not reason:
            return jsonify({'success': False, 'error': 'No reason provided'}), 400
        
        # Source and destination paths
        source_key = f'data/hcfa_json/valid/mapped/readyforprocess/fails/{filename}'
        dest_key = f'data/hcfa_json/valid/mapped/readyforprocess/denied/{filename}'
        
        # Move the file
        success = move(source_key, dest_key)
        
        if success:
            # Update summary
            remove_from_summary(filename, 'postprocessing_failed_summary.json')
            
            # Add to denied summary
            denied_entry = {
                'filename': filename,
                'reason': reason,
                'date_denied': datetime.now().isoformat()
            }
            update_summary(denied_entry, 'postprocessing_denied_summary.json')
            
            flash('File denied successfully', 'success')
            return jsonify({'success': True})
        else:
            flash('Failed to deny file', 'error')
            return jsonify({'success': False, 'error': 'Failed to move file'}), 500

    except Exception as e:
        logger.error(f"Error denying file: {str(e)}")
        flash(f'Error denying file: {str(e)}', 'error')
        return jsonify({'success': False, 'error': str(e)}), 500

@postprocessing_bp.route('/fails/<filename>/escalate', methods=['POST'])
def escalate_fail_file(filename):
    """Escalate a file and move it to the escalated directory."""
    try:
        # Get the reason from the request
        data = request.get_json()
        reason = data.get('escalation_reason')
        
        if not reason:
            return jsonify({'success': False, 'error': 'No reason provided'}), 400
        
        # Source and destination paths
        source_key = f'data/hcfa_json/valid/mapped/readyforprocess/fails/{filename}'
        dest_key = f'data/hcfa_json/valid/mapped/readyforprocess/escalated/{filename}'
        
        # Move the file
        success = move(source_key, dest_key)
        
        if success:
            # Update summary
            remove_from_summary(filename, 'postprocessing_failed_summary.json')
            
            # Add to escalated summary
            escalated_entry = {
                'filename': filename,
                'reason': reason,
                'date_escalated': datetime.now().isoformat()
            }
            update_summary(escalated_entry, 'postprocessing_escalated_summary.json')
            
            flash('File escalated successfully', 'success')
            return jsonify({'success': True})
        else:
            flash('Failed to escalate file', 'error')
            return jsonify({'success': False, 'error': 'Failed to move file'}), 500

    except Exception as e:
        logger.error(f"Error escalating file: {str(e)}")
        flash(f'Error escalating file: {str(e)}', 'error')
        return jsonify({'success': False, 'error': str(e)}), 500

@postprocessing_bp.route('/fails/<filename>')
def view_fail_file(filename):
    """View and edit a failed readyforprocess file."""
    try:
        # Get filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get the JSON data for this file
        key = f'data/hcfa_json/readyforprocess/fails/{filename}'
        json_data = get_s3_json(key)
        
        if not json_data:
            flash('File not found', 'error')
            return redirect(url_for('postprocessing.list_fail_files'))
        
        # Get list of all failed files for navigation
        json_path = BASE_DIR / "data" / "dashboard" / "postprocessing_failed_summary.json"
        try:
            with open(json_path, "r") as f:
                all_files = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading summary data: {str(e)}")
            all_files = []
        
        # Filter files based on filter parameters
        files = all_files
        if filter_params:
            # Filter by filenames if specified
            filenames_param = filter_params.get('filenames')
            if filenames_param:
                selected = set(f.strip() for f in str(filenames_param).split(",") if f.strip())
                files = [f for f in files if f.get("filename") in selected]
            
            # Filter by type if specified
            type_param = filter_params.get('type')
            if type_param and type_param != "All Types":
                files = [f for f in files if type_param in f.get("failure_types", [])]
            
            # Filter by provider if specified
            provider_param = filter_params.get('provider')
            if provider_param and provider_param != "All Providers":
                files = [f for f in files if f.get("provider") == provider_param]
            
            # Filter by FileMaker status if specified
            filemaker_param = filter_params.get('filemaker')
            if filemaker_param and filemaker_param != "All":
                if filemaker_param == "needs_correction":
                    files = [f for f in files if f.get("provider_validation", {}).get("is_valid") is False]
                elif filemaker_param == "valid":
                    files = [f for f in files if f.get("provider_validation", {}).get("is_valid") is True]
            
            # Filter by age if specified
            age_param = filter_params.get('age')
            if age_param and age_param != "All Dates":
                if age_param == "0–30 days":
                    files = [f for f in files if f.get("age_days", 0) <= 30]
                elif age_param == "31–60 days":
                    files = [f for f in files if 30 < f.get("age_days", 0) <= 60]
                elif age_param == "60+ days":
                    files = [f for f in files if f.get("age_days", 0) > 60]
            
            # Filter by search term if specified
            search_param = filter_params.get('q')
            if search_param:
                search_term = search_param.lower()
                files = [f for f in files if search_term in f.get("filename", "").lower()]
        
        # Get navigation info
        filenames = [f.get("filename") for f in files if f.get("filename")]
        current_index = next((i for i, f in enumerate(filenames) if f == filename), 0) + 1
        total_files = len(filenames)
        
        prev_file = filenames[current_index - 2] if current_index > 1 else None
        next_file = filenames[current_index] if current_index < total_files else None
        
        # Build filter query string for navigation
        filter_query = build_redirect_url('', filter_params)
        
        return render_template(
            'postprocessing/edit_fail.html',
            filename=filename,
            json_data=json_data,
            current_index=current_index,
            total_files=total_files,
            fails_count=len(filenames),
            prev_file=prev_file,
            next_file=next_file,
            filter_query=filter_query,
            filter_params=filter_params
        )

    except Exception as e:
        logger.error(f"Error viewing file {filename}: {str(e)}")
        flash(f"Error viewing file: {str(e)}", 'danger')
        return redirect(url_for('postprocessing.list_fail_files'))

@postprocessing_bp.route('/fails/<filename>/save', methods=['POST'])
def save_fail(filename):
    """Save changes to a failed readyforprocess file."""
    try:
        # Get the current data to preserve any fields not in the form
        key = f'data/hcfa_json/readyforprocess/fails/{filename}'
        current_data = get_s3_json(key)
        if not current_data:
            flash('File not found', 'error')
            return redirect(url_for('postprocessing.list_fail_files'))

        # Update patient info
        if 'patient_info' in request.form:
            current_data['patient_info'] = {
                'patient_name': request.form.get('patient_info[patient_name]', ''),
                'patient_dob': request.form.get('patient_info[patient_dob]', ''),
                'patient_zip': request.form.get('patient_info[patient_zip]', '')
            }

        # Update billing info
        if 'billing_info' in request.form:
            current_data['billing_info'] = {
                'billing_provider_name': request.form.get('billing_info[billing_provider_name]', ''),
                'billing_provider_npi': request.form.get('billing_info[billing_provider_npi]', ''),
                'billing_provider_tin': request.form.get('billing_info[billing_provider_tin]', ''),
                'billing_provider_address': request.form.get('billing_info[billing_provider_address]', ''),
                'patient_account_no': request.form.get('billing_info[patient_account_no]', ''),
                'total_charge': request.form.get('billing_info[total_charge]', '')
            }

        # Update service lines
        if 'service_lines' in request.form:
            service_lines = []
            for i in range(len(current_data.get('service_lines', []))):
                line = {
                    'cpt_code': request.form.get(f'service_lines[{i}][cpt_code]', ''),
                    'charge_amount': request.form.get(f'service_lines[{i}][charge_amount]', ''),
                    'date_of_service': request.form.get(f'service_lines[{i}][date_of_service]', ''),
                    'units': request.form.get(f'service_lines[{i}][units]', ''),
                    'place_of_service': request.form.get(f'service_lines[{i}][place_of_service]', ''),
                    'diagnosis_pointer': request.form.get(f'service_lines[{i}][diagnosis_pointer]', ''),
                    'modifiers': request.form.get(f'service_lines[{i}][modifiers]', '').split(',') if request.form.get(f'service_lines[{i}][modifiers]') else [],
                    'proc_desc': current_data['service_lines'][i].get('proc_desc', ''),
                    'category': current_data['service_lines'][i].get('category', '')
                }
                service_lines.append(line)
            current_data['service_lines'] = service_lines

        # Save the updated data
        success = upload_json_to_s3(current_data, key)

        if success:
            flash('File saved successfully', 'success')
        else:
            flash('Failed to save file', 'error')

        return redirect(url_for('postprocessing.view_fail_file', filename=filename))

    except Exception as e:
        logger.error(f"Error saving file {filename}: {str(e)}")
        flash('Error saving file', 'error')
        return redirect(url_for('postprocessing.view_fail_file', filename=filename))

@postprocessing_bp.route('/fails/<filename>/move', methods=['POST'])
def move_fail(filename):
    """Move a file from fails to readyforprocess directory."""
    try:
        # Source and destination paths in S3
        source_key = f'data/hcfa_json/readyforprocess/fails/{filename}'
        dest_key = f'data/hcfa_json/readyforprocess/{filename}'

        # Get the current data
        json_data = get_s3_json(source_key)
        if not json_data:
            flash('File not found', 'error')
            return redirect(url_for('postprocessing.list_fail_files'))
        
        # Add metadata about the move
        if 'processing_info' not in json_data:
            json_data['processing_info'] = {}
            
        json_data['processing_info'].update({
            'moved_from_fails': True,
            'moved_timestamp': datetime.now().isoformat(),
            'moved_by': session.get('username', 'system'),
        })
        
        # Save to destination
        success = upload_json_to_s3(json_data, dest_key)
        if not success:
            flash('Failed to save file to destination', 'error')
            return redirect(url_for('postprocessing.list_fail_files'))
        
        # Delete from source
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
        except Exception as e:
            logger.error(f"Error deleting source file: {str(e)}")
            flash('Warning: Could not delete source file', 'warning')
        
        # Update summary
        remove_from_summary(filename, 'postprocessing_failed_summary.json')
        update_summary(filename, 'postprocessing_summary.json')

        flash('File moved to ready for process', 'success')
        return redirect(url_for('postprocessing.list_fail_files'))

    except Exception as e:
        logger.error(f"Error moving file {filename}: {str(e)}")
        flash('Error moving file', 'error')
        return redirect(url_for('postprocessing.list_fail_files'))

@contextmanager
def get_db_connection():
    """Get a database connection."""
    conn = sqlite3.connect(FILEMAKER_DB)
    try:
        yield conn
    finally:
        conn.close()

def clean_tin(tin):
    """Clean a TIN by removing non-numeric characters."""
    return re.sub(r'\D', '', tin)

def get_cpt_codes_by_category():
    """Get CPT codes grouped by category from the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT cpt_code, category, description
            FROM cpt_codes
            ORDER BY category, cpt_code
        """)
        rows = cursor.fetchall()
        
        codes_by_category = {}
        for row in rows:
            cpt_code, category, description = row
            if category not in codes_by_category:
                codes_by_category[category] = []
            codes_by_category[category].append({
                'code': cpt_code,
                'description': description
            })
        
        return codes_by_category

def update_postprocess_fails_summary(filename, remove=False):
    """Update the postprocess fails summary JSON file."""
    summary_path = BASE_DIR / "data" / "dashboard" / "postprocessing_failed_summary.json"
    
    # Read existing summary
    try:
        with open(summary_path, 'r') as f:
            summary = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading summary data: {str(e)}")
        summary = []
    
    # Remove entry if requested
    if remove:
        summary = [f for f in summary if f.get("filename") != filename]
    
    # Save updated summary
    try:
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving summary data: {str(e)}")
        return False

def filter_files(files, filter_params):
    """Apply filters to files list."""
    if not filter_params:
        return files
        
    filtered_files = files
    
    # Filter by filenames if specified
    filenames_param = filter_params.get('filenames')
    if filenames_param:
        selected = set(f.strip() for f in str(filenames_param).split(",") if f.strip())
        filtered_files = [f for f in filtered_files if f.get("filename") in selected]
    
    # Filter by type if specified
    type_param = filter_params.get('type')
    if type_param and type_param != "All Types":
        filtered_files = [f for f in filtered_files if type_param in f.get("failure_types", [])]
    
    # Filter by provider if specified
    provider_param = filter_params.get('provider')
    if provider_param and provider_param != "All Providers":
        filtered_files = [f for f in filtered_files if f.get("provider") == provider_param]
    
    # Filter by FileMaker status if specified
    filemaker_param = filter_params.get('filemaker')
    if filemaker_param and filemaker_param != "All":
        if filemaker_param == "needs_correction":
            filtered_files = [f for f in filtered_files if f.get("provider_validation", {}).get("is_valid") is False]
        elif filemaker_param == "valid":
            filtered_files = [f for f in filtered_files if f.get("provider_validation", {}).get("is_valid") is True]
    
    # Filter by age if specified
    age_param = filter_params.get('age')
    if age_param and age_param != "All Dates":
        if age_param == "0–30 days":
            filtered_files = [f for f in filtered_files if f.get("age_days", 0) <= 30]
        elif age_param == "31–60 days":
            filtered_files = [f for f in filtered_files if 30 < f.get("age_days", 0) <= 60]
        elif age_param == "60+ days":
            filtered_files = [f for f in filtered_files if f.get("age_days", 0) > 60]
    
    # Filter by search term if specified
    search_param = filter_params.get('q')
    if search_param:
        search_term = search_param.lower()
        filtered_files = [f for f in filtered_files if search_term in f.get("filename", "").lower()]
    
    return filtered_files

def get_navigation_info(filenames, current_filename):
    """Get current position and adjacent files for navigation."""
    try:
        current_index = filenames.index(current_filename) + 1
        prev_file = filenames[current_index - 2] if current_index > 1 else None
        next_file = filenames[current_index] if current_index < len(filenames) else None
    except (ValueError, IndexError):
        current_index = 0
        prev_file = None
        next_file = None
    
    return current_index, prev_file, next_file

def build_filter_query(filter_params):
    """Build filter query string for URL."""
    if not filter_params:
        return ""
        
    query_parts = []
    for key, value in filter_params.items():
        if value:
            query_parts.append(f"{key}={urllib.parse.quote(str(value))}")
    
    return "?" + "&".join(query_parts) if query_parts else "" 