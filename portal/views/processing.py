from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, move
from utils.summary_manager import remove_from_summary, update_summary, get_summary_entry
import os
import re
import boto3
import sqlite3
import json
from pathlib import Path
from datetime import datetime
import logging
from flask import current_app
import pprint
from flask import Blueprint
import urllib.parse
from flask import session
from typing import Dict, List
from contextlib import contextmanager

# Define BASE_DIR at the module level
BASE_DIR = Path(__file__).resolve().parents[2]  # Assumes views/processing.py is two levels down from project root

# Database path - filemaker.db is in the root directory, not in data/
FILEMAKER_DB = BASE_DIR / "filemaker.db"

@contextmanager
def get_db_connection():
    """Context manager for database connections with explicit logging."""
    logger.info(f"Opening database connection to {FILEMAKER_DB}")
    try:
        conn = sqlite3.connect(FILEMAKER_DB)
        logger.info("Database connection established successfully")
        yield conn
    except sqlite3.Error as e:
        logger.error(f"SQLite error during connection: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error in database connection: {str(e)}", exc_info=True)
        raise
    finally:
        if 'conn' in locals():
            logger.info("Closing database connection")
            conn.close()

def get_cpt_codes_by_category() -> Dict[str, List[str]]:
    """
    Query the dim_proc table to get all CPT codes associated with each category.
    
    Returns:
        Dict[str, List[str]]: A dictionary mapping category names to lists of CPT codes.
        Example:
        {
            'mri_wo': ['70551', '70540', ...],
            'mri_w': ['70552', '70542', ...],
            'mri_wwo': ['70553', '70543', ...],
            'ct_wo': ['70450', '71250', ...],
            'ct_w': ['70460', '71260', ...],
            'ct_wwo': ['70470', '71270', ...],
            'xray': ['71045', '71046', ...],
            'ultrasound': ['76536', '76604', ...]
        }
    """
    category_mapping = {
        'mri_wo': 'MRI w/o',
        'mri_w': 'MRI w/',
        'mri_wwo': 'MRI w/&w/o',
        'ct_wo': 'CT w/o',
        'ct_w': 'CT w/',
        'ct_wwo': 'CT w/&w/o',
        'xray': 'XRAY',
        'ultrasound': 'ULTRASOUND'
    }
    
    result = {category: [] for category in category_mapping.keys()}
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Query for each category
            for category_key, category_name in category_mapping.items():
                cursor.execute("""
                    SELECT DISTINCT proc_cd 
                    FROM dim_proc 
                    WHERE category = ? 
                    ORDER BY proc_cd
                """, (category_name,))
                
                cpt_codes = [row[0] for row in cursor.fetchall()]
                result[category_key] = cpt_codes
                
                # Log the count of CPT codes found for each category
                logger.info(f"Found {len(cpt_codes)} CPT codes for category {category_name}")
        
        return result
        
    except sqlite3.Error as e:
        logger.error(f"Database error in get_cpt_codes_by_category: {str(e)}")
        return result
    except Exception as e:
        logger.error(f"Unexpected error in get_cpt_codes_by_category: {str(e)}")
        return result

# Set up logging
logging.basicConfig(level=logging.DEBUG)
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

@processing_bp.route('/summary')
def summary_dashboard():
    """Render the summary dashboard page."""
    json_path = BASE_DIR / "data" / "dashboard" / "failed_summary.json"
    
    try:
        with open(json_path, "r") as f:
            summary_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading summary data: {str(e)}")
        summary_data = []
    
    return render_template('processing/summary.html', summary_json=summary_data)

@processing_bp.route('/fails')
def list_fail_files():
    """List all files that failed processing validation."""
    json_path = BASE_DIR / "data" / "dashboard" / "failed_summary.json"
    
    try:
        with open(json_path, "r") as f:
            # Handle potential JSON decode errors during loading
            try:
                all_files = json.load(f)
                if not isinstance(all_files, list):
                    logger.error(f"Expected a list in {json_path}, got {type(all_files)}.")
                    all_files = [] # Default to empty list if not a list
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from {json_path}: {e}")
                all_files = [] # Default to empty list on decode error
    except FileNotFoundError:
        logger.error(f"Failed summary JSON not found at: {json_path}")
        all_files = [] # Default to empty list if file not found
    except Exception as e:
        logger.error(f"Unexpected error reading {json_path}: {e}")
        all_files = [] # Default to empty list on other errors

    # Get filter parameters from request or session
    filter_params = get_filter_params_from_request(request) 
    
    # Apply filters
    files = all_files
    
    # Filter by filenames if specified
    filenames_param = filter_params.get('filenames')
    if filenames_param:
        # Ensure filenames_param is treated as a string before splitting
        selected = set(f.strip() for f in str(filenames_param).split(",") if f.strip())
        # Filter safely using .get()
        files = [f for f in files if isinstance(f, dict) and f.get("filename") in selected]
    
    # Filter by type if specified
    type_param = filter_params.get('type')
    if type_param and type_param != "All Types":
        files = [f for f in files if isinstance(f, dict) and type_param in f.get("failure_types", [])]
    
    # Filter by provider if specified
    provider_param = filter_params.get('provider')
    if provider_param and provider_param != "All Providers":
        files = [f for f in files if isinstance(f, dict) and f.get("provider") == provider_param]
    
    # Filter by age if specified
    age_param = filter_params.get('age')
    if age_param and age_param != "All Dates":
        if age_param == "0–30 days":
            files = [f for f in files if isinstance(f, dict) and f.get("age_days", 0) <= 30]
        elif age_param == "31–60 days":
            files = [f for f in files if isinstance(f, dict) and 30 < f.get("age_days", 0) <= 60]
        elif age_param == "60+ days":
            files = [f for f in files if isinstance(f, dict) and f.get("age_days", 0) > 60]
    
    # Filter by search term if specified
    search_param = filter_params.get('q')
    if search_param:
        search_term = search_param.lower()
        files = [f for f in files if isinstance(f, dict) and search_term in f.get("filename", "").lower()]
    
    # Check if request wants JSON format
    if request.args.get('format') == 'json':
        return jsonify({
            'success': True,
            'bills': files,
            'count': len(files),
            'filters': filter_params
        })
    
    return render_template(
        "processing/fails.html",
        bills_json=files,
        fails_count=len(files),
        filter_params=filter_params
    )

@processing_bp.route('/fails/<filename>', methods=['GET', 'POST'])
def view_fail_file(filename):
    """View a specific file that failed processing validation."""
    # Construct the full S3 key
    key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
    
    # Get filter parameters from referrer if they exist
    filter_params = {}
    referrer = request.referrer or ""
    if "filenames=" in referrer:
        # Extract the filenames parameter from the referrer URL
        parsed_url = urllib.parse.urlparse(referrer)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        if 'filenames' in query_params:
            filter_params['filenames'] = query_params['filenames'][0]
    
    # Or directly from the request if provided
    for param in ['filenames']:
        if param in request.args:
            filter_params[param] = request.args.get(param)
    
    try:
        # Handle POST request for saving changes
        if request.method == 'POST':
            # Get the JSON data from the request
            form_data = request.form.to_dict()
            json_data = get_s3_json(key)
            
            # Update the JSON data with form values
            if 'category_rates' in form_data:
                category_rates = json.loads(form_data['category_rates'])
                if 'rate_assignment' not in json_data:
                    json_data['rate_assignment'] = {}
                json_data['rate_assignment']['category_rates'] = category_rates
            
            # Save the updated JSON back to S3
            upload_json_to_s3(json_data, key)
            
            flash('Changes saved successfully.', 'success')
            return redirect(url_for('processing.view_fail_file', filename=filename, **filter_params))
        
        # Handle GET request (view file)
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
        
        # If we have filter params, pass them to next/prev navigation as well
        filter_query = ""
        if filter_params:
            filter_query = "?" + "&".join([f"{k}={urllib.parse.quote(v)}" for k, v in filter_params.items() if v])
        
        return render_template('processing/edit_fail.html', 
                              filename=filename,
                              json_data=json_data,
                              fails_count=fails_count,
                              next_file=next_file,
                              prev_file=prev_file,
                              current_index=current_index,
                              total_files=len(all_files),
                              filter_params=filter_params,
                              filter_query=filter_query)
    except Exception as e:
        logger.error(f"Error viewing failed file {filename}: {str(e)}")
        return render_template('preprocessing/error.html', error=str(e))

@processing_bp.route('/fails/<filename>/submit', methods=['POST'])
def submit_fail_file(filename):
    """Submit updates to a failed file."""
    form_data = request.form.to_dict(flat=False)
    action = form_data.get('action', ['save'])[0]
    
    # Get the filter parameters from referrer or form data
    filter_params = {}
    referrer = request.referrer or ""
    
    # Extract filter parameters either from the form or from the referrer URL
    if form_data.get('filter_params'):
        # If filter parameters were passed in the form
        filter_params = json.loads(form_data.get('filter_params', ['{}'])[0])
    elif "filenames=" in referrer:
        # Extract the filenames parameter from the referrer URL
        parsed_url = urllib.parse.urlparse(referrer)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        if 'filenames' in query_params:
            filter_params['filenames'] = query_params['filenames'][0]
    
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
            # Clear validation failures and set status to PENDING
            if 'validation_info' not in json_data:
                json_data['validation_info'] = {}
            json_data['validation_info']['status'] = 'PENDING'
            json_data['validation_info']['failure_reasons'] = []
            
            # Move back to regular staging folder
            dest_key = f'data/hcfa_json/valid/mapped/staging/{filename}'
            upload_json_to_s3(json_data, dest_key)
            
            # Delete from fails folder
            s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
            
            # Remove from failed summary
            try:
                if remove_from_summary(filename):
                    logger.info(f"Removed {filename} from failed summary")
                else:
                    logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
            except Exception as e:
                logger.error(f"Error removing {filename} from summary: {str(e)}")
            
            flash('File has been moved back to staging for reprocessing.', 'success')
        else:
            # Regular save - keep in fails folder
            upload_json_to_s3(json_data, key)
            
            # Check validation status and update summary accordingly
            try:
                # Get current failure reasons
                failure_reasons = json_data.get('validation_info', {}).get('failure_reasons', [])
                
                if not failure_reasons:
                    # All issues fixed - remove from summary file
                    if remove_from_summary(filename):
                        logger.info(f"All failures resolved, removed {filename} from summary")
                    else:
                        logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
                else:
                    # Extract simplified failure types for the summary
                    failure_types = []
                    for reason in failure_reasons:
                        # Extract just the failure type (before the colon if present)
                        failure_type = reason.split(':', 1)[0].strip() if ':' in reason else reason.strip()
                        failure_types.append(failure_type)
                    
                    # Get provider information
                    provider_name = json_data.get('filemaker', {}).get('provider', {}).get('Billing Name', 
                               json_data.get('billing_info', {}).get('billing_provider_name', 'Unknown Provider'))
                    
                    # Get DOS (date of service)
                    dos = None
                    # Try to get DOS from filemaker data first
                    if 'filemaker' in json_data and 'line_items' in json_data['filemaker'] and json_data['filemaker']['line_items']:
                        dos = json_data['filemaker']['line_items'][0].get('DOS')
                    
                    # If not found, try service_lines
                    if not dos and 'service_lines' in json_data and json_data['service_lines']:
                        date_str = json_data['service_lines'][0].get('date_of_service', '')
                        if date_str:
                            # Handle format like "MM/DD/YY - MM/DD/YY" by taking the first date
                            if ' - ' in date_str:
                                date_str = date_str.split(' - ')[0]
                            
                            # Try to parse the date
                            try:
                                # Try different formats
                                from datetime import datetime
                                formats = ['%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d']
                                for fmt in formats:
                                    try:
                                        date_obj = datetime.strptime(date_str.strip(), fmt)
                                        dos = date_obj.strftime('%Y-%m-%d')
                                        break
                                    except ValueError:
                                        continue
                            except Exception as e:
                                logger.warning(f"Could not parse date {date_str}: {str(e)}")
                    
                    # Calculate age in days if DOS was found
                    age_days = 0
                    if dos:
                        try:
                            from datetime import datetime, date
                            date_obj = datetime.strptime(dos, '%Y-%m-%d').date()
                            age_days = (date.today() - date_obj).days
                        except Exception as e:
                            logger.warning(f"Could not calculate age for DOS {dos}: {str(e)}")
                    
                    # Create update data
                    update_data = {
                        'failure_types': failure_types,
                    }
                    
                    # Only add these fields if we have data
                    if provider_name and provider_name != 'Unknown Provider':
                        update_data['provider'] = provider_name
                    if dos:
                        update_data['dos'] = dos
                    if age_days > 0:
                        update_data['age_days'] = age_days
                    
                    # Check if entry exists
                    existing_entry = get_summary_entry(filename)
                    if existing_entry:
                        # Update existing entry
                        if update_summary(filename, update_data):
                            logger.info(f"Updated summary entry for {filename} with new failure types")
                        else:
                            logger.warning(f"Failed to update summary entry for {filename}")
                    else:
                        # Entry doesn't exist, try to create a new one with minimum required fields
                        from utils.summary_manager import add_to_summary
                        if 'provider' not in update_data:
                            update_data['provider'] = 'Unknown Provider'
                        if 'dos' not in update_data:
                            update_data['dos'] = date.today().strftime('%Y-%m-%d')
                        if 'age_days' not in update_data:
                            update_data['age_days'] = 0
                        
                        if add_to_summary(
                            filename=filename,
                            failure_types=update_data['failure_types'],
                            provider=update_data['provider'],
                            dos=update_data['dos'],
                            age_days=update_data['age_days']
                        ):
                            logger.info(f"Added new entry for {filename} to summary")
                        else:
                            logger.warning(f"Failed to add new entry for {filename} to summary")
            except Exception as e:
                logger.error(f"Error updating summary for {filename}: {str(e)}", exc_info=True)
            
            flash('Changes saved successfully.', 'success')
        
        # Redirect back to the fails files list with preserved filter parameters
        if filter_params:
            # Construct the URL with query parameters
            redirect_url = url_for('processing.list_fail_files')
            query_params = []
            for key, value in filter_params.items():
                if value:
                    query_params.append(f"{key}={urllib.parse.quote(value)}")
            
            if query_params:
                redirect_url = f"{redirect_url}?{'&'.join(query_params)}"
            
            return redirect(redirect_url)
        else:
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

# Add these helper functions for preserving navigation
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
            # Include all common filter parameters
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
        
    query_params = []
    for key, value in filter_params.items():
        if value:
            query_params.append(f"{key}={urllib.parse.quote(str(value))}")
    
    if query_params:
        return f"{url}?{'&'.join(query_params)}"
    
    return url

@processing_bp.route('/fails/<filename>/override', methods=['POST'])
def override_fail_file(filename):
    """Handle the override and pay functionality for failed files."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
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
        
        # Remove from failed summary
        try:
            if remove_from_summary(filename):
                logger.info(f"Removed {filename} from failed summary")
            else:
                logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
        except Exception as e:
            logger.error(f"Error removing {filename} from summary: {str(e)}")
        
        # Build response with filter parameters preserved
        redirect_url = url_for('processing.list_fail_files')
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            # For AJAX requests
            response_data = {
                'success': True,
                'message': 'Override approved successfully',
                'redirect': build_redirect_url(redirect_url, filter_params)
            }
            return jsonify(response_data)
        else:
            # For regular form submissions
            flash('Override approved successfully.', 'success')
            return redirect(build_redirect_url(redirect_url, filter_params))
    
    except Exception as e:
        logger.error(f"Error processing override for {filename}: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@processing_bp.route('/fails/<filename>/escalate', methods=['POST'])
def escalate_fail_file(filename):
    """Handle escalation of failed files to support team."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get escalation reason from form
        escalation_reason = request.form.get('reason', 'No reason provided')
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
        # Add escalation info to JSON
        if 'escalation' not in json_data:
            json_data['escalation'] = {}
        
        json_data['escalation'].update({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'reason': escalation_reason,
            'user': request.form.get('user', 'admin_user@clarity-dx.com'),  # TODO: Replace with actual user
            'status': 'PENDING'
        })
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'ESCALATED'
        
        # Save to escalations folder
        dest_key = f'data/hcfa_json/valid/mapped/staging/escalations/{filename}'
        upload_json_to_s3(json_data, dest_key)
        
        # Delete from fails folder
        s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
        
        # Remove from failed summary
        try:
            if remove_from_summary(filename):
                logger.info(f"Removed {filename} from failed summary")
            else:
                logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
        except Exception as e:
            logger.error(f"Error removing {filename} from summary: {str(e)}")
        
        # Create success message
        success_message = f"File {filename} has been escalated to the support team."
        
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            # For AJAX requests
            return jsonify({
                'status': 'success',
                'message': success_message,
                'redirect': build_redirect_url(url_for('processing.list_fail_files'), filter_params)
            })
        else:
            # For regular form submissions
            flash(success_message, 'success')
            return redirect(build_redirect_url(url_for('processing.list_fail_files'), filter_params))
            
    except Exception as e:
        logger.error(f"Error escalating file {filename}: {str(e)}")
        error_message = f"Failed to escalate file: {str(e)}"
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            return jsonify({
                'status': 'error',
                'message': error_message
            }), 500
        else:
            flash(error_message, 'danger')
            return redirect(url_for('processing.view_fail_file', filename=filename))

@processing_bp.route('/fails/<filename>/deny', methods=['POST'])
def deny_fail_file(filename):
    """Handle denial of failed files."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get denial reason from form
        denial_reason = request.form.get('denial_reason', 'No reason provided')
        
        # Get current JSON data
        key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(key)
        
        # Add denial info to JSON
        if 'denial' not in json_data:
            json_data['denial'] = {}
        
        json_data['denial'].update({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'reason': denial_reason,
            'user': request.form.get('user', 'admin_user@clarity-dx.com'),  # TODO: Replace with actual user
        })
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'DENIED'
        
        # Move to denied folder
        dest_key = f'data/hcfa_json/valid/mapped/staging/denied/{filename}'
        upload_json_to_s3(json_data, dest_key)
        
        # Delete from fails folder
        s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
        
        # Remove from failed summary
        try:
            if remove_from_summary(filename):
                logger.info(f"Removed {filename} from failed summary")
            else:
                logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
        except Exception as e:
            logger.error(f"Error removing {filename} from summary: {str(e)}")
        
        # Create success message
        success_message = f"File {filename} has been denied."
        
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            # For AJAX requests
            return jsonify({
                'status': 'success',
                'message': success_message,
                'redirect': build_redirect_url(url_for('processing.list_fail_files'), filter_params)
            })
        else:
            # For regular form submissions
            flash(success_message, 'success')
            return redirect(build_redirect_url(url_for('processing.list_fail_files'), filter_params))
            
    except Exception as e:
        logger.error(f"Error denying file {filename}: {str(e)}")
        error_message = f"Failed to deny file: {str(e)}"
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            return jsonify({
                'status': 'error',
                'message': error_message
            }), 500
        else:
            flash(error_message, 'danger')
            return redirect(url_for('processing.view_fail_file', filename=filename))

@processing_bp.route('/fails/<filename>/move-to-readyforprocess', methods=['POST'])
def move_to_readyforprocess(filename):
    """Move a file from fails folder to readyforprocess folder."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        try:
            json_data = get_s3_json(source_key)
        except Exception as e:
            logger.error(f"Error retrieving file {filename} from S3: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f"Could not retrieve file: {str(e)}"
            }), 404

        # Add metadata about the move
        if 'processing_info' not in json_data:
            json_data['processing_info'] = {}
        
        json_data['processing_info'].update({
            'moved_to_readyforprocess': True,
            'moved_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'moved_by': 'admin_user@clarity-dx.com',  # TODO: Replace with actual user
            'prior_status': json_data.get('validation_info', {}).get('status', 'UNKNOWN')
        })
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'READY_FOR_PROCESS'
        
        # Move to readyforprocess folder
        dest_key = f'data/hcfa_json/readyforprocess/{filename}'
        upload_json_to_s3(json_data, dest_key)
        
        # Delete from fails folder
        s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
        
        # Remove from failed summary
        try:
            if remove_from_summary(filename):
                logger.info(f"Removed {filename} from failed summary")
            else:
                logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
        except Exception as e:
            logger.error(f"Error removing {filename} from summary: {str(e)}")
        
        # Create success message
        success_message = f"File {filename} has been moved to ready for process queue."
        
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            # For AJAX requests
            return jsonify({
                'status': 'success',
                'message': success_message,
                'redirect': build_redirect_url(url_for('processing.list_fail_files'), filter_params)
            })
        else:
            # For regular form submissions
            flash(success_message, 'success')
            return redirect(build_redirect_url(url_for('processing.list_fail_files'), filter_params))
            
    except Exception as e:
        logger.error(f"Error moving file {filename} to readyforprocess: {str(e)}")
        error_message = f"Failed to move file: {str(e)}"
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            return jsonify({
                'status': 'error',
                'message': error_message
            }), 500
        else:
            flash(error_message, 'danger')
            return redirect(url_for('processing.view_fail_file', filename=filename))

def clean_tin(tin: str) -> str:
    """Clean and standardize TIN format by removing non-numeric characters."""
    if not tin:
        return ""
    cleaned = re.sub(r'[^0-9]', '', tin)
    logger.info(f"Cleaned TIN: original='{tin}', cleaned='{cleaned}'")
    return cleaned

def validate_db_connection():
    """Validate database connection and schema."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if ppo table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='ppo'
            """)
            if not cursor.fetchone():
                logger.error("PPO table does not exist in database")
                return False
            
            # Get table schema
            cursor.execute("PRAGMA table_info(ppo)")
            columns = {col[1]: col[2] for col in cursor.fetchall()}
            logger.debug(f"PPO table schema: {columns}")
            
            # Verify required columns
            required_columns = {
                'TIN': 'TEXT',
                'proc_cd': 'TEXT',
                'rate': 'REAL',
                'proc_category': 'TEXT'
            }
            
            for col, type_ in required_columns.items():
                if col not in columns:
                    logger.error(f"Required column {col} missing from ppo table")
                    return False
                if not columns[col].upper().startswith(type_):
                    logger.error(f"Column {col} has incorrect type: {columns[col]} (expected {type_})")
                    return False
            
            # Test insert
            try:
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("""
                    INSERT OR REPLACE INTO ppo (TIN, proc_cd, rate, proc_category)
                    VALUES ('TEST123', 'TEST456', 100.0, 'TEST_CAT')
                """)
                cursor.execute("SELECT * FROM ppo WHERE TIN = 'TEST123'")
                result = cursor.fetchone()
                logger.debug(f"Test insert result: {result}")
                cursor.execute("ROLLBACK")
                
                return True
            except sqlite3.Error as e:
                logger.error(f"Database test insert failed: {str(e)}")
                return False
                
    except sqlite3.Error as e:
        logger.error(f"Database validation failed: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database validation: {str(e)}")
        return False

@contextmanager
def get_db_connection():
    """Context manager for database connections with enhanced logging."""
    logger.debug(f"Opening database connection to {FILEMAKER_DB}")
    if not os.path.exists(FILEMAKER_DB):
        logger.error(f"Database file does not exist at {FILEMAKER_DB}")
        raise FileNotFoundError(f"Database file not found: {FILEMAKER_DB}")
        
    conn = None
    try:
        conn = sqlite3.connect(FILEMAKER_DB)
        logger.debug("Database connection established")
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

def insert_rate_to_db(conn, tin: str, proc_cd: str, rate: float, category: str) -> bool:
    """Helper function to insert a rate into the database with proper error handling."""
    try:
        cursor = conn.cursor()
        logger.debug(f"Inserting rate: TIN={tin}, proc_cd={proc_cd}, rate={rate}, category={category}")
        
        cursor.execute("""
            INSERT OR REPLACE INTO ppo 
            (TIN, proc_cd, rate, proc_category) 
            VALUES (?, ?, ?, ?)
        """, (tin, proc_cd, rate, category))
        
        # Verify the insert
        cursor.execute("SELECT * FROM ppo WHERE TIN = ? AND proc_cd = ?", (tin, proc_cd))
        result = cursor.fetchone()
        if result:
            logger.debug(f"Successfully inserted/updated rate: {result}")
            return True
        else:
            logger.error("Rate insertion failed - no record found after insert")
            return False
            
    except sqlite3.Error as e:
        logger.error(f"Database error during rate insertion: {str(e)}")
        raise

# Validate database on module load
if not validate_db_connection():
    logger.error("Database validation failed - rate assignment functionality may not work correctly")

@processing_bp.route('/fails/<filename>/assign-rates', methods=['POST'])
def assign_rates(filename):
    """Handle rate assignments for CPT codes with missing rates"""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get rate assignment data from request body
        if request.is_json:
            rate_data = request.get_json()
            logger.info(f"Received JSON data: {rate_data}")
        else:
            # Handle form data
            rate_data = {'rate_type': request.form.get('rate_type', 'individual')}
            
            if rate_data['rate_type'] == 'category':
                # Build category rates from form data
                category_rates = {}
                for key, value in request.form.items():
                    if key.startswith('category_rate['):
                        category = key.replace('category_rate[', '').replace(']', '')
                        try:
                            # Only include categories that are enabled
                            if request.form.get(f'category_enabled[{category}]') == 'on':
                                category_rates[category] = float(value)
                        except ValueError:
                            continue
                
                rate_data['category_rates'] = category_rates
                
            else:
                # Handle individual rates from form
                rates = []
                for key, value in request.form.items():
                    if key.startswith('rate-input-'):
                        cpt_code = key.replace('rate-input-', '')
                        modifier = request.form.get(f'modifier-{cpt_code}', '')
                        
                        try:
                            rates.append({
                                'cpt_code': cpt_code,
                                'rate': float(value),
                                'modifier': modifier if modifier else None
                            })
                        except ValueError:
                            continue
                
                rate_data['rates'] = rates
        
        # Log the processed data
        logger.info(f"Processed rate data for {filename}: {rate_data}")
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        try:
            json_data = get_s3_json(source_key)
        except Exception as e:
            logger.error(f"Error retrieving file {filename} from S3: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f"Could not retrieve file: {str(e)}"
            }), 404
        
        # Extract TIN from JSON and clean it
        tin = json_data.get('filemaker', {}).get('provider', {}).get('TIN', '')
        logger.info(f"Cleaned TIN: original='{tin}', cleaned='{clean_tin(tin)}'")
        clean_tin_value = clean_tin(tin)
        if not clean_tin_value:
            return jsonify({
                'status': 'error',
                'message': 'No valid TIN found in file'
            }), 400
        
        # Get Order ID from FileMaker data if available
        order_id = json_data.get('filemaker', {}).get('order', {}).get('Order_ID', '')
        
        # Process based on rate type
        rate_type = rate_data.get('rate_type')
        
        # Initialize category summary
        category_summary = {}
        
        # Prepare rate update data for saving to file
        rate_update_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'user': 'admin_user@clarity-dx.com',  # Replace with actual user when auth is added
            'filename': filename,
            'tin': clean_tin_value,
            'rate_type': rate_type,
            'source': 'manual_correction'
        }
        
        # Add order_id if available
        if order_id:
            rate_update_data['order_id'] = order_id
        
        if rate_type == 'individual':
            rates = rate_data.get('rates', [])
            rates_updated = False
            
            # Add rates to update data
            rate_update_data['rates'] = []
            
            for rate_item in rates:
                cpt_code = rate_item.get('cpt_code')
                rate_value = rate_item.get('rate')
                modifier = rate_item.get('modifier')
                
                # Add to rate update data
                rate_update_data['rates'].append({
                    'cpt_code': cpt_code,
                    'rate': rate_value,
                    'modifier': modifier
                })
                
                logger.info(f"Processing individual rate for CPT {cpt_code}: ${rate_value}")
                
                # Update the service line with the rate
                for line in json_data.get('service_lines', []):
                    if line.get('cpt_code') == cpt_code:
                        line['assigned_rate'] = rate_value
                        rates_updated = True
                        logger.info(f"Updated service line for CPT {cpt_code} with rate ${rate_value}")
                
                # Try to insert into database if it exists
                try:
                    db_path = FILEMAKER_DB
                    if os.path.exists(db_path):
                        with get_db_connection() as conn:
                            cursor = conn.cursor()
                            logger.info(f"Inserting into ppo table: TIN={clean_tin_value}, proc_cd={cpt_code}, modifier={modifier}, rate=${rate_value}")
                            cursor.execute("""
                                INSERT OR REPLACE INTO ppo 
                                (TIN, proc_cd, modifier, rate) 
                                VALUES (?, ?, ?, ?)
                            """, (clean_tin_value, cpt_code, modifier, rate_value))
                            conn.commit()
                            logger.info(f"Successfully inserted/updated rate for CPT {cpt_code} in database")
                    else:
                        logger.warning(f"Database file not found at {db_path}, skipping database update for CPT {cpt_code}")
                except Exception as e:
                    # Log error but continue with other CPTs
                    logger.error(f"Database error for CPT {cpt_code}: {str(e)}", exc_info=True)
            
            if not rates_updated:
                logger.warning(f"No rates were updated for {filename}")
        
        elif rate_type == 'category':
            category_rates = rate_data.get('category_rates', {})
            
            # Add category rates to update data
            rate_update_data['category_rates'] = category_rates
            
            logger.info(f"Processing category rates: {category_rates}")
            
            if not category_rates:
                logger.error("No category rates provided")
                return jsonify({
                    'status': 'error',
                    'message': 'No category rates provided'
                }), 400
            
            # Get CPT codes by category
            cpt_by_category = get_cpt_codes_by_category()
            
            # Track which CPT codes were assigned for each category
            rate_update_data['cpt_assignments'] = {category: [] for category in category_rates.keys()}
            
            # Update rates based on CPT code's category
            for line in json_data.get('service_lines', []):
                cpt_code = line.get('cpt_code')
                if not cpt_code:
                    continue
                    
                # Find which category this CPT belongs to
                for category, cpt_list in cpt_by_category.items():
                    if cpt_code in cpt_list and category in category_rates:
                        rate = category_rates[category]
                        line['assigned_rate'] = rate
                        if category not in category_summary:
                            category_summary[category] = 0
                        category_summary[category] += 1
                        
                        # Add to rate update data
                        rate_update_data['cpt_assignments'][category].append(cpt_code)
                        
                        logger.info(f"Matched CPT {cpt_code} to category {category}, assigning rate ${rate}")
                        
                        # Try to insert into database
                        try:
                            db_path = FILEMAKER_DB
                            if os.path.exists(db_path):
                                with get_db_connection() as conn:
                                    cursor = conn.cursor()
                                    logger.info(f"Inserting into ppo table: TIN={clean_tin_value}, proc_cd={cpt_code}, rate=${rate}, category={category}")
                                    cursor.execute("""
                                        INSERT OR REPLACE INTO ppo 
                                        (TIN, proc_cd, rate, proc_category) 
                                        VALUES (?, ?, ?, ?)
                                    """, (clean_tin_value, cpt_code, rate, category))
                                    conn.commit()
                                    logger.info(f"Successfully inserted/updated rate for CPT {cpt_code} in category {category}")
                            else:
                                logger.warning(f"Database file not found at {db_path}, skipping database update for CPT {cpt_code}")
                        except Exception as e:
                            # Log error but continue
                            logger.error(f"Database error for CPT {cpt_code}: {str(e)}", exc_info=True)
                        break
            
            # Add category summary to rate update data
            rate_update_data['category_summary'] = category_summary
            logger.info(f"Category summary after processing: {category_summary}")
        
        # Create custom OTA case if specified
        elif rate_type == 'create_ota':
            # Add OTA-specific data
            rates = rate_data.get('rates', [])
            if not rates:
                logger.error("No rates provided for OTA creation")
                return jsonify({
                    'status': 'error',
                    'message': 'No rates provided for OTA creation'
                }), 400
                
            # Add rates to update data
            rate_update_data['rates'] = []
            rate_update_data['ota_data'] = {
                'provider_name': json_data.get('filemaker', {}).get('provider', {}).get('Billing Name', ''),
                'provider_npi': json_data.get('filemaker', {}).get('provider', {}).get('NPI', ''),
                'provider_type': json_data.get('filemaker', {}).get('provider', {}).get('Provider_Type', ''),
                'order_id': order_id,
                'notes': rate_data.get('notes', '')
            }
            
            for rate_item in rates:
                cpt_code = rate_item.get('cpt_code')
                rate_value = rate_item.get('rate')
                modifier = rate_item.get('modifier')
                
                # Add to rate update data
                rate_update_data['rates'].append({
                    'cpt_code': cpt_code,
                    'rate': rate_value,
                    'modifier': modifier
                })
                
                logger.info(f"Processing OTA rate for CPT {cpt_code}: ${rate_value}")
                
                # Update the service line with the rate
                for line in json_data.get('service_lines', []):
                    if line.get('cpt_code') == cpt_code:
                        line['assigned_rate'] = rate_value
                        line['rate_source'] = 'OTA'
                        logger.info(f"Updated service line for CPT {cpt_code} with OTA rate ${rate_value}")
            
            logger.info(f"Processing OTA case creation: {rate_update_data['ota_data']}")
        
        # Add rate assignment metadata
        if 'rate_assignment' not in json_data:
            json_data['rate_assignment'] = {}
        
        json_data['rate_assignment'].update({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'user': 'admin_user@clarity-dx.com',  # Replace with actual user when auth is added
            'rate_type': rate_type
        })
        
        # If category assignment, add category data
        if rate_type == 'category':
            json_data['rate_assignment'].update({
                'category_rates': category_rates,
                'category_summary': category_summary
            })
        
        # Save rate update data to local file
        try:
            # Create unique filename with timestamp
            timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
            update_filename = f"{clean_tin_value}_{timestamp_str}_{filename}.json"
            update_filepath = os.path.join('data', 'rate_updates', update_filename)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(update_filepath), exist_ok=True)
            
            # Write JSON file
            with open(update_filepath, 'w') as f:
                json.dump(rate_update_data, f, indent=2)
            
            logger.info(f"Saved rate update data to {update_filepath}")
        except Exception as e:
            logger.error(f"Error saving rate update data to file: {str(e)}", exc_info=True)
        
        # Update validation info
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        
        # Remove rate-related failure reasons
        updated_reasons = []
        for reason in json_data['validation_info'].get('failure_reasons', []):
            if not reason.startswith('RATE_MISSING:'):
                updated_reasons.append(reason)
        
        json_data['validation_info']['failure_reasons'] = updated_reasons
        
        # Determine next steps based on remaining failure reasons
        if not updated_reasons:
            # All validations pass - move to success folder
            json_data['validation_info']['status'] = 'PASS'
            
            # Save to success folder
            dest_key = f'data/hcfa_json/valid/mapped/staging/success/{filename}'
            upload_json_to_s3(json_data, dest_key)
            
            # Delete from fails folder
            s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
            
            # Remove from failed summary
            try:
                if remove_from_summary(filename):
                    logger.info(f"Removed {filename} from failed summary")
                else:
                    logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
            except Exception as e:
                logger.error(f"Error removing {filename} from summary: {str(e)}")
            
            success_message = "Rates assigned and file processed successfully!"
        else:
            # Other failures remain - keep in fails folder
            upload_json_to_s3(json_data, source_key)
            success_message = "Rates assigned successfully, but other validation issues remain."
        
        # Return success response
        return jsonify({
            'status': 'success',
            'message': success_message,
            'category_summary': category_summary if rate_type == 'category' else None,
            'redirect': url_for('processing.list_fail_files')  # Always redirect to list view
        })
        
    except Exception as e:
        logger.error(f"Error assigning rates for {filename}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f"Failed to assign rates: {str(e)}"
        }), 500
    
    
def get_s3_json(key):
    """Get JSON data from S3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error getting JSON from S3: {str(e)}")
        raise

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
        raise
