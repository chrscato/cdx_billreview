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
        return False 

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
        "processing/fails.html",
        bills_json=files,
        fails_count=len(files),
        filter_params=filter_params
    )

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
        
    query_params = []
    for key, value in filter_params.items():
        if value:
            query_params.append(f"{key}={urllib.parse.quote(str(value))}")
    
    if query_params:
        return f"{url}?{'&'.join(query_params)}"
    
    return url

@processing_bp.route('/fails/<filename>/move-to-readyforprocess', methods=['POST'])
def move_to_readyforprocess(filename):
    """Move a file from fails folder to readyforprocess folder."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
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

@processing_bp.route('/fails/<filename>/send-to-garbage', methods=['POST'])
def send_to_garbage(filename):
    """Move a failed file to the garbage folder."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get reason from form
        reason = request.form.get('reason', 'No reason provided')
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
        # Add reason to JSON
        json_data['garbage'] = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'reason': reason,
            'user': 'admin_user@clarity-dx.com'  # TODO: Replace with actual user
        }
        
        # Save to garbage folder
        dest_key = f'data/hcfa_json/valid/mapped/staging/garbage/{filename}'
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
        success_message = f"File {filename} has been moved to the garbage folder."
        
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
        logger.error(f"Error moving file {filename} to garbage: {str(e)}")
        error_message = f"Failed to move file to garbage: {str(e)}"
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
        denial_reason = request.form.get('reason', 'No reason provided')
        
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
                        modifier = request.form.get(f'modifier-input-{cpt_code}', '')
                        
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
        json_data = get_s3_json(source_key)
        
        # Extract TIN from JSON and clean it
        tin = json_data.get('filemaker', {}).get('provider', {}).get('TIN', '')
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
        
        # Save back to S3
        upload_json_to_s3(json_data, source_key)
        
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

@processing_bp.route('/fails/filters', methods=['POST'])
def update_filters():
    """Handle filter updates via AJAX."""
    try:
        # Get filter data from request
        filter_data = request.get_json()
        if not filter_data:
            return jsonify({'error': 'No filter data provided'}), 400
            
        # Store filters in session
        session['filter_params'] = filter_data
        
        # Return success
        return jsonify({
            'status': 'success',
            'message': 'Filters updated successfully'
        })
    except Exception as e:
        logger.error(f"Error updating filters: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@processing_bp.route('/fails/<filename>')
def view_fail_file(filename):
    """View details of a specific failed file."""
    try:
        # Get filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get the JSON data for this file
        key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(key)
        
        # Get list of all failed files for navigation
        json_path = BASE_DIR / "data" / "dashboard" / "failed_summary.json"
        try:
            with open(json_path, "r") as f:
                all_files = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            all_files = []
        
        # Apply filters to get the current subset of files
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
        
        # Get filenames list for navigation
        filenames = [f.get("filename") for f in files if f.get("filename")]
        
        # Find current position and adjacent files
        try:
            current_index = filenames.index(filename) + 1
            prev_file = filenames[current_index - 2] if current_index > 1 else None
            next_file = filenames[current_index] if current_index < len(filenames) else None
        except (ValueError, IndexError):
            current_index = 0
            prev_file = None
            next_file = None
        
        # Build filter query string for navigation
        filter_query = ""
        if filter_params:
            query_parts = []
            for key, value in filter_params.items():
                if value:
                    query_parts.append(f"{key}={urllib.parse.quote(str(value))}")
            if query_parts:
                filter_query = "?" + "&".join(query_parts)
        
        # Return JSON if requested
        if request.args.get('format') == 'json':
            return jsonify({
                'success': True,
                'data': json_data,
                'navigation': {
                    'current_index': current_index,
                    'total_files': len(filenames),
                    'prev_file': prev_file,
                    'next_file': next_file
                }
            })
        
        # Otherwise render template
        return render_template(
            'processing/edit_fail.html',
            filename=filename,
            json_data=json_data,
            current_index=current_index,
            total_files=len(filenames),
            fails_count=len(filenames),
            prev_file=prev_file,
            next_file=next_file,
            filter_query=filter_query,
            filter_params=filter_params
        )
        
    except Exception as e:
        logger.error(f"Error viewing file {filename}: {str(e)}")
        if request.args.get('format') == 'json':
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500
        else:
            flash(f"Error viewing file: {str(e)}", 'danger')
            return redirect(url_for('processing.list_fail_files')) 

def get_cpt_codes_by_category():
    """Get mapping of CPT codes to their categories."""
    return {
        'mri_wo': ['70551', '70552', '70553', '72141', '72146', '72148', '72195', '73221', '73721', '70540'],
        'mri_w': ['70542', '70543', '72142', '72147', '72149', '72196', '73222', '73722', '70541'],
        'mri_wwo': ['70553', '72156', '72157', '72158', '72197', '73223', '73723'],
        'ct_wo': ['70450', '70486', '71250', '72125', '72128', '72131', '72192', '73200', '73700'],
        'ct_w': ['70460', '70487', '71260', '72126', '72129', '72132', '72193', '73201', '73701'],
        'ct_wwo': ['70470', '70488', '71270', '72127', '72130', '72133', '72194', '73202', '73702'],
        'xray': ['71045', '71046', '71047', '71048', '72020', '72040', '72050', '72052', '72070', '72072', '72074', '72080', '72100', '72110', '72114', '72120'],
        'ultrasound': ['76536', '76604', '76641', '76642', '76700', '76705', '76770', '76775', '76800', '76801', '76805', '76811', '76815', '76817']
    }

def clean_tin(tin):
    """Clean TIN by removing non-numeric characters."""
    if not tin:
        return None
    return re.sub(r'[^0-9]', '', tin)

@contextmanager
def get_db_connection():
    """Get a database connection."""
    conn = None
    try:
        conn = sqlite3.connect(FILEMAKER_DB)
        yield conn
    finally:
        if conn:
            conn.close()

@processing_bp.route('/fails/<filename>/submit', methods=['POST'])
def submit_fail_file(filename):
    """Handle form submission for a failed file."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
        # Update service lines from form data
        service_lines = []
        for i in range(len(json_data['service_lines'])):
            line = {
                'cpt_code': request.form.get(f'service_lines[{i}][cpt_code]'),
                'charge_amount': request.form.get(f'service_lines[{i}][charge_amount]'),
                'date_of_service': request.form.get(f'service_lines[{i}][date_of_service]'),
                'units': request.form.get(f'service_lines[{i}][units]'),
                'place_of_service': request.form.get(f'service_lines[{i}][place_of_service]'),
                'diagnosis_pointer': request.form.get(f'service_lines[{i}][diagnosis_pointer]'),
                'modifiers': request.form.get(f'service_lines[{i}][modifiers]', '').split(',') if request.form.get(f'service_lines[{i}][modifiers]') else []
            }
            service_lines.append(line)
        
        json_data['service_lines'] = service_lines
        
        # Add edit metadata
        if 'edit_history' not in json_data:
            json_data['edit_history'] = []
        
        json_data['edit_history'].append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'user': 'admin_user@clarity-dx.com',  # TODO: Replace with actual user
            'action': 'edit',
            'changes': {
                'service_lines': True
            }
        })
        
        # Save back to S3
        upload_json_to_s3(json_data, source_key)
        
        # Create success message
        success_message = "Changes saved successfully."
        
        # Handle different actions
        action = request.form.get('action', 'save')
        
        if action == 'save':
            flash(success_message, 'success')
            return redirect(url_for('processing.view_fail_file', filename=filename))
        elif action == 'move_to_staging':
            return redirect(url_for('processing.move_to_staging', filename=filename))
        else:
            flash(success_message, 'success')
            return redirect(url_for('processing.list_fail_files'))
            
    except Exception as e:
        logger.error(f"Error submitting changes for {filename}: {str(e)}")
        flash(f"Error saving changes: {str(e)}", 'danger')
        return redirect(url_for('processing.view_fail_file', filename=filename))

@processing_bp.route('/fails/<filename>/move-to-staging', methods=['POST'])
def move_to_staging(filename):
    """Move a file from fails folder to staging folder."""
    try:
        # Extract filter parameters
        filter_params = get_filter_params_from_request(request)
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
        # Add metadata about the move
        if 'processing_info' not in json_data:
            json_data['processing_info'] = {}
        
        json_data['processing_info'].update({
            'moved_to_staging': True,
            'moved_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'moved_by': 'admin_user@clarity-dx.com',  # TODO: Replace with actual user
            'prior_status': json_data.get('validation_info', {}).get('status', 'UNKNOWN')
        })
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'STAGING'
        
        # Move to staging folder
        dest_key = f'data/hcfa_json/valid/mapped/staging/{filename}'
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
        success_message = f"File {filename} has been moved to staging."
        
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
        logger.error(f"Error moving file {filename} to staging: {str(e)}")
        error_message = f"Failed to move file: {str(e)}"
        if 'X-Requested-With' in request.headers and request.headers['X-Requested-With'] == 'XMLHttpRequest':
            return jsonify({
                'status': 'error',
                'message': error_message
            }), 500
        else:
            flash(error_message, 'danger')
            return redirect(url_for('processing.view_fail_file', filename=filename))

def confirmMoveToStaging():
    """Confirm moving a file to staging."""
    return """
        if (confirm('Are you sure you want to move this file to staging?')) {
            document.getElementById('form-action').value = 'move_to_staging';
            document.getElementById('edit-form').submit();
        }
    """ 

@processing_bp.route('/processing/fails/<filename>/update-filemaker', methods=['POST'])
def update_filemaker_data(filename):
    try:
        # Get form data
        billing_name = request.form.get('billing_name')
        billing_address_1 = request.form.get('billing_address_1')
        billing_address_city = request.form.get('billing_address_city')
        billing_address_state = request.form.get('billing_address_state')
        billing_address_postal_code = request.form.get('billing_address_postal_code')
        tin = request.form.get('tin')
        npi = request.form.get('npi')

        # Validate required fields
        if not all([billing_name, billing_address_1, billing_address_city, 
                   billing_address_state, billing_address_postal_code, tin, npi]):
            return jsonify({
                'success': False, 
                'error': 'All fields are required'
            }), 400

        # Get the current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        if not json_data:
            return jsonify({'success': False, 'error': 'Could not find JSON file'}), 404

        # Get the provider's PrimaryKey from the JSON data
        provider_key = json_data['filemaker']['provider']['PrimaryKey']
        if not provider_key:
            return jsonify({'success': False, 'error': 'No provider PrimaryKey found in JSON data'}), 400

        # Update the FileMaker database with proper transaction handling
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Start transaction
                cursor.execute("BEGIN TRANSACTION")
                
                try:
                    # Update the providers table
                    cursor.execute("""
                        UPDATE providers 
                        SET "Billing Name" = ?,
                            "Billing Address 1" = ?,
                            "Billing Address City" = ?,
                            "Billing Address State" = ?,
                            "Billing Address Postal Code" = ?,
                            "TIN" = ?,
                            "NPI" = ?
                        WHERE PrimaryKey = ?
                    """, (
                        billing_name,
                        billing_address_1,
                        billing_address_city,
                        billing_address_state,
                        billing_address_postal_code,
                        tin,
                        npi,
                        provider_key
                    ))
                    
                    # Verify the update was successful
                    cursor.execute("SELECT COUNT(*) FROM providers WHERE PrimaryKey = ?", (provider_key,))
                    if cursor.fetchone()[0] == 0:
                        raise Exception("Provider not found after update")
                    
                    # Commit the transaction
                    conn.commit()
                    
                except Exception as e:
                    # Rollback on error
                    conn.rollback()
                    raise
                
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return jsonify({
                'success': False,
                'error': f'Database update failed: {str(e)}'
            }), 500

        # Update the JSON data
        json_data['filemaker']['provider']['Billing Name'] = billing_name
        json_data['filemaker']['provider']['Billing Address 1'] = billing_address_1
        json_data['filemaker']['provider']['Billing Address City'] = billing_address_city
        json_data['filemaker']['provider']['Billing Address State'] = billing_address_state
        json_data['filemaker']['provider']['Billing Address Postal Code'] = billing_address_postal_code
        json_data['filemaker']['provider']['TIN'] = tin
        json_data['filemaker']['provider']['NPI'] = npi

        # Save the updated JSON back to S3
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=source_key,
                Body=json.dumps(json_data, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Error uploading JSON to S3: {str(e)}")
            return jsonify({
                'success': False,
                'error': f'Failed to save JSON data: {str(e)}'
            }), 500

        # Remove from failed summary
        try:
            if remove_from_summary(filename):
                logger.info(f"Removed {filename} from failed summary")
            else:
                logger.warning(f"Failed to remove {filename} from summary - entry may not exist")
        except Exception as e:
            logger.error(f"Error removing {filename} from summary: {str(e)}")

        flash('FileMaker data updated and removed from queue successfully.', 'success')
        return redirect(url_for('processing.list_fail_files', filemaker='needs_correction'))

    except Exception as e:
        logger.error(f"Error updating FileMaker data: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500 