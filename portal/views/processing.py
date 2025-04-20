from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from utils.s3_utils import list_objects, get_s3_json, upload_json_to_s3, move
import os
import re
import boto3
import sqlite3
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
    

@processing_bp.route('/fails/<filename>/assign-rates', methods=['POST'])
def assign_rates(filename):
    """Handle rate assignments for CPT codes with missing rates"""
    try:
        # Get JSON data from request
        data = request.get_json()
        tin = data.get('tin', '')  # Should be clean TIN (9 digits)
        rates = data.get('rates', [])
        notes = data.get('notes', '')
        provider_network = data.get('provider_network', '')
        
        if not tin or not rates:
            return jsonify({
                'success': False,
                'error': 'Missing required data (TIN or rates)'
            }), 400
        
        # Clean TIN to ensure it's just 9 digits
        tin = ''.join(filter(str.isdigit, tin))
        if len(tin) != 9:
            return jsonify({
                'success': False,
                'error': f'Invalid TIN format: {tin} (must be 9 digits)'
            }), 400
        
        # Get the current JSON data for the file
        key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(key)
        
        # Handle In Network rate assignments
        if provider_network == 'In Network':
            # Connect to database
            conn = sqlite3.connect('filemaker.db')
            cursor = conn.cursor()
            
            try:
                # Check if PPO table exists, create it if not
                cursor.execute("""
                    SELECT name FROM sqlite_master WHERE type='table' AND name='ppo'
                """)
                
                if not cursor.fetchone():
                    # Create PPO table if it doesn't exist
                    cursor.execute("""
                        CREATE TABLE ppo (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            TIN TEXT NOT NULL,
                            proc_cd TEXT NOT NULL,
                            modifier TEXT,
                            rate REAL NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                
                # Process each rate
                for rate_data in rates:
                    cpt_code = rate_data.get('cpt_code')
                    rate = rate_data.get('rate')
                    modifier = rate_data.get('modifier')
                    
                    # Check if record already exists
                    cursor.execute("""
                        SELECT * FROM ppo 
                        WHERE TIN = ? AND proc_cd = ? AND (modifier = ? OR (? IS NULL AND modifier IS NULL))
                    """, (tin, cpt_code, modifier, modifier))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing record
                        cursor.execute("""
                            UPDATE ppo SET rate = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE TIN = ? AND proc_cd = ? AND (modifier = ? OR (? IS NULL AND modifier IS NULL))
                        """, (rate, tin, cpt_code, modifier, modifier))
                        logger.info(f"Updated existing rate for TIN {tin}, CPT {cpt_code}, modifier {modifier}")
                    else:
                        # Insert new record
                        cursor.execute("""
                            INSERT INTO ppo (TIN, proc_cd, modifier, rate)
                            VALUES (?, ?, ?, ?)
                        """, (tin, cpt_code, modifier, rate))
                        logger.info(f"Inserted new rate for TIN {tin}, CPT {cpt_code}, modifier {modifier}")
                
                # Commit changes
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
            
            # Add rate assignment info to JSON
            if 'rate_assignment' not in json_data:
                json_data['rate_assignment'] = {}
                
            json_data['rate_assignment'] = {
                'tin': tin,
                'rates': rates,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'user': 'admin_user@clarity-dx.com',  # TODO: Replace with actual user
                'notes': notes,
                'provider_network': provider_network
            }
            
            # Update validation status
            if 'validation_info' not in json_data:
                json_data['validation_info'] = {}
            
            # Remove the rate missing failure reasons
            if 'failure_reasons' in json_data['validation_info']:
                updated_reasons = []
                for reason in json_data['validation_info'].get('failure_reasons', []):
                    is_rate_issue = False
                    for rate_data in rates:
                        if reason.startswith(f'RATE_MISSING: {rate_data["cpt_code"]}'):
                            is_rate_issue = True
                            break
                    if not is_rate_issue:
                        updated_reasons.append(reason)
                
                json_data['validation_info']['failure_reasons'] = updated_reasons
            
            # If no more failures, update status and move to success folder
            if not json_data['validation_info'].get('failure_reasons'):
                json_data['validation_info']['status'] = 'PASS'
                dest_key = f'data/hcfa_json/valid/mapped/staging/success/{filename}'
                upload_json_to_s3(json_data, dest_key)
                # Delete from fails folder
                s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
                logger.info(f"File {filename} moved to success folder")
            else:
                # Still has other failures, save back to fails folder
                upload_json_to_s3(json_data, key)
                logger.info(f"File {filename} updated but still has failures: {json_data['validation_info'].get('failure_reasons')}")
            
            return jsonify({
                'success': True,
                'message': 'Rate assigned successfully'
            })
        
        # Handle Out of Network rate assignments (OTA)
        else:
            # This will be implemented separately
            return jsonify({
                'success': False,
                'error': 'OTA rate assignment not yet implemented'
            }), 501
            
    except sqlite3.Error as e:
        logger.error(f"Database error assigning rates for {filename}: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        }), 500
    except Exception as e:
        logger.error(f"Error assigning rates for {filename}: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@processing_bp.route('/fails/<filename>/escalate', methods=['POST'])
def escalate_fail_file(filename):
    """Handle escalation of a failed file."""
    try:
        # Get escalation reason from request body
        escalation_reason = request.form.get('reason', 'No reason provided')
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
        # Add escalation metadata
        if 'escalation' not in json_data:
            json_data['escalation'] = {}
        
        json_data['escalation'].update({
            'reason': escalation_reason,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'user': request.form.get('user', 'admin_user@clarity-dx.com')  # TODO: Replace with actual user
        })
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'ESCALATED'
        
        # Move file to escalations folder
        dest_key = f'data/hcfa_json/valid/mapped/staging/escalations/{filename}'
        upload_json_to_s3(json_data, dest_key)
        
        # Delete from fails folder
        s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
        
        flash('File has been escalated successfully.', 'success')
        return jsonify({
            'status': 'success',
            'message': 'File escalated successfully'
        })
        
    except Exception as e:
        logger.error(f"Error escalating file {filename}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@processing_bp.route('/fails/<filename>/deny', methods=['POST'])
def deny_fail_file(filename):
    """Handle denial of a failed file."""
    try:
        # Get denial reason from request body
        denial_reason = request.form.get('reason')
        
        # Validate denial reason
        valid_reasons = ['CO-50', 'Claim not found in FileMaker']
        if not denial_reason or denial_reason not in valid_reasons:
            return jsonify({
                'status': 'error',
                'message': 'Invalid denial reason. Must be either "CO-50" or "Claim not found in FileMaker"'
            }), 400
        
        # Get current JSON data
        source_key = f'data/hcfa_json/valid/mapped/staging/fails/{filename}'
        json_data = get_s3_json(source_key)
        
        # Add denial metadata
        if 'denial' not in json_data:
            json_data['denial'] = {}
        
        json_data['denial'].update({
            'reason': denial_reason,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'user': request.form.get('user', 'admin_user@clarity-dx.com')  # TODO: Replace with actual user
        })
        
        # Update validation status
        if 'validation_info' not in json_data:
            json_data['validation_info'] = {}
        json_data['validation_info']['status'] = 'DENIED'
        
        # Move file to denials folder
        dest_key = f'data/hcfa_json/denials/{filename}'
        upload_json_to_s3(json_data, dest_key)
        
        # Delete from fails folder
        s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
        
        flash('File has been denied successfully.', 'success')
        return jsonify({
            'status': 'success',
            'message': 'File denied successfully'
        })
        
    except Exception as e:
        logger.error(f"Error denying file {filename}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500