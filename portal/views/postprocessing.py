import os
import json
import shutil
from flask import Blueprint, render_template, flash, redirect, request, url_for, current_app

postprocessing_bp = Blueprint('postprocessing', __name__)

@postprocessing_bp.route('/')
def index():
    """View postprocessing dashboard."""
    try:
        # Read failed_summary.json for stats
        stats = {
            'total_files': 0,
            'failure_types': {}
        }
        
        summary_path = os.path.join(current_app.root_path, 'data', 'failed_summary.json')
        if os.path.exists(summary_path):
            with open(summary_path, 'r') as f:
                summary = json.load(f)
                
            stats['total_files'] = len(summary)
            
            # Count failure types
            for file in summary:
                for failure in file['failure_types']:
                    stats['failure_types'][failure] = stats['failure_types'].get(failure, 0) + 1

        return render_template('postprocessing.html', stats=stats)

    except Exception as e:
        current_app.logger.error(f"Error loading postprocessing dashboard: {str(e)}")
        flash('Error loading dashboard', 'error')
        return redirect(url_for('home.home'))

@postprocessing_bp.route('/fails')
def fails():
    """View failed files summary."""
    try:
        # Read failed_summary.json
        summary_path = os.path.join(current_app.root_path, 'data', 'failed_summary.json')
        if not os.path.exists(summary_path):
            return render_template('postprocessing/fails.html', stats={'total_files': 0, 'failure_types': {}}, files=[])

        with open(summary_path, 'r') as f:
            summary = json.load(f)

        # Prepare statistics
        stats = {
            'total_files': len(summary),
            'failure_types': {}
        }

        # Count failure types
        for file in summary:
            for failure in file['failure_types']:
                stats['failure_types'][failure] = stats['failure_types'].get(failure, 0) + 1

        return render_template('postprocessing/fails.html', stats=stats, files=summary)

    except Exception as e:
        current_app.logger.error(f"Error loading fails summary: {str(e)}")
        flash('Error loading fails summary', 'error')
        return redirect(url_for('postprocessing.index'))

@postprocessing_bp.route('/fails/<filename>')
def edit_fail(filename):
    """View and edit a failed file."""
    try:
        # Read failed_summary.json to get file info
        summary_path = os.path.join(current_app.root_path, 'data', 'failed_summary.json')
        if not os.path.exists(summary_path):
            flash('Summary file not found', 'error')
            return redirect(url_for('postprocessing.fails'))

        with open(summary_path, 'r') as f:
            summary = json.load(f)

        # Find the file in summary
        file_info = next((f for f in summary if f['filename'] == filename), None)
        if not file_info:
            flash('File not found in summary', 'error')
            return redirect(url_for('postprocessing.fails'))

        # Read the actual file content
        file_path = os.path.join(current_app.config['FAILS_DIR'], filename)
        if not os.path.exists(file_path):
            flash('File not found', 'error')
            return redirect(url_for('postprocessing.fails'))

        with open(file_path, 'r') as f:
            content = f.read()

        return render_template('postprocessing/edit_fail.html', 
                             file={**file_info, 'content': content})

    except Exception as e:
        current_app.logger.error(f"Error loading file {filename}: {str(e)}")
        flash('Error loading file', 'error')
        return redirect(url_for('postprocessing.fails'))

@postprocessing_bp.route('/fails/<filename>/save', methods=['POST'])
def save_fail(filename):
    """Save changes to a failed file."""
    try:
        content = request.form.get('content')
        if not content:
            flash('No content provided', 'error')
            return redirect(url_for('postprocessing.edit_fail', filename=filename))

        # Validate JSON
        try:
            json.loads(content)
        except json.JSONDecodeError as e:
            flash(f'Invalid JSON: {str(e)}', 'error')
            return redirect(url_for('postprocessing.edit_fail', filename=filename))

        # Save the file
        file_path = os.path.join(current_app.config['FAILS_DIR'], filename)
        with open(file_path, 'w') as f:
            f.write(content)

        flash('File saved successfully', 'success')
        return redirect(url_for('postprocessing.edit_fail', filename=filename))

    except Exception as e:
        current_app.logger.error(f"Error saving file {filename}: {str(e)}")
        flash('Error saving file', 'error')
        return redirect(url_for('postprocessing.edit_fail', filename=filename))

@postprocessing_bp.route('/fails/<filename>/move', methods=['POST'])
def move_fail(filename):
    """Move a file from fails to readyforprocess directory."""
    try:
        fails_path = os.path.join(current_app.config['FAILS_DIR'], filename)
        ready_path = os.path.join(current_app.config['READYFORPROCESS_DIR'], filename)

        if not os.path.exists(fails_path):
            flash('File not found', 'error')
            return redirect(url_for('postprocessing.fails'))

        # Move the file
        shutil.move(fails_path, ready_path)

        # Update failed_summary.json
        summary_path = os.path.join(current_app.root_path, 'data', 'failed_summary.json')
        if os.path.exists(summary_path):
            with open(summary_path, 'r') as f:
                summary = json.load(f)
            
            # Remove the file from summary
            summary = [f for f in summary if f['filename'] != filename]
            
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)

        flash('File moved to ready for process', 'success')
        return redirect(url_for('postprocessing.fails'))

    except Exception as e:
        current_app.logger.error(f"Error moving file {filename}: {str(e)}")
        flash('Error moving file', 'error')
        return redirect(url_for('postprocessing.fails')) 