from flask import Blueprint, render_template
from portal.views.preprocessing import get_mapped_files_count

processing_bp = Blueprint('processing', __name__)

@processing_bp.route('/')
def processing():
    mapped_count = get_mapped_files_count()
    return render_template('processing.html', mapped_count=mapped_count) 