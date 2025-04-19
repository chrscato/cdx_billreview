from flask import Blueprint, render_template

postprocessing_bp = Blueprint('postprocessing', __name__)

@postprocessing_bp.route('/')
def postprocessing():
    return render_template('postprocessing.html') 