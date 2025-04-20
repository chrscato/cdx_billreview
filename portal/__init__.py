from flask import Flask
from flask_bootstrap5 import Bootstrap
from pathlib import Path
import os

def create_app():
    app = Flask(__name__)
    Bootstrap(app)
    
    # Set secret key - use environment variable or fallback to a random key
    app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', os.urandom(24))
    
    # Register blueprints
    from .views.home import home_bp
    from .views.preprocessing import preprocessing_bp
    from .views.processing import processing_bp
    from .views.postprocessing import postprocessing_bp
    
    app.register_blueprint(home_bp, url_prefix='/')
    app.register_blueprint(preprocessing_bp, url_prefix='/preprocessing')
    app.register_blueprint(processing_bp, url_prefix='/processing')
    app.register_blueprint(postprocessing_bp, url_prefix='/postprocessing')
    
    return app 