from flask import Flask
from flask_bootstrap import Bootstrap
from flask_login import LoginManager
from pathlib import Path
import os
import sys
import logging
import subprocess
from utils.summary_manager import ensure_summary_file

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    Bootstrap(app)
    
    # Set secret key - use environment variable or fallback to a random key
    app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', os.urandom(24))
    
    # Configure session to be more secure
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['REMEMBER_COOKIE_DURATION'] = 30 * 24 * 60 * 60  # 30 days
    app.config['REMEMBER_COOKIE_SECURE'] = False  # Set to True if using HTTPS
    app.config['REMEMBER_COOKIE_HTTPONLY'] = True
    
    # Set default directories
    app.config['FAILS_DIR'] = os.path.join(app.root_path, 'data', 'fails')
    app.config['READYFORPROCESS_DIR'] = os.path.join(app.root_path, 'data', 'readyforprocess')
    
    # Initialize Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'warning'
    
    @login_manager.user_loader
    def load_user(user_id):
        from .models import User
        return User.get(user_id)
    
    # Ensure the failed_summary.json file exists and is valid
    try:
        if ensure_summary_file():
            logger.info("Failed summary file verified successfully")
        else:
            logger.warning("Failed summary file appears invalid, created a new empty file")
    except Exception as e:
        logger.error(f"Error ensuring failed summary file: {str(e)}")
    
    # Register blueprints
    from .views.home import home_bp
    from .views.preprocessing import preprocessing_bp
    from .views.processing import processing_bp
    from .views.postprocessing import postprocessing_bp
    from .views.auth import auth_bp
    
    app.register_blueprint(home_bp, url_prefix='/')
    app.register_blueprint(preprocessing_bp, url_prefix='/preprocessing')
    app.register_blueprint(processing_bp, url_prefix='/processing')
    app.register_blueprint(postprocessing_bp, url_prefix='/postprocessing')
    app.register_blueprint(auth_bp, url_prefix='/auth')
    
    # Register CLI commands
    @app.cli.command('refresh-summary')
    def refresh_summary_command():
        """Refresh the failed_summary.json file from S3 fails directory."""
        try:
            logger.info("Starting summary refresh...")
            
            # Get the project root directory
            project_root = Path(__file__).resolve().parents[1]
            
            # Path to the refresh_summary.py script
            script_path = project_root / 'scripts' / 'refresh_summary.py'
            
            # Ensure the script exists
            if not script_path.exists():
                logger.error(f"Refresh script not found at {script_path}")
                sys.exit(1)
            
            # Execute the script as a subprocess
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True
            )
            
            # Log the output
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    logger.info(f"Script output: {line}")
                    
            if result.stderr:
                for line in result.stderr.strip().split('\n'):
                    logger.error(f"Script error: {line}")
            
            # Check the return code
            if result.returncode == 0:
                logger.info("Summary refresh completed successfully")
            else:
                logger.error(f"Summary refresh failed with exit code {result.returncode}")
                
            return result.returncode
            
        except Exception as e:
            logger.error(f"Error during summary refresh: {str(e)}")
            return 1
    
    return app 