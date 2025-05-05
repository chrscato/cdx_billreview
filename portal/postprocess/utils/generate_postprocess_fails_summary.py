import os
import json
import logging
import sys
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_postprocess_fails_summary():
    """Generate summary of files in readyforprocess/fails directory using validate_ready.py output."""
    try:
        # Get the validation summary from validate_ready.py output
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        summary_path = os.path.join(project_root, 'data', 'dashboard', 'summary.json')
        
        if not os.path.exists(summary_path):
            logger.error(f"Could not find validation summary file at {summary_path}")
            return []
            
        with open(summary_path, 'r') as f:
            validation_data = json.load(f)
            
        if not validation_data:
            logger.error("Validation summary file is empty")
            return []
            
        # Save the summary to the dashboard directory
        portal_dir = Path(__file__).resolve().parents[2]
        dashboard_path = portal_dir / "data" / "dashboard" / "postprocessing_failed_summary.json"
        
        os.makedirs(dashboard_path.parent, exist_ok=True)
        with open(dashboard_path, 'w') as f:
            json.dump(validation_data, f, indent=2)
        
        logger.info(f"Generated summary with {len(validation_data)} failed files at {dashboard_path}")
        return validation_data
        
    except Exception as e:
        logger.error(f"Error generating summary: {str(e)}")
        return []

if __name__ == "__main__":
    try:
        fails = generate_postprocess_fails_summary()
        if fails is not None:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 