import os
import sys
import pytest
from pathlib import Path

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the required modules
from portal.views.processing import assign_rates, get_cpt_codes_by_category

# Common fixtures that can be used across test files
@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory"""
    return project_root / 'tests' / 'data'

@pytest.fixture
def sample_cpt_codes():
    """Return a sample of CPT codes for testing"""
    return {
        'mri_wo': ['70551', '70540'],
        'mri_w': ['70552', '70542'],
        'mri_wwo': ['70553', '70543'],
        'ct_wo': ['70450', '71250'],
        'ct_w': ['70460', '71260'],
        'ct_wwo': ['70470', '71270'],
        'xray': ['71045', '71046'],
        'ultrasound': ['76536', '76604']
    }

@pytest.fixture
def sample_rates():
    """Return sample rate data for testing"""
    return {
        'mri_wo': 190.00,
        'mri_w': 250.00,
        'mri_wwo': 300.00,
        'ct_wo': 150.00,
        'ct_w': 200.00,
        'ct_wwo': 250.00,
        'xray': 75.00,
        'ultrasound': 100.00
    } 