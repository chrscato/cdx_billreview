import pytest
import json
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from portal.views.processing import assign_rates, get_cpt_codes_by_category
from flask import Flask, jsonify

# Test data setup
@pytest.fixture
def mock_s3_client():
    with patch('portal.views.processing.s3_client') as mock:
        yield mock

@pytest.fixture
def mock_db_connection():
    with patch('portal.views.processing.get_db_connection') as mock:
        # Mock cursor and connection
        cursor_mock = MagicMock()
        conn_mock = MagicMock()
        conn_mock.cursor.return_value = cursor_mock
        mock.return_value.__enter__.return_value = conn_mock
        yield mock

@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['TESTING'] = True
    
    # Register the route
    app.route('/fails/<filename>/assign-rates', methods=['POST'])(assign_rates)
    
    return app

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def sample_json_data():
    return {
        'cpt_codes': {
            '70551': {'category': 'mri_wo', 'current_rate': None},
            '71045': {'category': 'xray', 'current_rate': None},
            '72125': {'category': 'ct_wo', 'current_rate': None}
        }
    }

@pytest.fixture
def category_cpt_mapping():
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

class TestCategoryRateAssignment:
    """Test suite for category-based rate assignment functionality"""
    
    def test_get_cpt_codes_by_category(self, mock_db_connection):
        """Test retrieval of CPT codes by category from database"""
        cursor_mock = mock_db_connection.return_value.__enter__.return_value.cursor.return_value
        
        # Mock the database response
        cursor_mock.fetchall.side_effect = [
            [('70551',), ('70540',)],  # MRI w/o
            [('70552',), ('70542',)],  # MRI w/
            [('70553',), ('70543',)],  # MRI w/&w/o
            [('70450',), ('71250',)],  # CT w/o
            [('70460',), ('71260',)],  # CT w/
            [('70470',), ('71270',)],  # CT w/&w/o
            [('71045',), ('71046',)],  # XRAY
            [('76536',), ('76604',)]   # ULTRASOUND
        ]
        
        result = get_cpt_codes_by_category()
        
        # Verify all categories are present
        assert set(result.keys()) == {'mri_wo', 'mri_w', 'mri_wwo', 'ct_wo', 'ct_w', 'ct_wwo', 'xray', 'ultrasound'}
        
        # Verify some specific mappings
        assert '70551' in result['mri_wo']
        assert '71045' in result['xray']
        
        # Verify database was queried for each category
        assert cursor_mock.execute.call_count == 8

    @pytest.mark.parametrize("category,rate,expected_count", [
        ('mri_wo', 190.00, 1),    # Should update 70551
        ('xray', 75.00, 1),       # Should update 71045
        ('ct_wo', 150.00, 0),     # Should update none (no matching CPT codes)
    ])
    def test_single_category_assignment(self, client, mock_s3_client, sample_json_data, category, rate, expected_count):
        # Mock S3 get_object to return our sample data
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(sample_json_data).encode())
        }
        
        form_data = {
            f'category_enabled[{category}]': 'on',
            f'category_rate[{category}]': str(rate),
            'rate_type': 'category'
        }
        
        response = client.post(f'/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert len(data['updated_codes']) == expected_count

    def test_multiple_category_assignment(self, client, mock_s3_client, sample_json_data):
        # Mock S3 get_object to return our sample data
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(sample_json_data).encode())
        }
        
        form_data = {
            'category_enabled[mri_wo]': 'on',
            'category_rate[mri_wo]': '190.00',
            'category_enabled[xray]': 'on',
            'category_rate[xray]': '75.00',
            'rate_type': 'category'
        }
        
        response = client.post('/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert len(data['updated_codes']) == 2

    def test_empty_category_rates(self, client, mock_s3_client, sample_json_data):
        # Mock S3 get_object to return our sample data
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(sample_json_data).encode())
        }
        
        form_data = {
            'category_enabled[mri_wo]': 'on',
            'category_rate[mri_wo]': '',
            'rate_type': 'category'
        }
        
        response = client.post('/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] is False
        assert 'Invalid rate value' in data['message']

    def test_invalid_rate_values(self, client, mock_s3_client, sample_json_data):
        # Mock S3 get_object to return our sample data
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(sample_json_data).encode())
        }
        
        form_data = {
            'category_enabled[mri_wo]': 'on',
            'category_rate[mri_wo]': 'invalid',
            'rate_type': 'category'
        }
        
        response = client.post('/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] is False
        assert 'Invalid rate value' in data['message']

    def test_cpt_code_category_conflict(self, client, mock_s3_client, sample_json_data):
        # Mock S3 get_object to return our sample data
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(sample_json_data).encode())
        }
        
        # First set a CPT code rate
        form_data = {
            'cpt_code': '70551',
            'rate': '200.00',
            'rate_type': 'individual'
        }
        response = client.post('/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 200

        # Then try to set category rate
        form_data = {
            'category_enabled[mri_wo]': 'on',
            'category_rate[mri_wo]': '190.00',
            'rate_type': 'category'
        }
        response = client.post('/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 409
        data = json.loads(response.data)
        assert data['success'] is False
        assert 'Conflict with existing CPT code rates' in data['message']

    def test_persistence_verification(self, client, mock_s3_client, sample_json_data):
        # Mock S3 get_object to return our sample data
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(sample_json_data).encode())
        }
        
        form_data = {
            'category_enabled[mri_wo]': 'on',
            'category_rate[mri_wo]': '190.00',
            'rate_type': 'category'
        }
        
        response = client.post('/fails/test.json/assign-rates', data=form_data)
        assert response.status_code == 200
        
        # Verify S3 client was called to save the updated data
        mock_s3_client.put_object.assert_called_once()
        _, kwargs = mock_s3_client.put_object.call_args
        assert kwargs['Bucket'] == 'your-bucket-name'
        assert kwargs['Key'] == 'test.json'
        
        # Verify rate assignment metadata
        uploaded_data = json.loads(mock_s3_client.put_object.call_args[1]['Body'].decode('utf-8'))
        assert 'rate_assignment' in uploaded_data
        assert uploaded_data['rate_assignment']['rate_type'] == 'category'
        assert uploaded_data['rate_assignment']['category_rates'] == {'mri_wo': 190.00}
        assert 'timestamp' in uploaded_data['rate_assignment']
        
        # Verify category summary was saved
        assert 'category_summary' in uploaded_data['rate_assignment']
        assert uploaded_data['rate_assignment']['category_summary']['mri_wo'] == 1 