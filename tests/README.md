# Category Rate Assignment Test Plan

This document outlines the test plan for the category-based rate assignment functionality in the bill review system.

## Test Coverage

The test suite covers the following key areas:

1. **Category Mapping**
   - Verification of CPT code to category mapping from database
   - Handling of all supported categories (MRI, CT, X-ray, Ultrasound)
   - Validation of category data structure

2. **Single Category Assignment**
   - Assignment of rates to a single category
   - Verification of rate updates for matching CPT codes
   - Handling of categories with no matching CPT codes
   - Summary information accuracy

3. **Multiple Category Assignment**
   - Simultaneous rate assignment to multiple categories
   - Verification of correct rate application per category
   - Cross-category validation
   - Summary aggregation accuracy

4. **Edge Cases and Error Handling**
   - Empty category rate submissions
   - Invalid rate values
   - CPT codes appearing in multiple categories
   - Missing or malformed data

5. **Data Persistence**
   - Verification of S3 storage
   - Rate assignment metadata
   - Category summary persistence
   - JSON structure integrity

## Running the Tests

### Prerequisites
- Python 3.8+
- pytest
- pytest-mock
- boto3

### Installation
```bash
pip install pytest pytest-mock boto3
```

### Running Tests
```bash
# Run all tests
pytest tests/test_category_rate_assignment.py -v

# Run specific test class
pytest tests/test_category_rate_assignment.py::TestCategoryRateAssignment -v

# Run specific test
pytest tests/test_category_rate_assignment.py::TestCategoryRateAssignment::test_multiple_category_assignment -v
```

### Test Environment Setup
The tests use fixtures to mock:
- S3 client for storage operations
- Database connections for category mapping
- Sample JSON data for consistent testing

## Test Scenarios

### 1. Category Mapping Tests
- Verify all categories are correctly mapped
- Validate CPT code assignments
- Check database query functionality

### 2. Rate Assignment Tests
- Single category rate assignment
- Multiple category rate assignment
- Rate validation and error handling
- Category summary generation

### 3. Edge Case Tests
- Empty submissions
- Invalid rate values
- CPT code conflicts
- Data persistence verification

## Expected Results

Each test verifies:
1. Correct rate assignment to CPT codes
2. Accurate category summary generation
3. Proper error handling
4. Data persistence in S3
5. Response format and content

## Manual Testing Checklist

In addition to automated tests, perform these manual checks:

1. **UI Interaction**
   - [ ] Category checkboxes enable/disable correctly
   - [ ] Rate input fields validate properly
   - [ ] Summary display shows correct information
   - [ ] Error messages are clear and helpful

2. **Data Flow**
   - [ ] Form submission captures all selected categories
   - [ ] Rates are applied correctly
   - [ ] Summary information matches expectations
   - [ ] Database updates are accurate

3. **Integration**
   - [ ] Category assignments work with other features
   - [ ] Navigation flows correctly after assignment
   - [ ] Error handling integrates with UI
   - [ ] Performance is acceptable

## Troubleshooting

Common issues and solutions:

1. **Test Failures**
   - Check mock configurations
   - Verify fixture data
   - Ensure database connection settings
   - Validate S3 credentials

2. **Integration Issues**
   - Verify API endpoints
   - Check JSON structure
   - Validate rate formats
   - Review error handling

## Maintenance

To maintain test quality:

1. Update test data when:
   - Adding new categories
   - Modifying CPT code mappings
   - Changing rate formats
   - Updating API responses

2. Review and update tests when:
   - Modifying category assignment logic
   - Changing data persistence methods
   - Updating UI interactions
   - Adding new features 